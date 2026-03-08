use anyhow::Context;
use async_mutex::Mutex;
use async_trait::async_trait;
use indexmap::IndexMap;
use mongodb::bson::Document;
use opendal::Operator;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration, Instant};

use crate::{
    checker::check_log::{CheckLog, CheckSummaryLog, DiffColValue, TableCheckCount},
    checker::state_store::{CheckerCheckpointCommit, CheckerStateRow, SqlCheckerStateStore},
    rdb_query_builder::RdbQueryBuilder,
    rdb_router::RdbRouter,
    sinker::base_sinker::BaseSinker,
    sinker::mongo::mongo_cmd,
};
use dt_common::meta::{
    col_value::ColValue, mongo::mongo_constant::MongoConstants, mysql::mysql_tb_meta::MysqlTbMeta,
    pg::pg_tb_meta::PgTbMeta, position::Position, rdb_meta_manager::RdbMetaManager,
    rdb_tb_meta::RdbTbMeta, row_data::RowData, row_type::RowType,
};
use dt_common::{
    log_diff, log_error, log_info, log_miss, log_sql, log_summary, log_warn,
    monitor::monitor::Monitor, utils::limit_queue::LimitedQueue,
};

#[path = "cdc_state.rs"]
mod cdc_state;
#[path = "checker_engine.rs"]
mod engine;
#[cfg(test)]
#[path = "base_checker_tests.rs"]
mod tests;

pub const CHECKER_MAX_QUERY_BATCH: usize = 1000;

#[derive(Debug, Clone)]
pub enum CheckerTbMeta {
    Mysql(MysqlTbMeta),
    Pg(PgTbMeta),
    Mongo(RdbTbMeta),
}

impl CheckerTbMeta {
    pub fn basic(&self) -> &RdbTbMeta {
        match self {
            CheckerTbMeta::Mysql(m) => &m.basic,
            CheckerTbMeta::Pg(m) => &m.basic,
            CheckerTbMeta::Mongo(m) => m,
        }
    }

    fn build_miss_sql(&self, src_row_data: &RowData) -> anyhow::Result<Option<String>> {
        let after = match &src_row_data.after {
            Some(after) if !after.is_empty() => after.clone(),
            _ => return Ok(None),
        };
        if matches!(self, CheckerTbMeta::Mongo(_)) {
            return Ok(mongo_cmd::build_insert_cmd(src_row_data));
        }
        let mut insert_row = RowData::new(
            src_row_data.schema.clone(),
            src_row_data.tb.clone(),
            RowType::Insert,
            None,
            Some(after),
        );
        insert_row.refresh_data_size();
        self.build_rdb_query(&insert_row, false)
    }

    fn build_delete_sql(&self, dst_row_data: &RowData) -> anyhow::Result<Option<String>> {
        if matches!(self, CheckerTbMeta::Mongo(_)) {
            return Ok(mongo_cmd::build_delete_cmd(dst_row_data));
        }
        let dst_after = match &dst_row_data.after {
            Some(after) if !after.is_empty() => after.clone(),
            _ => return Ok(None),
        };
        let mut delete_row = RowData::new(
            dst_row_data.schema.clone(),
            dst_row_data.tb.clone(),
            RowType::Delete,
            Some(dst_after),
            None,
        );
        delete_row.refresh_data_size();
        self.build_rdb_query(&delete_row, false)
    }

    fn build_diff_sql(
        &self,
        src_row_data: &RowData,
        dst_row_data: &RowData,
        diff_col_values: &HashMap<String, DiffColValue>,
        match_full_row: bool,
    ) -> anyhow::Result<Option<String>> {
        if diff_col_values.is_empty() {
            return Ok(None);
        }
        if matches!(self, CheckerTbMeta::Mongo(_)) {
            return Ok(mongo_cmd::build_update_cmd(src_row_data, diff_col_values));
        }
        let Some(src_after) = src_row_data.require_after().ok() else {
            return Ok(None);
        };
        let update_after: HashMap<_, _> = diff_col_values
            .keys()
            .filter_map(|col| src_after.get(col).map(|v| (col.clone(), v.clone())))
            .collect();
        if update_after.is_empty() {
            return Ok(None);
        }

        let Some(update_before) = dst_row_data
            .require_after()
            .ok()
            .or_else(|| dst_row_data.require_before().ok())
            .filter(|m| !m.is_empty())
            .cloned()
        else {
            return Ok(None);
        };

        let mut update_row = RowData::new(
            src_row_data.schema.clone(),
            src_row_data.tb.clone(),
            RowType::Update,
            Some(update_before),
            Some(update_after),
        );
        update_row.refresh_data_size();
        self.build_rdb_query(&update_row, match_full_row)
    }

    fn build_rdb_query(
        &self,
        row_data: &RowData,
        match_full_row: bool,
    ) -> anyhow::Result<Option<String>> {
        match self {
            CheckerTbMeta::Mysql(meta) => {
                let meta_cow = if match_full_row {
                    let mut owned = meta.clone();
                    owned.basic.id_cols = owned.basic.cols.clone();
                    Cow::Owned(owned)
                } else {
                    Cow::Borrowed(meta)
                };
                RdbQueryBuilder::new_for_mysql(meta_cow.as_ref(), None)
                    .get_query_sql(row_data, false)
                    .map(Some)
            }
            CheckerTbMeta::Pg(meta) => {
                let meta_cow = if match_full_row {
                    let mut owned = meta.clone();
                    owned.basic.id_cols = owned.basic.cols.clone();
                    Cow::Owned(owned)
                } else {
                    Cow::Borrowed(meta)
                };
                RdbQueryBuilder::new_for_pg(meta_cow.as_ref(), None)
                    .get_query_sql(row_data, false)
                    .map(Some)
            }
            CheckerTbMeta::Mongo(_) => unreachable!("Mongo handled before build_rdb_query"),
        }
    }
}

enum CheckInconsistency {
    Miss,
    Diff(HashMap<String, DiffColValue>),
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CheckEntry {
    log: CheckLog,
    revise_sql: Option<String>,
    is_miss: bool,
    src_row_data: RowData,
}

const CHECKER_MAX_STORE_SIZE: usize = 100_000;

struct RetryItem {
    row: RowData,
    retries_left: u32,
    next_retry_at: Instant,
}

struct BoundedNdjsonBuffer {
    size_limit: usize,
    row_limit: usize,
    bytes: usize,
    lines: VecDeque<Vec<u8>>,
}

impl BoundedNdjsonBuffer {
    fn new(size_limit: usize, row_limit: usize) -> Self {
        Self {
            size_limit: size_limit.max(1),
            row_limit: row_limit.max(1),
            bytes: 0,
            lines: VecDeque::new(),
        }
    }

    fn push(&mut self, log: &CheckLog) {
        let Ok(line) = serde_json::to_vec(log) else {
            return;
        };
        let line_size = line.len() + 1;
        if line_size > self.size_limit {
            return;
        }
        while self.lines.len() >= self.row_limit || self.bytes + line_size > self.size_limit {
            let Some(front) = self.lines.pop_front() else {
                break;
            };
            self.bytes = self.bytes.saturating_sub(front.len() + 1);
        }
        if self.lines.len() >= self.row_limit || self.bytes + line_size > self.size_limit {
            return;
        }
        self.bytes += line_size;
        self.lines.push_back(line);
    }

    fn into_bytes(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.bytes);
        for line in self.lines {
            buf.extend_from_slice(&line);
            buf.push(b'\n');
        }
        buf
    }
}

#[derive(Clone)]
pub struct CheckContext {
    pub monitor: Arc<Monitor>,
    pub summary: CheckSummaryLog,
    pub output_revise_sql: bool,
    pub extractor_meta_manager: Option<RdbMetaManager>,
    pub reverse_router: RdbRouter,
    pub output_full_row: bool,
    pub revise_match_full_row: bool,
    pub global_summary: Option<Arc<Mutex<CheckSummaryLog>>>,
    pub batch_size: usize,
    pub retry_interval_secs: u64,
    pub max_retries: u32,
    pub is_cdc: bool,
    pub check_log_dir: String,
    pub cdc_check_log_max_file_size: u64,
    pub cdc_check_log_max_rows: usize,
    pub s3_output: Option<(Operator, String)>,
    pub cdc_check_log_interval_secs: u64,
    pub state_store: Option<Arc<SqlCheckerStateStore>>,
    pub expected_resume_position: Option<Position>,
}

pub struct FetchResult {
    pub tb_meta: Arc<CheckerTbMeta>,
    pub dst_rows: Vec<RowData>,
}

enum CheckerMsg {
    ProcessBatchSync {
        batch: Arc<Vec<RowData>>,
        tx: oneshot::Sender<anyhow::Result<()>>,
    },
    RecordCheckpoint {
        position: Position,
        tx: oneshot::Sender<anyhow::Result<()>>,
    },
    Close {
        position: Option<Position>,
    },
}

pub struct DataCheckerHandle {
    tx: mpsc::Sender<CheckerMsg>,
    join_handle: Option<JoinHandle<anyhow::Result<()>>>,
    is_cdc: bool,
}

impl DataCheckerHandle {
    pub fn spawn<C: Checker>(
        checker: C,
        task_id: String,
        ctx: CheckContext,
        buffer_size: usize,
        name: &str,
    ) -> Self {
        let is_cdc = ctx.is_cdc;
        let (tx, rx) = mpsc::channel::<CheckerMsg>(buffer_size.max(1));

        let check_job = DataChecker::new(checker, task_id, ctx, rx, name);
        let join_handle = tokio::spawn(async move { check_job.run().await });

        Self {
            tx,
            join_handle: Some(join_handle),
            is_cdc,
        }
    }

    pub async fn check_sync(&self, data: Arc<Vec<RowData>>) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.send_request(
            |tx| CheckerMsg::ProcessBatchSync { batch: data, tx },
            "Checker worker closed",
            "sync",
        )
        .await
    }

    pub async fn close_with_position(&mut self, position: Option<&Position>) -> anyhow::Result<()> {
        let _ = self
            .tx
            .send(CheckerMsg::Close {
                position: position.cloned(),
            })
            .await;
        if let Some(handle) = self.join_handle.take() {
            handle.await??;
        }
        Ok(())
    }

    pub async fn record_checkpoint(&self, position: &Position) -> anyhow::Result<()> {
        if !self.is_cdc {
            return Ok(());
        }
        self.send_request(
            |tx| CheckerMsg::RecordCheckpoint {
                position: position.clone(),
                tx,
            },
            "failed to send checker checkpoint msg",
            "checkpoint",
        )
        .await
    }

    async fn send_request<F>(
        &self,
        build_msg: F,
        send_error: &str,
        action: &str,
    ) -> anyhow::Result<()>
    where
        F: FnOnce(oneshot::Sender<anyhow::Result<()>>) -> CheckerMsg,
    {
        let (tx, rx) = oneshot::channel();
        if let Err(err) = self.tx.send(build_msg(tx)).await {
            return Err(anyhow::anyhow!("{}: {}", send_error, err));
        }
        Self::receive_result(rx, action).await
    }

    async fn receive_result(
        rx: oneshot::Receiver<anyhow::Result<()>>,
        action: &str,
    ) -> anyhow::Result<()> {
        rx.await
            .map_err(|err| anyhow::anyhow!("checker {} response dropped: {}", action, err))?
    }
}

#[async_trait]
pub trait Checker: Send + Sync + 'static {
    async fn fetch(&mut self, src_rows: &[&RowData]) -> anyhow::Result<FetchResult>;
    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

struct DataChecker<C: Checker> {
    checker: C,
    task_id: String,
    ctx: CheckContext,
    retry_queue: VecDeque<RetryItem>,
    retry_next_at: Option<Instant>,
    store: IndexMap<u128, CheckEntry>,
    rx: mpsc::Receiver<CheckerMsg>,
    name: String,
    store_dirty: bool,
    evicted_miss: usize,
    evicted_diff: usize,
    checkpoint_seq: u64,
    last_checkpoint_position: Option<Position>,
}

impl<C: Checker> DataChecker<C> {
    pub fn new(
        checker: C,
        task_id: String,
        ctx: CheckContext,
        rx: mpsc::Receiver<CheckerMsg>,
        name: &str,
    ) -> Self {
        Self {
            checker,
            task_id,
            ctx,
            retry_queue: VecDeque::new(),
            retry_next_at: None,
            store: IndexMap::new(),
            rx,
            name: name.to_string(),
            store_dirty: false,
            evicted_miss: 0,
            evicted_diff: 0,
            checkpoint_seq: 0,
            last_checkpoint_position: None,
        }
    }

    fn remember_error(first_error: &mut Option<anyhow::Error>, err: anyhow::Error) {
        first_error.get_or_insert(err);
    }

    async fn init_cdc_state(&mut self) {
        if !self.ctx.is_cdc {
            return;
        }
        let needs_recheck = match self.load_initial_state().await {
            Ok(needs_recheck) => needs_recheck,
            Err(err) => {
                log_warn!(
                    "Checker [{}] failed to load state, start from empty store: {}",
                    self.name,
                    err
                );
                return;
            }
        };
        if !needs_recheck {
            return;
        }
        if let Err(err) = self.run_recheck().await {
            log_warn!(
                "Checker [{}] failed to run recheck phase: {}",
                self.name,
                err
            );
        }
    }

    async fn shutdown_logged(&mut self, first_error: &mut Option<anyhow::Error>) {
        if let Err(err) = self.shutdown().await {
            log_error!("Checker [{}] close failed: {}", self.name, err);
            Self::remember_error(first_error, err);
        }
    }

    async fn maybe_snapshot_and_output(&mut self) {
        let summary_path = std::path::Path::new(&self.ctx.check_log_dir).join("summary.log");
        let summary_ready = std::fs::metadata(&summary_path)
            .map(|meta| meta.len() > 0)
            .unwrap_or(false);
        if !self.store_dirty && summary_ready {
            return;
        }
        match self.snapshot_and_output().await {
            Ok(()) => self.store_dirty = false,
            Err(err) => log_error!("Checker [{}] cdc output failed: {}", self.name, err),
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        log_info!("Checker [{}] started.", self.name);
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut first_error: Option<anyhow::Error> = None;
        self.init_cdc_state().await;
        let output_secs = self.ctx.cdc_check_log_interval_secs.max(1);
        let mut output_interval = tokio::time::interval(Duration::from_secs(output_secs));
        output_interval.tick().await;

        loop {
            tokio::select! {
                msg = self.rx.recv() => {
                    match msg {
                        Some(CheckerMsg::ProcessBatchSync { batch, tx }) => {
                            let result = self.check_batch(batch.as_slice(), true).await;
                            let _ = tx.send(result);
                        }
                        Some(CheckerMsg::RecordCheckpoint { position, tx }) => {
                            let result = self.record_checkpoint(position).await;
                            if let Err(err) = &result {
                                log_warn!("Checker [{}] checkpoint persist failed: {}", self.name, err);
                            }
                            let _ = tx.send(result);
                        }
                        Some(CheckerMsg::Close { position }) => {
                            if let Some(position) = position.filter(|p| !matches!(p, Position::None))
                            {
                                self.last_checkpoint_position = Some(position);
                            }
                            self.shutdown_logged(&mut first_error).await;
                            break;
                        }
                        None => {
                            self.shutdown_logged(&mut first_error).await;
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    if let Err(err) = self.process_due_retries().await {
                        log_error!("Checker [{}] retry failed: {}", self.name, err);
                        Self::remember_error(&mut first_error, err);
                    }
                }
                _ = output_interval.tick(), if self.ctx.is_cdc => {
                    self.maybe_snapshot_and_output().await;
                }
            }
        }
        log_info!("Checker [{}] stopped.", self.name);

        match first_error {
            Some(err) if self.ctx.is_cdc => {
                log_warn!(
                    "Checker [{}] had internal errors in CDC mode, ignored to keep pipeline running: {}",
                    self.name,
                    err
                );
                Ok(())
            }
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    async fn finish_summary_and_meta(&mut self) -> anyhow::Result<()> {
        let common = &mut self.ctx;
        let global_summary_opt = common.global_summary.clone();
        let summary = &mut common.summary;
        summary.end_time = chrono::Local::now().to_rfc3339();
        summary.is_consistent = summary.miss_count == 0 && summary.diff_count == 0;
        if let Some(global_summary) = global_summary_opt {
            global_summary.lock().await.merge(summary);
        } else if !common.is_cdc {
            log_summary!("{}", summary);
        }
        if let Some(meta_manager) = common.extractor_meta_manager.as_mut() {
            meta_manager.close().await
        } else {
            Ok(())
        }
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        if self.ctx.is_cdc {
            if self.store_dirty {
                if let Some(position) = self.last_checkpoint_position.clone() {
                    if let Err(err) = self.record_checkpoint(position).await {
                        log_warn!(
                            "Checker [{}] failed to persist final checkpoint: {}",
                            self.name,
                            err
                        );
                    } else {
                        self.store_dirty = false;
                    }
                }
            }
            if let Err(err) = self.snapshot_and_output().await {
                log_error!("Checker [{}] final cdc output failed: {}", self.name, err);
            }
            self.store.clear();
        } else {
            self.drain_retries().await?;
            self.flush_store();
        }
        self.finish_summary_and_meta().await?;
        self.checker.close().await
    }
}

pub fn split_query_rows<'a>(
    rows: &'a [RowData],
    id_cols: &[String],
) -> (Vec<&'a RowData>, Vec<&'a RowData>) {
    rows.iter().partition(|row| has_null_key(row, id_cols))
}

pub fn has_null_key(row_data: &RowData, id_cols: &[String]) -> bool {
    let col_values = match row_data.row_type {
        RowType::Delete => row_data.require_before().ok(),
        _ => row_data.require_after().ok(),
    };
    col_values.is_some_and(|vals| {
        id_cols
            .iter()
            .any(|col| matches!(vals.get(col), Some(ColValue::None) | None))
    })
}
