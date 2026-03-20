use anyhow::{anyhow, Context};
use async_mutex::Mutex;
use async_trait::async_trait;
use indexmap::IndexMap;
use mongodb::bson::Document;
use opendal::Operator;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex as StdMutex,
};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration, Instant};

use crate::{
    checker::check_log::{CheckLog, CheckSummaryLog, DiffColValue, TableCheckCount},
    checker::state_store::{CheckerCheckpointCommit, CheckerStateRow, CheckerStateStore},
    rdb_query_builder::RdbQueryBuilder,
    rdb_router::RdbRouter,
    sinker::base_sinker::BaseSinker,
    sinker::mongo::mongo_cmd,
};
use dt_common::meta::{
    col_value::ColValue, ddl_meta::ddl_data::DdlData, mongo::mongo_constant::MongoConstants,
    mysql::mysql_tb_meta::MysqlTbMeta, pg::pg_tb_meta::PgTbMeta, position::Position,
    rdb_meta_manager::RdbMetaManager, rdb_tb_meta::RdbTbMeta, row_data::RowData, row_type::RowType,
};
use dt_common::{
    log_diff, log_error, log_info, log_miss, log_sql, log_summary, log_warn,
    monitor::monitor::Monitor, utils::limit_queue::LimitedQueue,
};

#[path = "cdc_state.rs"]
mod cdc_state;
#[path = "checker_engine.rs"]
mod engine;

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
    pub state_store: Option<Arc<CheckerStateStore>>,
    pub expected_resume_position: Option<Position>,
}

pub struct FetchResult {
    pub tb_meta: Arc<CheckerTbMeta>,
    pub dst_rows: Vec<RowData>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CheckerStoreKey {
    schema: String,
    tb: String,
    row_key: u128,
}

impl CheckerStoreKey {
    fn from_row_data(row_data: &RowData, row_key: u128) -> Self {
        Self {
            schema: row_data.schema.clone(),
            tb: row_data.tb.clone(),
            row_key,
        }
    }
}

#[async_trait]
pub trait Checker: Send + Sync + 'static {
    async fn fetch(&mut self, src_rows: &[&RowData]) -> anyhow::Result<FetchResult>;
    async fn refresh_meta(&mut self, _data: &[DdlData]) -> anyhow::Result<()> {
        Ok(())
    }
    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

enum CheckerMsg {
    ProcessBatchAsync {
        batch: Vec<RowData>,
    },
    RecordCheckpoint {
        position: Position,
        tx: oneshot::Sender<anyhow::Result<()>>,
    },
    RefreshMeta {
        data: Vec<DdlData>,
        tx: oneshot::Sender<anyhow::Result<()>>,
    },
    Close {
        position: Option<Position>,
    },
}

#[derive(Default)]
struct CheckerRuntimeState {
    failed: AtomicBool,
    first_error: StdMutex<Option<String>>,
}

impl CheckerRuntimeState {
    fn mark_failed(&self, err: &anyhow::Error) {
        self.mark_failed_message(format!("{:#}", err));
    }

    fn mark_failed_message(&self, message: String) {
        self.failed.store(true, Ordering::Release);
        let mut guard = self.first_error.lock().unwrap();
        if guard.is_none() {
            *guard = Some(message);
        }
    }

    fn has_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    fn error(&self, context: &str) -> Option<anyhow::Error> {
        self.first_error
            .lock()
            .unwrap()
            .clone()
            .map(|message| anyhow!("{}: {}", context, message))
    }
}

#[derive(Clone)]
struct DataCheckerShared {
    tx: mpsc::Sender<CheckerMsg>,
    is_cdc: bool,
    persists_position_checkpoint: bool,
    runtime_state: Arc<CheckerRuntimeState>,
}

#[derive(Clone)]
pub struct DataCheckerHandle {
    shared: DataCheckerShared,
    join_handle: Arc<Mutex<Option<JoinHandle<anyhow::Result<()>>>>>,
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
        let persists_position_checkpoint = ctx.is_cdc && ctx.state_store.is_some();
        let (tx, rx) = mpsc::channel::<CheckerMsg>(buffer_size.max(1));
        let runtime_state = Arc::new(CheckerRuntimeState::default());

        let check_job = DataChecker::new(checker, task_id, ctx, rx, name, runtime_state.clone());
        let join_handle = tokio::spawn(async move { check_job.run().await });

        Self {
            shared: DataCheckerShared {
                tx,
                is_cdc,
                persists_position_checkpoint,
                runtime_state,
            },
            join_handle: Arc::new(Mutex::new(Some(join_handle))),
        }
    }

    pub async fn enqueue_check(&self, data: Vec<RowData>) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        if let Some(err) = self
            .shared
            .runtime_state
            .error("skip checker enqueue because checker already failed")
        {
            return Err(err);
        }
        self.shared
            .tx
            .send(CheckerMsg::ProcessBatchAsync { batch: data })
            .await
            .map_err(|err| anyhow::anyhow!("failed to enqueue checker batch: {}", err))
    }

    pub async fn close_with_position(&mut self, position: Option<&Position>) -> anyhow::Result<()> {
        let position = if self.shared.runtime_state.has_failed() {
            None
        } else {
            position.cloned()
        };
        let _ = self.shared.tx.send(CheckerMsg::Close { position }).await;
        if let Some(handle) = self.join_handle.lock().await.take() {
            handle.await??;
        }
        Ok(())
    }

    pub async fn record_checkpoint(&self, position: &Position) -> anyhow::Result<()> {
        if !self.shared.is_cdc {
            return Ok(());
        }
        if let Some(err) = self
            .shared
            .runtime_state
            .error("skip checker checkpoint because checker already failed")
        {
            return Err(err);
        }
        let (tx, rx) = oneshot::channel();
        self.shared
            .tx
            .send(CheckerMsg::RecordCheckpoint {
                position: position.clone(),
                tx,
            })
            .await
            .map_err(|err| anyhow::anyhow!("failed to send checker checkpoint msg: {}", err))?;
        rx.await
            .map_err(|err| anyhow::anyhow!("checker checkpoint response dropped: {}", err))?
    }

    pub async fn refresh_meta(&self, data: Vec<DdlData>) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        if let Some(err) = self
            .shared
            .runtime_state
            .error("skip checker refresh_meta because checker already failed")
        {
            return Err(err);
        }
        let (tx, rx) = oneshot::channel();
        self.shared
            .tx
            .send(CheckerMsg::RefreshMeta { data, tx })
            .await
            .map_err(|err| anyhow::anyhow!("failed to send checker refresh meta msg: {}", err))?;
        rx.await
            .map_err(|err| anyhow::anyhow!("checker refresh meta response dropped: {}", err))?
    }

    pub fn persists_position_checkpoint(&self) -> bool {
        self.shared.persists_position_checkpoint
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

struct RetryItem {
    row: RowData,
    retries_left: u32,
    next_retry_at: Instant,
}

struct BoundedLineBuffer {
    size_limit: usize,
    row_limit: Option<usize>,
    bytes: usize,
    lines: VecDeque<Vec<u8>>,
}

impl BoundedLineBuffer {
    fn new(size_limit: usize, row_limit: Option<usize>) -> Self {
        Self {
            size_limit: size_limit.max(1),
            row_limit: row_limit.map(|limit| limit.max(1)),
            bytes: 0,
            lines: VecDeque::new(),
        }
    }

    fn push_bytes(&mut self, line: Vec<u8>) {
        let line_size = line.len() + 1;
        if line_size > self.size_limit {
            return;
        }
        while self
            .row_limit
            .is_some_and(|limit| self.lines.len() >= limit)
            || self.bytes + line_size > self.size_limit
        {
            let Some(front) = self.lines.pop_front() else {
                break;
            };
            self.bytes = self.bytes.saturating_sub(front.len() + 1);
        }
        if self
            .row_limit
            .is_some_and(|limit| self.lines.len() >= limit)
            || self.bytes + line_size > self.size_limit
        {
            return;
        }
        self.bytes += line_size;
        self.lines.push_back(line);
    }

    fn push_str(&mut self, line: &str) {
        self.push_bytes(line.as_bytes().to_vec());
    }

    fn push_json<T: Serialize>(&mut self, value: &T) {
        let Ok(line) = serde_json::to_vec(value) else {
            return;
        };
        self.push_bytes(line);
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

struct DataChecker<C: Checker> {
    checker: C,
    task_id: String,
    ctx: CheckContext,
    runtime_state: Arc<CheckerRuntimeState>,
    retry_queue: VecDeque<RetryItem>,
    retry_next_at: Option<Instant>,
    store: IndexMap<CheckerStoreKey, CheckEntry>,
    rx: mpsc::Receiver<CheckerMsg>,
    name: String,
    store_dirty: bool,
    last_checkpoint_position: Option<Position>,
    snapshot_dirty: bool,
}

impl<C: Checker> DataChecker<C> {
    pub fn new(
        checker: C,
        task_id: String,
        ctx: CheckContext,
        rx: mpsc::Receiver<CheckerMsg>,
        name: &str,
        runtime_state: Arc<CheckerRuntimeState>,
    ) -> Self {
        Self {
            checker,
            task_id,
            ctx,
            runtime_state,
            retry_queue: VecDeque::new(),
            retry_next_at: None,
            store: IndexMap::new(),
            rx,
            name: name.to_string(),
            store_dirty: false,
            last_checkpoint_position: None,
            snapshot_dirty: true,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        log_info!("Checker [{}] started.", self.name);
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut first_error: Option<anyhow::Error> = None;
        if let Err(err) = self.init_cdc_state().await {
            log_error!(
                "Checker [{}] failed to initialize CDC state: {}",
                self.name,
                err
            );
            self.runtime_state.mark_failed(&err);
            Self::remember_error(&mut first_error, err);
            self.shutdown_logged(&mut first_error).await;
            log_info!("Checker [{}] stopped.", self.name);
            return Err(first_error.expect("init failure should set first_error"));
        }
        let output_secs = self.ctx.cdc_check_log_interval_secs.max(1);
        let mut output_interval = tokio::time::interval(Duration::from_secs(output_secs));
        output_interval.tick().await;

        loop {
            tokio::select! {
                msg = self.rx.recv() => {
                    match msg {
                        Some(CheckerMsg::ProcessBatchAsync { batch }) => {
                            if let Err(err) = self.check_batch(&batch, true).await {
                                log_error!("Checker [{}] async batch failed: {}", self.name, err);
                                self.runtime_state.mark_failed(&err);
                                Self::remember_error(&mut first_error, err);
                            }
                        }
                        Some(CheckerMsg::RecordCheckpoint { position, tx }) => {
                            let result = if let Some(err) = self.runtime_state.error(
                                "skip checker checkpoint because checker already failed",
                            ) {
                                Err(err)
                            } else {
                                self.record_checkpoint(position).await
                            };
                            if let Err(err) = &result {
                                log_error!("Checker [{}] checkpoint persist failed: {}", self.name, err);
                                self.runtime_state.mark_failed(err);
                                Self::remember_error(&mut first_error, anyhow!("{:#}", err));
                            }
                            let _ = tx.send(result);
                        }
                        Some(CheckerMsg::RefreshMeta { data, tx }) => {
                            let result = if let Some(err) = self.runtime_state.error(
                                "skip checker refresh_meta because checker already failed",
                            ) {
                                Err(err)
                            } else {
                                self.checker.refresh_meta(&data).await
                            };
                            if let Err(err) = &result {
                                log_error!("Checker [{}] refresh meta failed: {}", self.name, err);
                                self.runtime_state.mark_failed(err);
                                Self::remember_error(&mut first_error, anyhow!("{:#}", err));
                            }
                            let _ = tx.send(result);
                        }
                        Some(CheckerMsg::Close { position }) => {
                            if let Some(position) = position
                                .filter(|p| !matches!(p, Position::None))
                                .filter(|_| !self.runtime_state.has_failed())
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
                        self.runtime_state.mark_failed(&err);
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
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        let shutdown_result = if self.ctx.is_cdc {
            self.shutdown_cdc().await
        } else {
            self.shutdown_non_cdc().await
        };
        let summary_result = self.finish_summary_and_meta().await;
        let close_result = self.checker.close().await;

        shutdown_result?;
        summary_result?;
        close_result?;
        Ok(())
    }

    async fn shutdown_cdc(&mut self) -> anyhow::Result<()> {
        if self.store_dirty && !self.runtime_state.has_failed() {
            if let Some(position) = self.last_checkpoint_position.clone() {
                self.record_checkpoint(position).await.with_context(|| {
                    format!("Checker [{}] failed to persist final checkpoint", self.name)
                })?;
                self.store_dirty = false;
            }
        }

        let output_result = self
            .snapshot_and_output()
            .await
            .with_context(|| format!("Checker [{}] final cdc output failed", self.name));
        self.store.clear();
        self.update_pending_counter();
        output_result
    }

    async fn shutdown_non_cdc(&mut self) -> anyhow::Result<()> {
        self.drain_retries().await?;
        self.flush_store().await;
        Ok(())
    }

    async fn finish_summary_and_meta(&mut self) -> anyhow::Result<()> {
        let common = &mut self.ctx;
        let summary = &mut common.summary;
        summary.end_time = chrono::Local::now().to_rfc3339();
        summary.is_consistent = summary.miss_count == 0 && summary.diff_count == 0;
        if let Some(global_summary) = common.global_summary.clone() {
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

    async fn init_cdc_state(&mut self) -> anyhow::Result<()> {
        if !self.ctx.is_cdc {
            return Ok(());
        }
        let needs_recheck = self.load_initial_state().await?;
        if !needs_recheck {
            return Ok(());
        }
        self.run_recheck().await
    }

    async fn maybe_snapshot_and_output(&mut self) {
        if !self.snapshot_dirty {
            return;
        }
        match self.snapshot_and_output().await {
            Ok(()) => {
                self.snapshot_dirty = false;
            }
            Err(err) => log_error!("Checker [{}] cdc output failed: {}", self.name, err),
        }
    }

    async fn shutdown_logged(&mut self, first_error: &mut Option<anyhow::Error>) {
        if let Err(err) = self.shutdown().await {
            log_error!("Checker [{}] close failed: {}", self.name, err);
            Self::remember_error(first_error, err);
        }
    }

    fn remember_error(first_error: &mut Option<anyhow::Error>, err: anyhow::Error) {
        first_error.get_or_insert(err);
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{checker::check_log::CheckSummaryLog, rdb_router::RdbRouter};
    use async_trait::async_trait;
    use dt_common::{
        config::config_enums::DbType, meta::ddl_meta::ddl_statement::MysqlAlterTableStatement,
    };
    use std::{
        fs,
        sync::atomic::{AtomicUsize, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    struct RefreshOnlyChecker {
        refresh_count: Arc<AtomicUsize>,
    }

    struct NoopChecker;
    struct FailingFetchChecker;

    #[async_trait]
    impl Checker for RefreshOnlyChecker {
        async fn fetch(&mut self, _src_rows: &[&RowData]) -> anyhow::Result<FetchResult> {
            unreachable!("refresh_meta test should not call fetch")
        }

        async fn refresh_meta(&mut self, data: &[DdlData]) -> anyhow::Result<()> {
            self.refresh_count.fetch_add(data.len(), Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl Checker for NoopChecker {
        async fn fetch(&mut self, _src_rows: &[&RowData]) -> anyhow::Result<FetchResult> {
            unreachable!("noop checker test should not call fetch")
        }
    }

    #[async_trait]
    impl Checker for FailingFetchChecker {
        async fn fetch(&mut self, _src_rows: &[&RowData]) -> anyhow::Result<FetchResult> {
            anyhow::bail!("forced checker fetch failure")
        }
    }

    fn build_check_context(check_log_dir: String, is_cdc: bool, start_time: &str) -> CheckContext {
        CheckContext {
            monitor: Arc::new(Monitor::new("checker", "unit-test", 1, 1, 1)),
            summary: CheckSummaryLog {
                start_time: start_time.to_string(),
                ..Default::default()
            },
            output_revise_sql: false,
            extractor_meta_manager: None,
            reverse_router: RdbRouter {
                schema_map: HashMap::new(),
                tb_map: HashMap::new(),
                col_map: HashMap::new(),
                topic_map: HashMap::new(),
            },
            output_full_row: false,
            revise_match_full_row: false,
            global_summary: None,
            batch_size: 1,
            retry_interval_secs: 0,
            max_retries: 0,
            is_cdc,
            check_log_dir,
            cdc_check_log_max_file_size: 1,
            cdc_check_log_max_rows: 1,
            s3_output: None,
            cdc_check_log_interval_secs: 1,
            state_store: None,
            expected_resume_position: None,
        }
    }

    fn build_temp_dir(prefix: &str) -> String {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("ape_dts_{prefix}_{unique}"));
        fs::create_dir_all(&path).expect("failed to create temp dir");
        path.to_string_lossy().into_owned()
    }

    fn build_row_data() -> RowData {
        RowData::new(
            "test_db".to_string(),
            "test_tb".to_string(),
            RowType::Insert,
            None,
            Some(HashMap::from([("id".to_string(), ColValue::Long(1))])),
        )
    }

    fn build_cdc_checker(check_log_dir: String, start_time: &str) -> DataChecker<NoopChecker> {
        let (_tx, rx) = mpsc::channel(1);
        DataChecker::new(
            NoopChecker,
            "checker-cdc-test".to_string(),
            build_check_context(check_log_dir, true, start_time),
            rx,
            "NoopChecker",
            Arc::new(CheckerRuntimeState::default()),
        )
    }

    #[tokio::test]
    async fn refresh_meta_message_reaches_checker_worker() {
        let refresh_count = Arc::new(AtomicUsize::new(0));
        let mut handle = DataCheckerHandle::spawn(
            RefreshOnlyChecker {
                refresh_count: refresh_count.clone(),
            },
            "checker-refresh-meta-test".to_string(),
            build_check_context(".".to_string(), false, ""),
            1,
            "RefreshOnlyChecker",
        );

        let ddl = DdlData {
            default_schema: "test_db".to_string(),
            db_type: DbType::Mysql,
            statement: dt_common::meta::ddl_meta::ddl_statement::DdlStatement::MysqlAlterTable(
                MysqlAlterTableStatement {
                    db: "test_db".to_string(),
                    tb: "test_tb".to_string(),
                    unparsed: String::new(),
                },
            ),
            ..Default::default()
        };

        handle.refresh_meta(vec![ddl]).await.unwrap();
        assert_eq!(refresh_count.load(Ordering::SeqCst), 1);
        handle.close_with_position(None).await.unwrap();
    }

    #[tokio::test]
    async fn first_periodic_snapshot_overwrites_stale_summary_file() {
        let dir = build_temp_dir("checker_stale_summary");
        fs::write(format!("{dir}/summary.log"), "stale-summary\n").unwrap();

        let mut checker = build_cdc_checker(dir.clone(), "fresh-run");
        checker.maybe_snapshot_and_output().await;

        let summary_raw = fs::read_to_string(format!("{dir}/summary.log")).unwrap();
        let summary: CheckSummaryLog = serde_json::from_str(summary_raw.trim()).unwrap();
        assert_eq!(summary.start_time, "fresh-run");
        assert_eq!(summary.miss_count, 0);
        assert_eq!(summary.diff_count, 0);

        fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn failed_async_batch_blocks_subsequent_checkpoint() {
        let mut handle = DataCheckerHandle::spawn(
            FailingFetchChecker,
            "checker-checkpoint-gate-test".to_string(),
            build_check_context(".".to_string(), true, ""),
            4,
            "FailingFetchChecker",
        );

        handle.enqueue_check(vec![build_row_data()]).await.unwrap();
        let checkpoint = Position::RdbSnapshotFinished {
            db_type: "mysql".to_string(),
            schema: "test_db".to_string(),
            tb: "test_tb".to_string(),
        };

        let result = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let checkpoint_result = handle.record_checkpoint(&checkpoint).await;
                if checkpoint_result.is_err() {
                    break checkpoint_result;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("checkpoint should stop after checker failure");

        assert!(result.is_err());
        assert!(handle.close_with_position(Some(&checkpoint)).await.is_err());
    }
}
