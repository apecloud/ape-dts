use async_mutex::Mutex;
use async_trait::async_trait;
use chrono::Local;
use indexmap::{IndexMap, IndexSet};
use opendal::Operator;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex as StdMutex,
};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

use super::struct_checker::StructCheckerHandle;
use crate::{
    checker::check_log::{
        CheckLog, CheckLogJsonExt, CheckSummaryLog, CheckTableSummaryLog, DiffColValue,
    },
    checker::state_store::CheckerStateStore,
    rdb_query_builder::RdbQueryBuilder,
    rdb_router::RdbRouter,
    sinker::mongo::mongo_cmd,
};
use dt_common::meta::{
    col_value::ColValue, ddl_meta::ddl_data::DdlData, mysql::mysql_tb_meta::MysqlTbMeta,
    pg::pg_tb_meta::PgTbMeta, position::Position, rdb_meta_manager::RdbMetaManager,
    rdb_tb_meta::RdbTbMeta, row_data::RowData, row_type::RowType,
    struct_meta::struct_data::StructData,
};
use dt_common::{
    log_error, log_info, log_summary, log_warn,
    monitor::{monitor::Monitor, task_monitor::TaskMonitor},
    utils::limit_queue::LimitedQueue,
};

#[path = "cdc_state.rs"]
mod cdc_state;
#[path = "checker_engine.rs"]
mod checker_engine;

pub const CHECKER_MAX_QUERY_BATCH: usize = 1000;

pub(super) fn is_summary_consistent(summary: &CheckSummaryLog, init_failed: bool) -> bool {
    !init_failed && summary.miss_count == 0 && summary.diff_count == 0 && summary.skip_count == 0
}

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
    pub task_monitor: Option<Arc<TaskMonitor>>,
    pub summary: CheckSummaryLog,
    pub output_revise_sql: bool,
    pub extractor_meta_manager: Option<RdbMetaManager>,
    pub reverse_router: RdbRouter,
    pub output_full_row: bool,
    pub revise_match_full_row: bool,
    pub global_summary: Option<Arc<Mutex<CheckSummaryLog>>>,
    pub batch_size: usize,
    pub sample_rate: Option<u8>,
    pub retry_interval_secs: u64,
    pub max_retries: u32,
    pub is_cdc: bool,
    pub check_log_dir: String,
    pub cdc_check_log_max_file_size: u64,
    pub cdc_check_log_max_rows: usize,
    pub s3_output: Option<(Operator, String)>,
    pub cdc_check_log_interval_secs: u64,
    pub state_store: Option<Arc<CheckerStateStore>>,
    pub source_checker: Option<Arc<Mutex<Box<dyn Checker>>>>,
    pub expected_resume_position: Option<Position>,
}

impl CheckContext {
    fn record_row_table_counts(&mut self, row: &RowData, checked_count: usize, skip_count: usize) {
        if checked_count == 0 && skip_count == 0 {
            return;
        }
        let (schema, tb) = self.reverse_router.get_tb_map(&row.schema, &row.tb);
        let has_target = row.schema != schema || row.tb != tb;
        self.summary.merge_table(CheckTableSummaryLog {
            schema: schema.to_string(),
            tb: tb.to_string(),
            target_schema: has_target.then(|| row.schema.clone()),
            target_tb: has_target.then(|| row.tb.clone()),
            checked_count,
            skip_count,
            ..Default::default()
        });
    }

    fn record_entry_table_counts(&mut self, entry: &CheckEntry) {
        let (miss_count, diff_count) = if entry.is_miss() {
            (1, 0)
        } else if entry.counts_as_diff() {
            (0, 1)
        } else {
            return;
        };
        self.summary.merge_table(CheckTableSummaryLog {
            schema: entry.log.schema.clone(),
            tb: entry.log.tb.clone(),
            target_schema: entry.log.target_schema.clone(),
            target_tb: entry.log.target_tb.clone(),
            miss_count,
            diff_count,
            ..Default::default()
        });
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CheckerStoreKey {
    schema: String,
    tb: String,
    row_key: u128,
}

impl CheckerStoreKey {
    fn new(schema: &str, tb: &str, row_key: u128) -> Self {
        Self {
            schema: schema.to_string(),
            tb: tb.to_string(),
            row_key,
        }
    }
}

#[async_trait]
pub trait Checker: Send + Sync + 'static {
    async fn load_table_meta(&mut self, lookup_row: &RowData)
        -> anyhow::Result<Arc<CheckerTbMeta>>;
    async fn fetch_rows_by_keys(
        &mut self,
        table_meta: Arc<CheckerTbMeta>,
        lookup_rows: &[&RowData],
    ) -> anyhow::Result<Vec<RowData>>;
    async fn refresh_meta(&mut self, _data: &[DdlData]) -> anyhow::Result<()> {
        Ok(())
    }
}

enum CheckerControlMsg {
    RecordCheckpoint { position: Position },
    RefreshMeta { data: Vec<DdlData> },
    Close { position: Option<Position> },
}

#[derive(Clone)]
struct DataCheckerShared {
    batch_queue: Arc<StdMutex<LimitedQueue<Vec<RowData>>>>,
    batch_notify: Arc<Notify>,
    control_tx: mpsc::UnboundedSender<CheckerControlMsg>,
    dropped_batches: Arc<AtomicU64>,
    dropped_items: Arc<AtomicU64>,
    is_cdc: bool,
}

#[derive(Clone)]
pub struct DataCheckerHandle {
    shared: DataCheckerShared,
    join_handle: Arc<Mutex<Option<JoinHandle<anyhow::Result<()>>>>>,
}

pub enum CheckerHandle {
    Data(DataCheckerHandle),
    Struct(StructCheckerHandle),
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
        let batch_queue = Arc::new(StdMutex::new(LimitedQueue::new(buffer_size.max(1))));
        let batch_notify = Arc::new(Notify::new());
        let dropped_items = Arc::new(AtomicU64::new(0));
        let (control_tx, control_rx) = mpsc::unbounded_channel::<CheckerControlMsg>();

        let check_job = DataChecker::new(
            checker,
            task_id,
            ctx,
            CheckerIo {
                batch_queue: batch_queue.clone(),
                batch_notify: batch_notify.clone(),
                dropped_items: dropped_items.clone(),
                control_rx,
            },
            name,
        );
        let join_handle = tokio::spawn(async move { check_job.run().await });

        Self {
            shared: DataCheckerShared {
                batch_queue,
                batch_notify,
                control_tx,
                dropped_batches: Arc::new(AtomicU64::new(0)),
                dropped_items,
                is_cdc,
            },
            join_handle: Arc::new(Mutex::new(Some(join_handle))),
        }
    }

    pub async fn enqueue_check(&self, data: Vec<RowData>) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let dropped = {
            let mut queue = self.shared.batch_queue.lock().unwrap();
            queue.push_with_eviction(data)
        };
        if let Some(dropped) = dropped {
            self.shared.dropped_items.fetch_add(
                u64::try_from(dropped.len()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            let dropped_batches = self.shared.dropped_batches.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped_batches == 1 || dropped_batches % 100 == 0 {
                log_warn!(
                    "checker queue overflowed; dropped oldest batch, total dropped batches: {}",
                    dropped_batches
                );
            }
        }
        self.shared.batch_notify.notify_one();
        Ok(())
    }

    pub async fn close_with_position(&mut self, position: Option<&Position>) -> anyhow::Result<()> {
        if self
            .shared
            .control_tx
            .send(CheckerControlMsg::Close {
                position: position.cloned(),
            })
            .is_err()
        {
            log_warn!("checker close signal dropped because checker already stopped");
        }
        self.shared.batch_notify.notify_one();
        if let Some(handle) = self.join_handle.lock().await.take() {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => log_warn!("checker task ended with error: {}", err),
                Err(err) => log_warn!("checker task join failed: {}", err),
            }
        }
        Ok(())
    }

    pub async fn record_checkpoint(&self, position: &Position) -> anyhow::Result<()> {
        if !self.shared.is_cdc {
            return Ok(());
        }
        if self
            .shared
            .control_tx
            .send(CheckerControlMsg::RecordCheckpoint {
                position: position.clone(),
            })
            .is_err()
        {
            log_warn!("checker checkpoint signal dropped because checker already stopped");
        }
        self.shared.batch_notify.notify_one();
        Ok(())
    }

    pub async fn refresh_meta(&self, data: Vec<DdlData>) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        if self
            .shared
            .control_tx
            .send(CheckerControlMsg::RefreshMeta { data })
            .is_err()
        {
            log_warn!("checker refresh_meta signal dropped because checker already stopped");
        }
        Ok(())
    }
}

impl CheckerHandle {
    pub async fn close_with_position(&mut self, position: Option<&Position>) -> anyhow::Result<()> {
        match self {
            Self::Data(handle) => handle.close_with_position(position).await,
            Self::Struct(handle) => handle.close().await,
        }
    }

    pub async fn record_checkpoint(&self, position: &Position) -> anyhow::Result<()> {
        match self {
            Self::Data(handle) => handle.record_checkpoint(position).await,
            Self::Struct(_) => Ok(()),
        }
    }

    pub async fn refresh_meta(&self, data: Vec<DdlData>) -> anyhow::Result<()> {
        match self {
            Self::Data(handle) => handle.refresh_meta(data).await,
            Self::Struct(_) => Ok(()),
        }
    }

    pub async fn check_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        match self {
            Self::Data(_) => Ok(()),
            Self::Struct(handle) => handle.check_struct(data).await,
        }
    }
}

enum CheckInconsistency {
    Miss,
    Diff(HashMap<String, DiffColValue>),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct RecheckKey {
    schema: String,
    tb: String,
    is_delete: bool,
    #[serde(with = "dt_common::meta::tagged_col_value_map")]
    pk: BTreeMap<String, ColValue>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CheckEntry {
    key: RecheckKey,
    log: CheckLog,
    revise_sql: Option<String>,
    diff_cols: Option<Vec<String>>,
}

impl RecheckKey {
    fn from_row_data(row_data: &RowData, id_cols: &[String]) -> anyhow::Result<Self> {
        let values = match row_data.row_type {
            RowType::Delete => row_data.require_before()?,
            _ => row_data.require_after()?,
        };
        let pk = id_cols
            .iter()
            .map(|col| {
                values
                    .get(col)
                    .cloned()
                    .map(|value| (col.clone(), value))
                    .ok_or_else(|| anyhow::anyhow!("missing id col value: {col}"))
            })
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        Ok(Self {
            schema: row_data.schema.clone(),
            tb: row_data.tb.clone(),
            is_delete: row_data.row_type == RowType::Delete,
            pk,
        })
    }

    fn to_lookup_row(&self) -> RowData {
        let values = self
            .pk
            .iter()
            .map(|(col, value)| (col.clone(), value.clone()))
            .collect::<HashMap<_, _>>();
        if self.is_delete {
            RowData::new(
                self.schema.clone(),
                self.tb.clone(),
                RowType::Delete,
                Some(values),
                None,
            )
        } else {
            RowData::new(
                self.schema.clone(),
                self.tb.clone(),
                RowType::Insert,
                None,
                Some(values),
            )
        }
    }
}

impl CheckEntry {
    fn is_miss(&self) -> bool {
        self.diff_cols.is_none()
    }

    fn counts_as_diff(&self) -> bool {
        self.diff_cols.is_some()
    }
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

    fn push_json<T: Serialize>(&mut self, value: &T) -> bool {
        let line = match serde_json::to_vec(value) {
            Ok(line) => line,
            Err(e) => {
                log_warn!(
                    "Skipping checker JSON output because serialization failed: {}",
                    e
                );
                return false;
            }
        };
        self.push_bytes(line);
        true
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
    retry_queue: VecDeque<RetryItem>,
    retry_next_at: Option<Instant>,
    store: IndexMap<CheckerStoreKey, CheckEntry>,
    dirty_upserts: IndexSet<CheckerStoreKey>,
    dirty_deletes: IndexMap<CheckerStoreKey, String>,
    batch_queue: Arc<StdMutex<LimitedQueue<Vec<RowData>>>>,
    batch_notify: Arc<Notify>,
    dropped_items: Arc<AtomicU64>,
    control_rx: mpsc::UnboundedReceiver<CheckerControlMsg>,
    pending_controls: VecDeque<CheckerControlMsg>,
    name: String,
    // Tracks store changes since the last DB checkpoint and is cleared by `record_checkpoint`.
    store_dirty: bool,
    last_checkpoint_position: Option<Position>,
    persisted_identity_keys: Option<BTreeSet<String>>,
    // Tracks store or summary changes since the last log or S3 output and is cleared by `snapshot_and_output`.
    snapshot_dirty: bool,
    // Set when `init_cdc_state` fails to avoid overwriting historical inconsistency records.
    init_failed: bool,
    close_requested: bool,
}

struct CheckerIo {
    batch_queue: Arc<StdMutex<LimitedQueue<Vec<RowData>>>>,
    batch_notify: Arc<Notify>,
    dropped_items: Arc<AtomicU64>,
    control_rx: mpsc::UnboundedReceiver<CheckerControlMsg>,
}

impl<C: Checker> DataChecker<C> {
    pub fn new(
        checker: C,
        task_id: String,
        mut ctx: CheckContext,
        io: CheckerIo,
        name: &str,
    ) -> Self {
        if ctx.summary.start_time.is_empty() {
            ctx.summary.start_time = Local::now().to_rfc3339();
        }
        let persisted_identity_keys = ctx.state_store.as_ref().map(|_| BTreeSet::new());
        Self {
            checker,
            task_id,
            ctx,
            retry_queue: VecDeque::new(),
            retry_next_at: None,
            store: IndexMap::new(),
            dirty_upserts: IndexSet::new(),
            dirty_deletes: IndexMap::new(),
            batch_queue: io.batch_queue,
            batch_notify: io.batch_notify,
            dropped_items: io.dropped_items,
            control_rx: io.control_rx,
            pending_controls: VecDeque::new(),
            name: name.to_string(),
            store_dirty: false,
            last_checkpoint_position: None,
            persisted_identity_keys,
            snapshot_dirty: true,
            init_failed: false,
            close_requested: false,
        }
    }

    fn account_dropped_item_skips(&mut self) {
        let delta = self.dropped_items.swap(0, Ordering::Relaxed);
        if delta == 0 {
            return;
        }
        self.ctx.summary.skip_count = self
            .ctx
            .summary
            .skip_count
            .saturating_add(usize::try_from(delta).unwrap_or(usize::MAX));
        if self.ctx.is_cdc {
            self.snapshot_dirty = true;
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        log_info!("Checker [{}] started.", self.name);
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        if let Err(err) = self.init_cdc_state().await {
            log_error!(
                "Checker [{}] failed to initialize CDC state: {}",
                self.name,
                err
            );
            self.init_failed = true;
        }
        let output_secs = self.ctx.cdc_check_log_interval_secs.max(1);
        let mut output_interval = tokio::time::interval(Duration::from_secs(output_secs));
        output_interval.tick().await;

        loop {
            if self.close_requested {
                self.drain_pending_batches().await;
                break;
            }

            tokio::select! {
                biased;
                msg = self.control_rx.recv() => {
                    match msg {
                        Some(msg) => {
                            self.pending_controls.push_back(msg);
                            self.drain_pending_batches().await;
                        }
                        None => self.close_requested = true,
                    }
                }
                _ = self.batch_notify.notified() => {
                    self.drain_pending_batches().await;
                }
                _ = interval.tick() => {
                    if let Err(err) = self.process_due_retries().await {
                        log_error!("Checker [{}] retry failed: {}", self.name, err);
                    }
                }
                _ = output_interval.tick(), if self.ctx.is_cdc => {
                    if let Err(err) = self.maybe_snapshot_and_output().await {
                        log_error!("Checker [{}] cdc output failed: {}", self.name, err);
                    }
                }
            }
        }
        if let Err(err) = self.shutdown().await {
            log_error!("Checker [{}] shutdown failed: {}", self.name, err);
        }
        log_info!("Checker [{}] stopped.", self.name);
        Ok(())
    }

    async fn handle_control_msg(&mut self, msg: CheckerControlMsg) {
        match msg {
            CheckerControlMsg::RecordCheckpoint { position } => {
                if let Err(err) = self.record_checkpoint(position).await {
                    log_error!("Checker [{}] checkpoint failed: {}", self.name, err);
                }
            }
            CheckerControlMsg::RefreshMeta { data } => {
                if let Err(err) = self.checker.refresh_meta(&data).await {
                    log_error!("Checker [{}] refresh_meta failed: {}", self.name, err);
                }
            }
            CheckerControlMsg::Close { position } => {
                if let Some(position) = position.filter(|p| !matches!(p, Position::None)) {
                    self.last_checkpoint_position = Some(position);
                }
                self.close_requested = true;
            }
        }
    }

    fn collect_pending_controls(&mut self) {
        while let Ok(msg) = self.control_rx.try_recv() {
            self.pending_controls.push_back(msg);
        }
    }

    async fn drain_pending_batches(&mut self) {
        loop {
            self.account_dropped_item_skips();
            self.collect_pending_controls();

            let batch = {
                let mut queue = self.batch_queue.lock().unwrap();
                queue.pop()
            };
            let Some(batch) = batch else {
                if let Some(msg) = self.pending_controls.pop_front() {
                    self.handle_control_msg(msg).await;
                    continue;
                }
                return;
            };

            if let Err(err) = self.check_batch(&batch, true).await {
                log_error!("Checker [{}] batch failed: {}", self.name, err);
            }
        }
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        if self.ctx.is_cdc {
            if self.store_dirty {
                if let Some(position) = self.last_checkpoint_position.clone() {
                    if let Err(err) = self.record_checkpoint(position).await {
                        log_error!("Checker [{}] final checkpoint failed: {}", self.name, err);
                    }
                }
            }
            if let Err(err) = self.snapshot_and_output().await {
                log_error!("Checker [{}] final output failed: {}", self.name, err);
            }
            self.store.clear();
            self.update_pending_counter();
        } else {
            if let Err(err) = self.drain_retries().await {
                log_error!("Checker [{}] drain retries failed: {}", self.name, err);
            }
            self.flush_store().await;
        }
        self.finish_summary_and_meta().await?;
        Ok(())
    }

    async fn finish_summary_and_meta(&mut self) -> anyhow::Result<()> {
        self.account_dropped_item_skips();
        self.finish_local_summary();
        let common = &mut self.ctx;
        let summary = &mut common.summary;
        if let Some(global_summary) = common.global_summary.clone() {
            global_summary.lock().await.merge(summary);
        } else if !common.is_cdc {
            if let Some(log) = summary.to_json_line() {
                log_summary!("{}", log);
            }
        }
        if let Some(meta_manager) = common.extractor_meta_manager.as_mut() {
            meta_manager.close().await
        } else {
            Ok(())
        }
    }

    fn finish_local_summary(&mut self) {
        let summary = &mut self.ctx.summary;
        summary.end_time = Local::now().to_rfc3339();
        summary.is_consistent = is_summary_consistent(summary, self.init_failed);
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

    async fn maybe_snapshot_and_output(&mut self) -> anyhow::Result<()> {
        if !self.snapshot_dirty {
            return Ok(());
        }
        self.snapshot_and_output().await?;
        self.snapshot_dirty = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{checker::check_log::CheckSummaryLog, rdb_router::RdbRouter};
    use async_trait::async_trait;
    use dt_common::{meta::row_type::RowType, monitor::monitor::Monitor};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::{
        sync::{mpsc, Notify},
        time::{timeout, Duration},
    };

    #[derive(Clone)]
    struct BlockingFetchChecker {
        fetch_started: mpsc::UnboundedSender<()>,
        fetch_gate: Arc<Notify>,
    }

    #[async_trait]
    impl Checker for BlockingFetchChecker {
        async fn load_table_meta(
            &mut self,
            _lookup_row: &RowData,
        ) -> anyhow::Result<Arc<CheckerTbMeta>> {
            Ok(Arc::new(CheckerTbMeta::Mongo(RdbTbMeta {
                schema: "s1".to_string(),
                tb: "t1".to_string(),
                id_cols: vec!["id".to_string()],
                ..Default::default()
            })))
        }

        async fn fetch_rows_by_keys(
            &mut self,
            _table_meta: Arc<CheckerTbMeta>,
            _lookup_rows: &[&RowData],
        ) -> anyhow::Result<Vec<RowData>> {
            let _ = self.fetch_started.send(());
            self.fetch_gate.notified().await;
            Err(anyhow::anyhow!("unit-test fetch failure"))
        }
    }

    fn build_ctx() -> CheckContext {
        CheckContext {
            monitor: Arc::new(Monitor::new("checker", "unit-test", 1, 1, 1)),
            task_monitor: None,
            summary: CheckSummaryLog::default(),
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
            sample_rate: None,
            retry_interval_secs: 0,
            max_retries: 0,
            is_cdc: false,
            check_log_dir: String::new(),
            cdc_check_log_max_file_size: 1,
            cdc_check_log_max_rows: 1,
            s3_output: None,
            cdc_check_log_interval_secs: 1,
            state_store: None,
            source_checker: None,
            expected_resume_position: None,
        }
    }

    fn build_row(id: i32) -> RowData {
        RowData::new(
            "s1".to_string(),
            "t1".to_string(),
            RowType::Insert,
            None,
            Some(HashMap::from([("id".to_string(), ColValue::Long(id))])),
        )
    }

    #[tokio::test]
    async fn enqueue_check_tracks_dropped_item_count() {
        let (control_tx, _control_rx) = mpsc::unbounded_channel();
        let handle = DataCheckerHandle {
            shared: DataCheckerShared {
                batch_queue: Arc::new(StdMutex::new(LimitedQueue::new(1))),
                batch_notify: Arc::new(Notify::new()),
                control_tx,
                dropped_batches: Arc::new(AtomicU64::new(0)),
                dropped_items: Arc::new(AtomicU64::new(0)),
                is_cdc: false,
            },
            join_handle: Arc::new(Mutex::new(None)),
        };

        handle
            .enqueue_check(vec![build_row(1), build_row(2)])
            .await
            .unwrap();
        handle.enqueue_check(vec![build_row(3)]).await.unwrap();

        assert_eq!(handle.shared.dropped_batches.load(Ordering::Relaxed), 1);
        assert_eq!(handle.shared.dropped_items.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn record_checkpoint_does_not_wait_for_older_batches_to_finish() {
        let (fetch_started_tx, mut fetch_started_rx) = mpsc::unbounded_channel();
        let fetch_gate = Arc::new(Notify::new());
        let mut ctx = build_ctx();
        ctx.is_cdc = true;
        let handle = DataCheckerHandle::spawn(
            BlockingFetchChecker {
                fetch_started: fetch_started_tx,
                fetch_gate: fetch_gate.clone(),
            },
            "task-1".to_string(),
            ctx,
            4,
            "unit-test",
        );
        let mut handle = handle;

        handle.enqueue_check(vec![build_row(1)]).await.unwrap();
        timeout(Duration::from_secs(1), fetch_started_rx.recv())
            .await
            .unwrap()
            .unwrap();

        let checkpoint = Position::Kafka {
            topic: "test".to_string(),
            partition: 0,
            offset: 1,
        };
        timeout(
            Duration::from_millis(50),
            handle.record_checkpoint(&checkpoint),
        )
        .await
        .unwrap()
        .unwrap();

        fetch_gate.notify_waiters();
        timeout(Duration::from_secs(1), handle.close_with_position(None))
            .await
            .unwrap()
            .unwrap();
    }

    #[test]
    fn data_checker_initializes_summary_start_time() {
        let (fetch_started, _fetch_started_rx) = mpsc::unbounded_channel();
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let checker = DataChecker::new(
            BlockingFetchChecker {
                fetch_started,
                fetch_gate: Arc::new(Notify::new()),
            },
            "task-1".to_string(),
            build_ctx(),
            CheckerIo {
                batch_queue: Arc::new(StdMutex::new(LimitedQueue::new(1))),
                batch_notify: Arc::new(Notify::new()),
                dropped_items: Arc::new(AtomicU64::new(0)),
                control_rx,
            },
            "unit-test",
        );

        assert!(!checker.ctx.summary.start_time.is_empty());
    }

    #[test]
    fn summary_with_skips_or_init_failure_is_not_consistent() {
        let summary = CheckSummaryLog {
            skip_count: 1,
            ..Default::default()
        };

        assert!(!super::is_summary_consistent(&summary, false));
        assert!(!super::is_summary_consistent(
            &CheckSummaryLog::default(),
            true
        ));
    }
}
