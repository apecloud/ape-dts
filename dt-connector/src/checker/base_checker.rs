use anyhow::Context;
use async_mutex::Mutex;
use async_trait::async_trait;
use indexmap::IndexMap;
use mongodb::bson::Document;
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration, Instant};

use crate::{
    checker::check_log::{
        CdcCheckSummaryLog, CheckLog, CheckSummaryLog, DiffColValue, TableCheckCount,
    },
    checker::check_log_uploader::CheckLogUploader,
    rdb_query_builder::RdbQueryBuilder,
    rdb_router::RdbRouter,
    sinker::base_sinker::BaseSinker,
    sinker::mongo::mongo_cmd,
};
use dt_common::meta::{
    col_value::ColValue, mongo::mongo_constant::MongoConstants, mysql::mysql_tb_meta::MysqlTbMeta,
    pg::pg_tb_meta::PgTbMeta, rdb_meta_manager::RdbMetaManager, rdb_tb_meta::RdbTbMeta,
    row_data::RowData, row_type::RowType,
};
use dt_common::{
    log_diff, log_error, log_info, log_miss, log_sql, log_summary, log_warn,
    monitor::{counter_type::CounterType, monitor::Monitor},
    utils::limit_queue::LimitedQueue,
};

pub const CHECKER_MAX_QUERY_BATCH: usize = 1000;

const CHECKER_MAX_RETRY_QUEUE_SIZE: usize = 100_000;

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

    pub fn mysql(&self) -> anyhow::Result<&MysqlTbMeta> {
        match self {
            CheckerTbMeta::Mysql(m) => Ok(m),
            _ => anyhow::bail!("Expected Mysql metadata"),
        }
    }

    pub fn pg(&self) -> anyhow::Result<&PgTbMeta> {
        match self {
            CheckerTbMeta::Pg(m) => Ok(m),
            _ => anyhow::bail!("Expected Pg metadata"),
        }
    }
}

enum CheckInconsistency {
    Miss,
    Diff(HashMap<String, DiffColValue>),
}

struct CheckEntry {
    log: CheckLog,
    revise_sql: Option<String>,
    is_miss: bool,
}

const CHECKER_MAX_STORE_SIZE: usize = 100_000;

struct RetryItem {
    row: RowData,
    retries_left: u32,
    next_retry_at: Instant,
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
    pub cdc_check_log_disk: bool,
    pub cdc_check_log_s3: bool,
    pub check_log_dir: String,
    pub s3_uploader: Option<Arc<CheckLogUploader>>,
    pub cdc_check_log_interval_secs: u64,
}

impl CheckContext {
    pub fn cdc_output_enabled(&self) -> bool {
        self.cdc_check_log_disk || self.cdc_check_log_s3
    }
}

pub struct FetchResult {
    pub tb_meta: Arc<CheckerTbMeta>,
    pub src_rows: Vec<RowData>,
    pub dst_rows: Vec<RowData>,
}

enum CheckerMsg {
    ProcessBatch(Vec<RowData>),
    Close,
}

pub struct DataCheckerHandle {
    tx: mpsc::Sender<CheckerMsg>,
    join_handle: Option<JoinHandle<anyhow::Result<()>>>,
    pending_rows: Arc<AtomicU64>,
    monitor: Arc<Monitor>,
}

impl DataCheckerHandle {
    pub fn spawn<C: Checker>(
        checker: C,
        ctx: CheckContext,
        buffer_size: usize,
        name: &str,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<CheckerMsg>(buffer_size.max(1));
        let pending_rows = Arc::new(AtomicU64::new(0));
        let monitor = ctx.monitor.clone();
        monitor.set_counter(CounterType::CheckerPending, 0);

        let check_job = DataChecker::new(
            checker,
            ctx,
            rx,
            pending_rows.clone(),
            monitor.clone(),
            name,
        );
        let join_handle = tokio::spawn(async move { check_job.run().await });

        Self {
            tx,
            join_handle: Some(join_handle),
            pending_rows,
            monitor,
        }
    }

    pub async fn check(&self, data: Vec<RowData>) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.send(data).await
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        let _ = self.tx.send(CheckerMsg::Close).await;
        if let Some(handle) = self.join_handle.take() {
            handle.await??;
        }
        Ok(())
    }

    async fn send(&self, data: Vec<RowData>) -> anyhow::Result<()> {
        let data_size = data.len() as u64;
        if let Err(err) = self.tx.send(CheckerMsg::ProcessBatch(data)).await {
            return Err(anyhow::anyhow!("Checker worker closed: {}", err));
        }
        let pending = self.pending_rows.fetch_add(data_size, Ordering::Relaxed) + data_size;
        self.monitor
            .set_counter(CounterType::CheckerPending, pending);
        Ok(())
    }
}

#[async_trait]
pub trait Checker: Send + Sync + 'static {
    async fn fetch(&mut self, src_rows: &[RowData]) -> anyhow::Result<FetchResult>;
    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

struct DataChecker<C: Checker> {
    checker: C,
    ctx: CheckContext,
    retry_queue: VecDeque<RetryItem>,
    retry_next_at: Option<Instant>,
    store: IndexMap<u128, CheckEntry>,
    rx: mpsc::Receiver<CheckerMsg>,
    pending_rows: Arc<AtomicU64>,
    monitor: Arc<Monitor>,
    name: String,
    store_dirty: bool,
    evicted_miss: usize,
    evicted_diff: usize,
}

impl<C: Checker> DataChecker<C> {
    pub fn new(
        checker: C,
        ctx: CheckContext,
        rx: mpsc::Receiver<CheckerMsg>,
        pending_rows: Arc<AtomicU64>,
        monitor: Arc<Monitor>,
        name: &str,
    ) -> Self {
        Self {
            checker,
            ctx,
            retry_queue: VecDeque::new(),
            retry_next_at: None,
            store: IndexMap::new(),
            rx,
            pending_rows,
            monitor,
            name: name.to_string(),
            store_dirty: false,
            evicted_miss: 0,
            evicted_diff: 0,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        log_info!("Checker [{}] started.", self.name);
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut first_error: Option<anyhow::Error> = None;

        let cdc_output_enabled = self.ctx.cdc_output_enabled();
        let output_secs = self.ctx.cdc_check_log_interval_secs.max(1);
        let mut output_interval = tokio::time::interval(Duration::from_secs(output_secs));
        output_interval.tick().await;

        loop {
            tokio::select! {
                msg = self.rx.recv() => {
                    match msg {
                        Some(CheckerMsg::ProcessBatch(batch)) => {
                            let batch_size = batch.len() as u64;
                            if let Err(err) = self.check_batch(batch, true).await {
                                log_error!("Checker [{}] batch failed: {}", self.name, err);
                                if first_error.is_none() {
                                    first_error = Some(err);
                                }
                            }
                            if batch_size > 0 {
                                self.pending_rows.fetch_sub(batch_size, Ordering::Relaxed);
                                let pending = self.pending_rows.load(Ordering::Relaxed);
                                self.monitor.set_counter(CounterType::CheckerPending, pending);
                            }
                        }
                        Some(CheckerMsg::Close) | None => {
                            if let Err(err) = self.shutdown().await {
                                log_error!("Checker [{}] close failed: {}", self.name, err);
                                if first_error.is_none() {
                                    first_error = Some(err);
                                }
                            }
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    if let Err(err) = self.process_due_retries().await {
                        log_error!("Checker [{}] retry failed: {}", self.name, err);
                        if first_error.is_none() {
                            first_error = Some(err);
                        }
                    }
                }
                _ = output_interval.tick(), if cdc_output_enabled => {
                    if self.store_dirty {
                        match self.snapshot_and_output().await {
                            Ok(()) => {
                                self.store_dirty = false;
                            }
                            Err(err) => {
                                log_error!("Checker [{}] cdc output failed: {}", self.name, err);
                            }
                        }
                    }
                }
            }
        }

        self.monitor.set_counter(CounterType::CheckerPending, 0);
        log_info!("Checker [{}] stopped.", self.name);

        if let Some(err) = first_error {
            return Err(err);
        }
        Ok(())
    }

    fn build_revise_sql(
        output_revise_sql: bool,
        revise_match_full_row: bool,
        tb_meta: &CheckerTbMeta,
        src_row_data: &RowData,
        dst_row_data: Option<&RowData>,
        diff_col_values: Option<&HashMap<String, DiffColValue>>,
    ) -> anyhow::Result<Option<String>> {
        if !output_revise_sql {
            return Ok(None);
        };

        if src_row_data.row_type == RowType::Delete {
            let dst_row = dst_row_data.context("missing dst row for delete revise")?;
            return tb_meta.build_delete_sql(dst_row);
        }

        match diff_col_values {
            None => tb_meta.build_miss_sql(src_row_data),
            Some(diff_col_values) => {
                let dst_row = dst_row_data.context("missing dst row in diff")?;
                tb_meta.build_diff_sql(
                    src_row_data,
                    dst_row,
                    diff_col_values,
                    revise_match_full_row,
                )
            }
        }
    }

    async fn build_check_entry(
        check_result: CheckInconsistency,
        src_row_data: &RowData,
        dst_row_data: Option<&RowData>,
        ctx: &mut CheckContext,
        tb_meta: &CheckerTbMeta,
    ) -> anyhow::Result<CheckEntry> {
        match check_result {
            CheckInconsistency::Miss => {
                let log = Self::build_miss_log(src_row_data, ctx, tb_meta).await?;
                let revise_sql = Self::build_revise_sql(
                    ctx.output_revise_sql,
                    ctx.revise_match_full_row,
                    tb_meta,
                    src_row_data,
                    None,
                    None,
                )?;
                Ok(CheckEntry {
                    log,
                    revise_sql,
                    is_miss: true,
                })
            }
            CheckInconsistency::Diff(diff_col_values) => {
                let dst_row = dst_row_data.context("missing dst row in diff")?;
                let revise_sql = Self::build_revise_sql(
                    ctx.output_revise_sql,
                    ctx.revise_match_full_row,
                    tb_meta,
                    src_row_data,
                    Some(dst_row),
                    Some(&diff_col_values),
                )?;
                let log =
                    Self::build_diff_log(src_row_data, dst_row, diff_col_values, ctx, tb_meta)
                        .await?;
                Ok(CheckEntry {
                    log,
                    revise_sql,
                    is_miss: false,
                })
            }
        }
    }

    async fn check_rows(
        &mut self,
        src_data: &[RowData],
        mut dst_row_data_map: HashMap<u128, RowData>,
        tb_meta: &CheckerTbMeta,
    ) -> anyhow::Result<(usize, Vec<RowData>)> {
        let mut skip_count = 0;
        let mut retry_rows = Vec::new();

        for src_row_data in src_data {
            let key = match Self::lookup_hash_code(src_row_data, tb_meta.basic()) {
                Ok(k) => k,
                Err(e) => {
                    log_warn!(
                        "Skipping unhashable row in {}.{}: {}",
                        src_row_data.schema,
                        src_row_data.tb,
                        e
                    );
                    skip_count += 1;
                    continue;
                }
            };

            self.cleanup_stale_update_key(src_row_data, tb_meta.basic(), key);
            let dst_row_data = dst_row_data_map.remove(&key);

            if !self.ctx.is_cdc {
                if self.ctx.max_retries > 0 {
                    if Self::is_inconsistent(src_row_data, dst_row_data.as_ref())? {
                        retry_rows.push(src_row_data.clone());
                    }
                } else {
                    self.reconcile_row_inconsistency(
                        key,
                        src_row_data,
                        dst_row_data.as_ref(),
                        tb_meta,
                    )
                    .await?;
                }
            } else {
                self.reconcile_row_inconsistency(key, src_row_data, dst_row_data.as_ref(), tb_meta)
                    .await?;
            }
        }

        Ok((skip_count, retry_rows))
    }

    fn compare_src_dst(
        src_row: &RowData,
        dst_row: Option<&RowData>,
    ) -> anyhow::Result<Option<CheckInconsistency>> {
        if src_row.row_type == RowType::Delete {
            return Ok(dst_row
                .is_some()
                .then(|| CheckInconsistency::Diff(HashMap::new())));
        }
        match dst_row {
            Some(dst_row) => {
                let diffs = Self::compare_row_data(src_row, dst_row)?;
                Ok((!diffs.is_empty()).then_some(CheckInconsistency::Diff(diffs)))
            }
            None => Ok(Some(CheckInconsistency::Miss)),
        }
    }

    fn compare_row_data(
        src_row_data: &RowData,
        dst_row_data: &RowData,
    ) -> anyhow::Result<HashMap<String, DiffColValue>> {
        let src = src_row_data
            .after
            .as_ref()
            .context("src after is missing")?;
        let dst = dst_row_data
            .after
            .as_ref()
            .context("dst after is missing")?;

        let mut diff_col_values = HashMap::new();
        for (col, src_val) in src {
            let maybe_diff = match dst.get(col) {
                Some(dst_val) if src_val.is_same_value(dst_val) => None,
                Some(dst_val) => {
                    let src_type = src_val.type_name();
                    let dst_type = dst_val.type_name();
                    let type_diff = src_type != dst_type;
                    Some(DiffColValue {
                        src: src_val.to_option_string(),
                        dst: dst_val.to_option_string(),
                        src_type: type_diff.then(|| src_type.to_string()),
                        dst_type: type_diff.then(|| dst_type.to_string()),
                    })
                }
                None => Some(DiffColValue {
                    src: src_val.to_option_string(),
                    dst: None,
                    src_type: Some(src_val.type_name().to_string()),
                    dst_type: None,
                }),
            };

            if let Some(entry) = maybe_diff {
                diff_col_values.insert(col.to_owned(), entry);
            }
        }

        if diff_col_values.contains_key(MongoConstants::DOC)
            && [src_row_data, dst_row_data].iter().any(|row| {
                matches!(
                    row.after.as_ref().and_then(|m| m.get(MongoConstants::DOC)),
                    Some(ColValue::MongoDoc(_))
                )
            })
        {
            diff_col_values =
                Self::expand_mongo_doc_diff(src_row_data, dst_row_data, diff_col_values);
        }

        Ok(diff_col_values)
    }

    fn lookup_hash_code(row_data: &RowData, tb_meta: &RdbTbMeta) -> anyhow::Result<u128> {
        let id_values = match row_data.row_type {
            RowType::Delete => row_data.require_before()?,
            _ => row_data.require_after()?,
        };
        Self::hash_from_id_values(id_values, tb_meta)
    }

    fn hash_from_id_values(
        id_values: &HashMap<String, ColValue>,
        tb_meta: &RdbTbMeta,
    ) -> anyhow::Result<u128> {
        let mut hash_code = 1u128;
        for col in &tb_meta.id_cols {
            let col_hash_code = id_values
                .get(col)
                .with_context(|| format!("missing id col value: {}", col))?
                .hash_code()
                .with_context(|| {
                    format!(
                        "unhashable _id value in schema: {}, tb: {}, col: {}",
                        tb_meta.schema, tb_meta.tb, col
                    )
                })?;
            if col_hash_code == 0 {
                return Ok(0);
            }
            hash_code = 31 * hash_code + col_hash_code as u128;
        }
        Ok(hash_code)
    }

    fn select_dst_row(
        src_row: &RowData,
        tb_meta: &CheckerTbMeta,
        dst_rows: Vec<RowData>,
    ) -> anyhow::Result<Option<RowData>> {
        let src_key = Self::lookup_hash_code(src_row, tb_meta.basic())?;
        for row in dst_rows {
            if Self::lookup_hash_code(&row, tb_meta.basic())? == src_key {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn log_entry(&self, entry: &CheckEntry) {
        if entry.is_miss {
            log_miss!("{}", entry.log);
        } else {
            log_diff!("{}", entry.log);
        }
        if let Some(sql) = &entry.revise_sql {
            log_sql!("{}", sql);
        }
    }

    fn update_summary_for_entry(&mut self, entry: &CheckEntry) {
        let summary = &mut self.ctx.summary;
        summary.end_time = chrono::Local::now().to_rfc3339();
        if entry.is_miss {
            summary.miss_count += 1;
        } else {
            summary.diff_count += 1;
        }
        if entry.revise_sql.is_some() {
            summary.sql_count = Some(summary.sql_count.unwrap_or(0) + 1);
        }
    }

    fn remove_store_entry(&mut self, key: u128) {
        if self.store.swap_remove(&key).is_some() {
            self.store_dirty = true;
        }
    }

    fn cleanup_stale_update_key(&mut self, row_data: &RowData, tb_meta: &RdbTbMeta, key: u128) {
        if row_data.row_type != RowType::Update {
            return;
        }

        // UPDATE can change primary/unique keys. If so, remove stale inconsistency
        // tracked by the old (before-image) key to avoid lingering false positives.
        match row_data.get_hash_code(tb_meta) {
            Ok(old_key) if old_key != key => self.remove_store_entry(old_key),
            Ok(_) => {}
            Err(err) => {
                log_warn!(
                    "Failed to compute pre-update key for {}.{}: {}",
                    row_data.schema,
                    row_data.tb,
                    err
                );
            }
        }
    }

    async fn reconcile_row_inconsistency(
        &mut self,
        key: u128,
        src_row_data: &RowData,
        dst_row_data: Option<&RowData>,
        tb_meta: &CheckerTbMeta,
    ) -> anyhow::Result<()> {
        if let Some(check_result) = Self::compare_src_dst(src_row_data, dst_row_data)? {
            let entry = Self::build_check_entry(
                check_result,
                src_row_data,
                dst_row_data,
                &mut self.ctx,
                tb_meta,
            )
            .await?;
            self.store_entry(key, entry);
        } else {
            self.remove_store_entry(key);
        }
        Ok(())
    }

    fn store_entry(&mut self, key: u128, entry: CheckEntry) {
        if !self.ctx.is_cdc {
            self.log_entry(&entry);
            self.update_summary_for_entry(&entry);
            return;
        }

        self.store_dirty = true;
        if self.store.len() >= CHECKER_MAX_STORE_SIZE && !self.store.contains_key(&key) {
            log_warn!(
                "Inconsistency store full (max {}), evicting oldest entry.",
                CHECKER_MAX_STORE_SIZE
            );
            if let Some((_, evicted)) = self.store.swap_remove_index(0) {
                if self.ctx.cdc_output_enabled() {
                    log_warn!(
                        "Evicted entry: schema={}, tb={}, id={:?}",
                        evicted.log.schema,
                        evicted.log.tb,
                        evicted.log.id_col_values
                    );
                    if evicted.is_miss {
                        self.evicted_miss += 1;
                    } else {
                        self.evicted_diff += 1;
                    }
                } else {
                    self.log_entry(&evicted);
                    self.update_summary_for_entry(&evicted);
                }
            }
        }
        self.store.insert(key, entry);
    }

    fn flush_store(&mut self) {
        let entries = std::mem::take(&mut self.store);
        for (_key, entry) in entries {
            self.log_entry(&entry);
            self.update_summary_for_entry(&entry);
        }
    }

    fn map_diff_col_values(
        reverse_router: &RdbRouter,
        src_row_data: &RowData,
        diff_col_values: HashMap<String, DiffColValue>,
    ) -> HashMap<String, DiffColValue> {
        let Some(col_map) = reverse_router.get_col_map(&src_row_data.schema, &src_row_data.tb)
        else {
            return diff_col_values;
        };

        let mut mapped = HashMap::with_capacity(diff_col_values.len());
        for (col, val) in diff_col_values {
            let key = col_map
                .get(&col)
                .filter(|c| *c != &col)
                .cloned()
                .unwrap_or(col);
            mapped.insert(key, val);
        }
        mapped
    }

    fn maybe_build_dst_row(
        reverse_router: &RdbRouter,
        dst_row_data: &RowData,
        output_full_row: bool,
    ) -> Option<HashMap<String, ColValue>> {
        if !output_full_row {
            return None;
        }
        if reverse_router
            .get_col_map(&dst_row_data.schema, &dst_row_data.tb)
            .is_some()
        {
            let routed = reverse_router.route_row(dst_row_data.clone());
            Self::clone_row_values(&routed)
        } else {
            Self::clone_row_values(dst_row_data)
        }
    }

    async fn build_miss_log(
        src_row_data: &RowData,
        ctx: &mut CheckContext,
        tb_meta: &CheckerTbMeta,
    ) -> anyhow::Result<CheckLog> {
        let (mapped_schema, mapped_tb) = ctx
            .reverse_router
            .get_tb_map(&src_row_data.schema, &src_row_data.tb);
        let has_col_map = ctx
            .reverse_router
            .get_col_map(&src_row_data.schema, &src_row_data.tb)
            .is_some();
        let schema_changed = src_row_data.schema != mapped_schema || src_row_data.tb != mapped_tb;

        let routed_row = if has_col_map {
            Cow::Owned(ctx.reverse_router.route_row(src_row_data.clone()))
        } else {
            Cow::Borrowed(src_row_data)
        };
        let (schema, tb) = if has_col_map {
            (routed_row.schema.clone(), routed_row.tb.clone())
        } else {
            (mapped_schema.to_string(), mapped_tb.to_string())
        };

        let id_col_values = if let Some(meta_manager) = ctx.extractor_meta_manager.as_mut() {
            let src_tb_meta = meta_manager.get_tb_meta(&schema, &tb).await?;
            Self::build_id_col_values(&routed_row, src_tb_meta)
                .context("Failed to build ID col values")?
        } else {
            Self::build_id_col_values(&routed_row, tb_meta.basic()).unwrap_or_default()
        };

        let src_row = if ctx.output_full_row {
            Self::clone_row_values(&routed_row)
        } else {
            None
        };

        Ok(CheckLog {
            schema,
            tb,
            target_schema: schema_changed.then(|| src_row_data.schema.clone()),
            target_tb: schema_changed.then(|| src_row_data.tb.clone()),
            id_col_values,
            diff_col_values: HashMap::new(),
            src_row,
            dst_row: None,
        })
    }

    async fn build_diff_log(
        src_row_data: &RowData,
        dst_row_data: &RowData,
        diff_col_values: HashMap<String, DiffColValue>,
        ctx: &mut CheckContext,
        tb_meta: &CheckerTbMeta,
    ) -> anyhow::Result<CheckLog> {
        let mut log = Self::build_miss_log(src_row_data, ctx, tb_meta).await?;
        log.diff_col_values =
            Self::map_diff_col_values(&ctx.reverse_router, src_row_data, diff_col_values);
        log.dst_row =
            Self::maybe_build_dst_row(&ctx.reverse_router, dst_row_data, ctx.output_full_row);
        Ok(log)
    }

    fn clone_row_values(row_data: &RowData) -> Option<HashMap<String, ColValue>> {
        match row_data.row_type {
            RowType::Insert | RowType::Update => row_data.after.clone(),
            RowType::Delete => row_data.before.clone(),
        }
    }

    fn build_id_col_values(
        row_data: &RowData,
        tb_meta: &RdbTbMeta,
    ) -> Option<HashMap<String, Option<String>>> {
        let col_values = match row_data.row_type {
            RowType::Delete => row_data.require_before().ok()?,
            _ => row_data.require_after().ok()?,
        };
        let mut id_col_values = HashMap::with_capacity(tb_meta.id_cols.len());
        for col in &tb_meta.id_cols {
            id_col_values.insert(col.to_owned(), col_values.get(col)?.to_option_string());
        }
        Some(id_col_values)
    }

    fn expand_mongo_doc_diff(
        src_row_data: &RowData,
        dst_row_data: &RowData,
        mut diff_col_values: HashMap<String, DiffColValue>,
    ) -> HashMap<String, DiffColValue> {
        diff_col_values.remove(MongoConstants::DOC);

        fn get_doc(row: &RowData) -> Option<&Document> {
            row.after
                .as_ref()
                .and_then(|after| after.get(MongoConstants::DOC))
                .and_then(|val| match val {
                    ColValue::MongoDoc(doc) => Some(doc),
                    _ => None,
                })
        }

        let src_doc = get_doc(src_row_data);
        let dst_doc = get_doc(dst_row_data);

        let keys: BTreeSet<_> = src_doc
            .into_iter()
            .flat_map(Document::keys)
            .chain(dst_doc.into_iter().flat_map(Document::keys))
            .cloned()
            .collect();

        for key in keys {
            let src_value = src_doc.and_then(|d| d.get(&key));
            let dst_value = dst_doc.and_then(|d| d.get(&key));
            if src_value == dst_value {
                continue;
            }
            let src_type_name = src_value.map(mongo_cmd::bson_type_name).unwrap_or("None");
            let dst_type_name = dst_value.map(mongo_cmd::bson_type_name).unwrap_or("None");
            let type_diff = src_type_name != dst_type_name;
            diff_col_values.insert(
                key,
                DiffColValue {
                    src: src_value.map(mongo_cmd::bson_to_log_literal),
                    dst: dst_value.map(mongo_cmd::bson_to_log_literal),
                    src_type: type_diff.then(|| src_type_name.to_string()),
                    dst_type: type_diff.then(|| dst_type_name.to_string()),
                },
            );
        }

        diff_col_values
    }

    fn enqueue_retry_rows(&mut self, rows: Vec<RowData>) {
        if rows.is_empty() {
            return;
        }

        let retry_at = Instant::now() + Duration::from_secs(self.ctx.retry_interval_secs);
        if self.retry_next_at.is_none_or(|current| retry_at < current) {
            self.retry_next_at = Some(retry_at);
        }
        let mut dropped = 0usize;
        for row in rows {
            if self.retry_queue.len() >= CHECKER_MAX_RETRY_QUEUE_SIZE {
                self.retry_queue.pop_front();
                dropped += 1;
            }
            self.retry_queue.push_back(RetryItem {
                row,
                retries_left: self.ctx.max_retries,
                next_retry_at: retry_at,
            });
        }
        if dropped > 0 {
            log_warn!(
                "Retry queue full (max {}), dropped {} oldest rows.",
                CHECKER_MAX_RETRY_QUEUE_SIZE,
                dropped
            );
            self.retry_next_at = None;
        }
    }

    async fn process_due_retries(&mut self) -> anyhow::Result<()> {
        if self.retry_queue.is_empty() {
            return Ok(());
        }
        let now = Instant::now();
        if self.retry_next_at.is_some_and(|t| t > now) {
            return Ok(());
        }

        let mut next_retry_at: Option<Instant> = None;
        let pending_len = self.retry_queue.len();
        for _ in 0..pending_len {
            let Some(item) = self.retry_queue.pop_front() else {
                break;
            };

            if item.next_retry_at > now {
                next_retry_at = Some(
                    next_retry_at
                        .map_or(item.next_retry_at, |c: Instant| c.min(item.next_retry_at)),
                );
                self.retry_queue.push_back(item);
                continue;
            }

            if let Some(rescheduled) = self.retry_check_item(item).await? {
                next_retry_at = Some(
                    next_retry_at.map_or(rescheduled.next_retry_at, |c: Instant| {
                        c.min(rescheduled.next_retry_at)
                    }),
                );
                self.retry_queue.push_back(rescheduled);
            }
        }
        self.retry_next_at = next_retry_at;
        Ok(())
    }

    async fn retry_check_item(&mut self, mut item: RetryItem) -> anyhow::Result<Option<RetryItem>> {
        let fetch_result = self.checker.fetch(std::slice::from_ref(&item.row)).await?;
        if fetch_result.src_rows.is_empty() {
            return Ok(None);
        }
        let tb_meta = fetch_result.tb_meta;
        let dst_row = Self::select_dst_row(&item.row, tb_meta.as_ref(), fetch_result.dst_rows)?;

        if item.retries_left > 1 {
            if !Self::is_inconsistent(&item.row, dst_row.as_ref())? {
                return Ok(None);
            }
            item.retries_left -= 1;
            item.next_retry_at = Instant::now() + Duration::from_secs(self.ctx.retry_interval_secs);
            return Ok(Some(item));
        }

        let key = Self::lookup_hash_code(&item.row, tb_meta.basic())?;
        self.cleanup_stale_update_key(&item.row, tb_meta.basic(), key);
        self.reconcile_row_inconsistency(key, &item.row, dst_row.as_ref(), tb_meta.as_ref())
            .await?;
        Ok(None)
    }

    async fn drain_retries(&mut self) -> anyhow::Result<()> {
        while !self.retry_queue.is_empty() {
            let next_retry_at = self
                .retry_queue
                .iter()
                .map(|item| item.next_retry_at)
                .min()
                .expect("retry queue should not be empty");
            let now = Instant::now();
            if next_retry_at > now {
                sleep(next_retry_at.duration_since(now)).await;
            }
            self.process_due_retries().await?;
        }
        self.retry_next_at = None;
        Ok(())
    }

    pub async fn check_batch(&mut self, data: Vec<RowData>, batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        if !batch {
            return self.process_batch(&data, true).await;
        }
        let batch_size = self.ctx.batch_size.max(1);
        for chunk in data.chunks(batch_size) {
            self.process_batch(chunk, false).await?;
        }
        Ok(())
    }

    async fn process_batch(
        &mut self,
        data: &[RowData],
        is_serial_mode: bool,
    ) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let start_time = tokio::time::Instant::now();
        let mut grouped: HashMap<(String, String), Vec<RowData>> = HashMap::new();
        for row in data {
            grouped
                .entry((row.schema.clone(), row.tb.clone()))
                .or_default()
                .push(row.clone());
        }

        let mut total_checked = 0usize;
        let mut total_skip_count = 0usize;
        let mut retry_rows = Vec::new();
        for rows in grouped.into_values() {
            let fetch_result = self.checker.fetch(&rows).await?;
            let tb_meta = fetch_result.tb_meta;
            let checkable = fetch_result.src_rows;
            if checkable.is_empty() {
                continue;
            }
            total_checked += checkable.len();

            let mut dst_row_data_map = HashMap::with_capacity(fetch_result.dst_rows.len());
            for row in fetch_result.dst_rows {
                dst_row_data_map.insert(Self::lookup_hash_code(&row, tb_meta.basic())?, row);
            }

            let (skip_count, table_retry_rows) = self
                .check_rows(&checkable, dst_row_data_map, tb_meta.as_ref())
                .await?;
            total_skip_count += skip_count;
            retry_rows.extend(table_retry_rows);
        }
        if total_checked == 0 {
            return Ok(());
        }

        let mut rts = LimitedQueue::new(1);
        rts.push((start_time.elapsed().as_millis() as u64, 1));
        self.ctx.summary.skip_count += total_skip_count;
        self.enqueue_retry_rows(retry_rows);

        let monitor = self.ctx.monitor.clone();
        if is_serial_mode {
            BaseSinker::update_serial_monitor(&monitor, total_checked as u64, 0).await?;
        } else {
            BaseSinker::update_batch_monitor(&monitor, total_checked as u64, 0).await?;
        }
        BaseSinker::update_monitor_rt(&monitor, &rts).await
    }

    async fn finish_summary_and_meta(&mut self) -> anyhow::Result<()> {
        let common = &mut self.ctx;
        let global_summary_opt = common.global_summary.clone();
        let summary = &mut common.summary;
        summary.end_time = chrono::Local::now().to_rfc3339();
        summary.is_consistent = summary.miss_count == 0 && summary.diff_count == 0;
        if let Some(global_summary) = global_summary_opt {
            global_summary.lock().await.merge(summary);
        } else {
            log_summary!("{}", summary);
        }
        if let Some(meta_manager) = common.extractor_meta_manager.as_mut() {
            meta_manager.close().await
        } else {
            Ok(())
        }
    }

    fn is_inconsistent(src_row: &RowData, dst_row: Option<&RowData>) -> anyhow::Result<bool> {
        if src_row.row_type == RowType::Delete {
            return Ok(dst_row.is_some());
        }
        let Some(dst_row) = dst_row else {
            return Ok(true);
        };
        let src = src_row.after.as_ref().context("src after is missing")?;
        let dst = dst_row.after.as_ref().context("dst after is missing")?;

        for (col, src_val) in src {
            match dst.get(col) {
                Some(dst_val) if src_val.is_same_value(dst_val) => {}
                _ => return Ok(true),
            }
        }
        Ok(false)
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        if !self.ctx.is_cdc {
            self.drain_retries().await?;
        }
        if self.ctx.cdc_output_enabled() {
            if let Err(err) = self.snapshot_and_output().await {
                log_error!("Checker [{}] final cdc output failed: {}", self.name, err);
            }
        }
        self.flush_store();
        self.finish_summary_and_meta().await?;
        self.checker.close().await
    }

    const CDC_LOG_MAX_ENTRIES: usize = 10_000;
    const CDC_LOG_MAX_FILE_SIZE: usize = 100 * 1024 * 1024;

    async fn snapshot_and_output(&self) -> anyhow::Result<()> {
        let mut miss_buf: Vec<u8> = Vec::new();
        let mut diff_buf: Vec<u8> = Vec::new();
        let mut miss_written = 0usize;
        let mut diff_written = 0usize;
        let mut table_counts: HashMap<String, TableCheckCount> = HashMap::new();

        for entry in self.store.values() {
            let tb_key = format!("{}.{}", entry.log.schema, entry.log.tb);
            let counts = table_counts.entry(tb_key).or_default();

            if entry.is_miss {
                counts.miss_count += 1;
                Self::append_entry_to_buf(&mut miss_buf, &mut miss_written, &entry.log);
            } else {
                counts.diff_count += 1;
                Self::append_entry_to_buf(&mut diff_buf, &mut diff_written, &entry.log);
            }
        }

        let total_miss: usize =
            table_counts.values().map(|c| c.miss_count).sum::<usize>() + self.evicted_miss;
        let total_diff: usize =
            table_counts.values().map(|c| c.diff_count).sum::<usize>() + self.evicted_diff;

        let summary = CdcCheckSummaryLog {
            start_time: self.ctx.summary.start_time.clone(),
            end_time: chrono::Local::now().to_rfc3339(),
            is_consistent: total_miss == 0 && total_diff == 0,
            total_miss_count: total_miss,
            total_diff_count: total_diff,
            tables: table_counts,
        };
        let summary_buf = serde_json::to_vec(&summary)?;

        if self.ctx.cdc_check_log_disk {
            Self::write_to_disk(&self.ctx.check_log_dir, &miss_buf, &diff_buf, &summary_buf)?;
        }
        if self.ctx.cdc_check_log_s3 {
            self.upload_to_s3(&miss_buf, &diff_buf, &summary_buf)
                .await?;
        }

        Ok(())
    }

    fn append_entry_to_buf(buf: &mut Vec<u8>, count: &mut usize, log: &CheckLog) {
        if *count >= Self::CDC_LOG_MAX_ENTRIES {
            return;
        }
        let Ok(line) = serde_json::to_string(log) else {
            return;
        };
        if buf.len() + line.len() + 1 > Self::CDC_LOG_MAX_FILE_SIZE {
            return;
        }
        buf.extend_from_slice(line.as_bytes());
        buf.push(b'\n');
        *count += 1;
    }

    fn write_to_disk(
        dir: &str,
        miss_buf: &[u8],
        diff_buf: &[u8],
        summary_buf: &[u8],
    ) -> anyhow::Result<()> {
        let path = std::path::Path::new(dir);
        std::fs::create_dir_all(path)?;
        std::fs::write(path.join("miss.log"), miss_buf)?;
        std::fs::write(path.join("diff.log"), diff_buf)?;
        std::fs::write(path.join("summary.log"), summary_buf)?;
        Ok(())
    }

    async fn upload_to_s3(
        &self,
        miss_buf: &[u8],
        diff_buf: &[u8],
        summary_buf: &[u8],
    ) -> anyhow::Result<()> {
        let Some(uploader) = &self.ctx.s3_uploader else {
            return Ok(());
        };
        let p = &uploader.key_prefix;
        let miss_key = format!("{p}/miss.log");
        let diff_key = format!("{p}/diff.log");
        let summary_key = format!("{p}/summary.log");
        tokio::try_join!(
            uploader.put(&miss_key, miss_buf),
            uploader.put(&diff_key, diff_buf),
            uploader.put(&summary_key, summary_buf),
        )?;
        Ok(())
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
    use dt_common::monitor::monitor::Monitor;
    use std::collections::HashSet;
    use std::sync::{atomic::AtomicU64, Arc, Mutex as StdMutex};
    use tokio::sync::mpsc;

    struct MockChecker {
        calls: Arc<StdMutex<Vec<(String, String, usize)>>>,
    }

    struct MissingDstChecker;

    fn build_test_tb_meta(schema: &str, tb: &str) -> Arc<CheckerTbMeta> {
        Arc::new(CheckerTbMeta::Mongo(RdbTbMeta {
            schema: schema.to_string(),
            tb: tb.to_string(),
            cols: vec!["id".to_string(), "v".to_string()],
            id_cols: vec!["id".to_string()],
            key_map: HashMap::from([("PRIMARY".to_string(), vec!["id".to_string()])]),
            ..Default::default()
        }))
    }

    #[async_trait]
    impl Checker for MockChecker {
        async fn fetch(&mut self, src_rows: &[RowData]) -> anyhow::Result<FetchResult> {
            let first = src_rows
                .first()
                .context("mock checker requires non-empty source rows")?;
            self.calls.lock().unwrap().push((
                first.schema.clone(),
                first.tb.clone(),
                src_rows.len(),
            ));

            let tb_meta = build_test_tb_meta(&first.schema, &first.tb);

            let dst_rows = src_rows
                .iter()
                .map(|row| {
                    let after = row.after.clone().or_else(|| row.before.clone());
                    RowData::new(
                        row.schema.clone(),
                        row.tb.clone(),
                        RowType::Insert,
                        None,
                        after,
                    )
                })
                .collect();

            Ok(FetchResult {
                tb_meta,
                src_rows: src_rows.to_vec(),
                dst_rows,
            })
        }
    }

    #[async_trait]
    impl Checker for MissingDstChecker {
        async fn fetch(&mut self, src_rows: &[RowData]) -> anyhow::Result<FetchResult> {
            let first = src_rows
                .first()
                .context("mock checker requires non-empty source rows")?;
            let tb_meta = build_test_tb_meta(&first.schema, &first.tb);
            Ok(FetchResult {
                tb_meta,
                src_rows: src_rows.to_vec(),
                dst_rows: vec![],
            })
        }
    }

    fn build_context(monitor: Arc<Monitor>, is_cdc: bool, batch_size: usize) -> CheckContext {
        CheckContext {
            monitor,
            summary: CheckSummaryLog {
                start_time: "test".to_string(),
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
            batch_size,
            retry_interval_secs: 0,
            max_retries: 0,
            is_cdc,
            cdc_check_log_disk: false,
            cdc_check_log_s3: false,
            check_log_dir: String::new(),
            s3_uploader: None,
            cdc_check_log_interval_secs: 1,
        }
    }

    fn build_insert_row(schema: &str, tb: &str, id: i32, v: &str) -> RowData {
        let mut after = HashMap::new();
        after.insert("id".to_string(), ColValue::Long(id));
        after.insert("v".to_string(), ColValue::String(v.to_string()));
        RowData::new(
            schema.to_string(),
            tb.to_string(),
            RowType::Insert,
            None,
            Some(after),
        )
    }

    fn build_update_key_changed_row(
        schema: &str,
        tb: &str,
        before_id: i32,
        after_id: i32,
        before_v: &str,
        after_v: &str,
    ) -> RowData {
        let mut before = HashMap::new();
        before.insert("id".to_string(), ColValue::Long(before_id));
        before.insert("v".to_string(), ColValue::String(before_v.to_string()));

        let mut after = HashMap::new();
        after.insert("id".to_string(), ColValue::Long(after_id));
        after.insert("v".to_string(), ColValue::String(after_v.to_string()));
        RowData::new(
            schema.to_string(),
            tb.to_string(),
            RowType::Update,
            Some(before),
            Some(after),
        )
    }

    #[tokio::test]
    async fn process_batch_groups_fetch_by_table() {
        let calls = Arc::new(StdMutex::new(Vec::new()));
        let checker = MockChecker {
            calls: calls.clone(),
        };

        let monitor = Arc::new(Monitor::new("checker", "test", 1, 10, 10));
        let ctx = build_context(monitor.clone(), false, 8);
        let (_tx, rx) = mpsc::channel(8);
        let mut data_checker = DataChecker::new(
            checker,
            ctx,
            rx,
            Arc::new(AtomicU64::new(0)),
            monitor,
            "test",
        );

        let rows = vec![
            build_insert_row("s1", "t1", 1, "a"),
            build_insert_row("s1", "t2", 2, "b"),
        ];
        data_checker.process_batch(&rows, false).await.unwrap();

        let calls = calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 2);
        let tables: HashSet<(String, String)> = calls
            .iter()
            .map(|(schema, tb, _)| (schema.clone(), tb.clone()))
            .collect();
        assert!(tables.contains(&("s1".to_string(), "t1".to_string())));
        assert!(tables.contains(&("s1".to_string(), "t2".to_string())));
    }

    #[tokio::test]
    async fn cdc_update_with_changed_key_should_not_mark_miss() {
        let checker = MockChecker {
            calls: Arc::new(StdMutex::new(Vec::new())),
        };
        let monitor = Arc::new(Monitor::new("checker", "test", 1, 10, 10));
        let ctx = build_context(monitor.clone(), true, 8);
        let (_tx, rx) = mpsc::channel(8);
        let mut data_checker = DataChecker::new(
            checker,
            ctx,
            rx,
            Arc::new(AtomicU64::new(0)),
            monitor,
            "test",
        );

        let row = build_update_key_changed_row("s1", "t1", 1, 2, "before", "after");
        data_checker.process_batch(&[row], false).await.unwrap();

        assert!(data_checker.store.is_empty());
        assert_eq!(data_checker.ctx.summary.miss_count, 0);
        assert_eq!(data_checker.ctx.summary.diff_count, 0);
    }

    #[tokio::test]
    async fn non_cdc_without_retry_should_record_miss_immediately() {
        let checker = MissingDstChecker;
        let monitor = Arc::new(Monitor::new("checker", "test", 1, 10, 10));
        let mut ctx = build_context(monitor.clone(), false, 8);
        ctx.max_retries = 0;
        ctx.retry_interval_secs = 10;
        let (_tx, rx) = mpsc::channel(8);
        let mut data_checker = DataChecker::new(
            checker,
            ctx,
            rx,
            Arc::new(AtomicU64::new(0)),
            monitor,
            "test",
        );

        let row = build_insert_row("s1", "t1", 1, "a");
        data_checker.process_batch(&[row], false).await.unwrap();

        assert_eq!(data_checker.ctx.summary.miss_count, 1);
        assert_eq!(data_checker.ctx.summary.diff_count, 0);
        assert!(data_checker.retry_queue.is_empty());
        assert!(data_checker.store.is_empty());
    }
}
