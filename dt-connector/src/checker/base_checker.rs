use anyhow::Context;
use async_mutex::Mutex;
use async_trait::async_trait;
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
    checker::check_log::{CheckLog, CheckSummaryLog, DiffColValue},
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

    fn build_miss_sql(&self, src_row_data: &Arc<RowData>) -> anyhow::Result<Option<String>> {
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

        self.build_insert_query(&insert_row)
    }

    fn build_diff_sql(
        &self,
        src_row_data: &Arc<RowData>,
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

        self.build_update_query(&update_row, match_full_row)
    }

    fn build_insert_query(&self, row_data: &RowData) -> anyhow::Result<Option<String>> {
        match self {
            CheckerTbMeta::Mysql(meta) => RdbQueryBuilder::new_for_mysql(meta, None)
                .get_query_sql(row_data, false)
                .map(Some),
            CheckerTbMeta::Pg(meta) => RdbQueryBuilder::new_for_pg(meta, None)
                .get_query_sql(row_data, false)
                .map(Some),
            CheckerTbMeta::Mongo(_) => unreachable!("Mongo should be handled"),
        }
    }

    fn build_update_query(
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
            CheckerTbMeta::Mongo(_) => unreachable!("Mongo should be handled in build_miss_sql"),
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

struct RetryItem {
    row: Arc<RowData>,
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
}

pub struct FetchResult {
    pub tb_meta: Arc<CheckerTbMeta>,
    pub src_rows: Vec<Arc<RowData>>,
    pub dst_rows: Vec<RowData>,
}

enum CheckerMsg {
    ProcessBatch(Vec<Arc<RowData>>),
    Close,
}

pub struct DataCheckerHandle {
    tx: mpsc::Sender<CheckerMsg>,
    join_handle: Option<JoinHandle<()>>,
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
        let join_handle = tokio::spawn(check_job.run());

        Self {
            tx,
            join_handle: Some(join_handle),
            pending_rows,
            monitor,
        }
    }

    pub async fn check(&self, data: Vec<Arc<RowData>>) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.send(data).await
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        let _ = self.tx.send(CheckerMsg::Close).await;
        if let Some(handle) = self.join_handle.take() {
            handle.await?;
        }
        Ok(())
    }

    async fn send(&self, data: Vec<Arc<RowData>>) -> anyhow::Result<()> {
        let data_size = data.len() as u64;
        if let Err(err) = self.tx.send(CheckerMsg::ProcessBatch(data)).await {
            return Err(anyhow::anyhow!("Checker worker closed: {}", err));
        }
        if data_size > 0 {
            let pending = self.pending_rows.fetch_add(data_size, Ordering::Relaxed) + data_size;
            self.monitor
                .set_counter(CounterType::CheckerPending, pending);
        }
        Ok(())
    }
}

#[async_trait]
pub trait Checker: Send + Sync + 'static {
    async fn fetch(&mut self, src_rows: &[Arc<RowData>]) -> anyhow::Result<FetchResult>;
    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

struct DataChecker<C: Checker> {
    checker: C,
    ctx: CheckContext,
    retry_queue: VecDeque<RetryItem>,
    retry_next_at: Option<Instant>,
    rx: mpsc::Receiver<CheckerMsg>,
    pending_rows: Arc<AtomicU64>,
    monitor: Arc<Monitor>,
    name: String,
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
            rx,
            pending_rows,
            monitor,
            name: name.to_string(),
        }
    }

    pub async fn run(mut self) {
        log_info!("Checker [{}] background worker started.", self.name);
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                msg = self.rx.recv() => {
                    match msg {
                        Some(CheckerMsg::ProcessBatch(batch)) => {
                            let batch_size = batch.len() as u64;
                            if let Err(err) = self.check_batch(batch, true).await {
                                log_error!("Checker [{}] batch failed: {}", self.name, err);
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
                            }
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    if let Err(err) = self.process_due_retries().await {
                        log_error!("Checker [{}] retry failed: {}", self.name, err);
                    }
                }
            }
        }

        self.monitor.set_counter(CounterType::CheckerPending, 0);
        log_info!("Checker [{}] background worker stopped.", self.name);
    }

    fn build_revise_sql(
        output_revise_sql: bool,
        revise_match_full_row: bool,
        tb_meta: &CheckerTbMeta,
        src_row_data: &Arc<RowData>,
        dst_row_data: Option<&RowData>,
        diff_col_values: Option<&HashMap<String, DiffColValue>>,
    ) -> anyhow::Result<Option<String>> {
        if !output_revise_sql {
            return Ok(None);
        };

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

    fn log_revise_sql(sql: Option<String>, sql_count: &mut usize) {
        if let Some(sql) = sql {
            log_sql!("{}", sql);
            *sql_count += 1;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_inconsistency(
        check_result: CheckInconsistency,
        src_row_data: &Arc<RowData>,
        dst_row_data: Option<&RowData>,
        ctx: &mut CheckContext,
        tb_meta: &CheckerTbMeta,
        miss: &mut Vec<CheckLog>,
        diff: &mut Vec<CheckLog>,
        sql_count: &mut usize,
    ) -> anyhow::Result<()> {
        match check_result {
            CheckInconsistency::Miss => {
                let log = Self::build_miss_log(src_row_data, ctx, tb_meta).await?;
                miss.push(log);
                let revise_sql = Self::build_revise_sql(
                    ctx.output_revise_sql,
                    ctx.revise_match_full_row,
                    tb_meta,
                    src_row_data,
                    None,
                    None,
                )?;
                Self::log_revise_sql(revise_sql, sql_count);
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
                diff.push(log);
                Self::log_revise_sql(revise_sql, sql_count);
            }
        }

        Ok(())
    }

    async fn check_and_generate_logs(
        src_data: &[Arc<RowData>],
        mut dst_row_data_map: HashMap<u128, RowData>,
        ctx: &mut CheckContext,
        tb_meta: &CheckerTbMeta,
        max_retries: u32,
    ) -> anyhow::Result<(Vec<CheckLog>, Vec<CheckLog>, usize, Vec<Arc<RowData>>)> {
        let mut miss = Vec::new();
        let mut diff = Vec::new();
        let mut sql_count = 0;
        let mut retry_rows = Vec::new();

        if max_retries > 0 {
            for src_row_data in src_data {
                let key = src_row_data.get_hash_code(tb_meta.basic())?;
                let dst_row_data = dst_row_data_map.remove(&key);
                if Self::is_inconsistent(src_row_data, dst_row_data.as_ref())? {
                    retry_rows.push(src_row_data.clone());
                }
            }
            return Ok((miss, diff, sql_count, retry_rows));
        }

        for src_row_data in src_data {
            let key = src_row_data.get_hash_code(tb_meta.basic())?;
            let dst_row_data = dst_row_data_map.remove(&key);
            let check_result = Self::compare_src_dst(src_row_data, dst_row_data.as_ref())?;
            let Some(check_result) = check_result else {
                continue;
            };

            Self::handle_inconsistency(
                check_result,
                src_row_data,
                dst_row_data.as_ref(),
                ctx,
                tb_meta,
                &mut miss,
                &mut diff,
                &mut sql_count,
            )
            .await?;
        }

        Ok((miss, diff, sql_count, retry_rows))
    }

    fn compare_src_dst(
        src_row: &Arc<RowData>,
        dst_row: Option<&RowData>,
    ) -> anyhow::Result<Option<CheckInconsistency>> {
        if let Some(dst_row) = dst_row {
            let diffs = Self::compare_row_data(src_row, dst_row)?;
            if diffs.is_empty() {
                Ok(None)
            } else {
                Ok(Some(CheckInconsistency::Diff(diffs)))
            }
        } else {
            Ok(Some(CheckInconsistency::Miss))
        }
    }

    fn compare_row_data(
        src_row_data: &Arc<RowData>,
        dst_row_data: &RowData,
    ) -> anyhow::Result<HashMap<String, DiffColValue>> {
        let src = src_row_data
            .after
            .as_ref()
            .context("src row data after is missing")?;
        let dst = dst_row_data
            .after
            .as_ref()
            .context("dst row data after is missing")?;

        let mut diff_col_values: Option<HashMap<String, DiffColValue>> = None;
        for (col, src_val) in src {
            let dst_val = dst.get(col);
            let maybe_diff = match dst_val {
                Some(dst_val) if src_val.is_same_value(dst_val) => None,
                Some(dst_val) => {
                    let src_type = src_val.type_name();
                    let dst_type = dst_val.type_name();
                    let type_diff = src_type != dst_type;
                    let src_type = type_diff.then(|| src_type.to_string());
                    let dst_type = type_diff.then(|| dst_type.to_string());

                    Some(DiffColValue {
                        src: src_val.to_option_string(),
                        dst: dst_val.to_option_string(),
                        src_type,
                        dst_type,
                    })
                }
                None => Some(DiffColValue {
                    src: src_val.to_option_string(),
                    dst: None,
                    src_type: Some(src_val.type_name().to_string()),
                    dst_type: None,
                }),
            };

            if let Some(diff_entry) = maybe_diff {
                diff_col_values
                    .get_or_insert_with(HashMap::new)
                    .insert(col.to_owned(), diff_entry);
            }
        }

        let mut diff_col_values = diff_col_values.unwrap_or_default();
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

    fn select_dst_row(
        src_row: &Arc<RowData>,
        tb_meta: &CheckerTbMeta,
        dst_rows: Vec<RowData>,
    ) -> anyhow::Result<Option<RowData>> {
        if dst_rows.is_empty() {
            return Ok(None);
        }

        let src_key = src_row.get_hash_code(tb_meta.basic())?;
        for row in dst_rows {
            if row.get_hash_code(tb_meta.basic())? == src_key {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    async fn log_single_inconsistency(
        &mut self,
        check_result: CheckInconsistency,
        src_row_data: &Arc<RowData>,
        dst_row_data: Option<&RowData>,
        tb_meta: &CheckerTbMeta,
    ) -> anyhow::Result<()> {
        let mut miss = Vec::new();
        let mut diff = Vec::new();
        let mut sql_count = 0;
        Self::handle_inconsistency(
            check_result,
            src_row_data,
            dst_row_data,
            &mut self.ctx,
            tb_meta,
            &mut miss,
            &mut diff,
            &mut sql_count,
        )
        .await?;

        Self::log_dml(&miss, &diff);
        let summary = &mut self.ctx.summary;
        summary.end_time = chrono::Local::now().to_rfc3339();
        summary.miss_count += miss.len();
        summary.diff_count += diff.len();
        if sql_count > 0 {
            summary.sql_count = Some(summary.sql_count.unwrap_or(0) + sql_count);
        }
        let monitor = self.ctx.monitor.clone();
        monitor
            .add_counter(CounterType::CheckerMissCount, miss.len() as u64)
            .await
            .add_counter(CounterType::CheckerDiffCount, diff.len() as u64)
            .await
            .add_counter(CounterType::CheckerGenerateSqlCount, sql_count as u64)
            .await;
        Ok(())
    }

    fn log_dml(miss: &[CheckLog], diff: &[CheckLog]) {
        for log in miss {
            log_miss!("{}", log);
        }
        for log in diff {
            log_diff!("{}", log);
        }
    }

    fn map_diff_col_values(
        reverse_router: &RdbRouter,
        src_row_data: &Arc<RowData>,
        diff_col_values: HashMap<String, DiffColValue>,
    ) -> HashMap<String, DiffColValue> {
        let Some(col_map) = reverse_router.get_col_map(&src_row_data.schema, &src_row_data.tb)
        else {
            return diff_col_values;
        };

        let mut mapped = HashMap::with_capacity(diff_col_values.len());
        for (col, val) in diff_col_values {
            if let Some(mapped_col) = col_map.get(&col) {
                if mapped_col != &col {
                    mapped.insert(mapped_col.clone(), val);
                    continue;
                }
            }
            mapped.insert(col, val);
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

        let has_col_map = reverse_router
            .get_col_map(&dst_row_data.schema, &dst_row_data.tb)
            .is_some();
        if has_col_map {
            let reverse_dst_row_data = reverse_router.route_row(dst_row_data.clone());
            Self::clone_row_values(&reverse_dst_row_data)
        } else {
            Self::clone_row_values(dst_row_data)
        }
    }

    async fn build_miss_log(
        src_row_data: &Arc<RowData>,
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
            Cow::Owned(ctx.reverse_router.route_row(src_row_data.as_ref().clone()))
        } else {
            Cow::Borrowed(src_row_data.as_ref())
        };
        let (schema, tb) = if has_col_map {
            (routed_row.schema.clone(), routed_row.tb.clone())
        } else {
            (mapped_schema.to_string(), mapped_tb.to_string())
        };

        let id_col_values = if let Some(meta_manager) = ctx.extractor_meta_manager.as_mut() {
            let src_tb_meta = meta_manager.get_tb_meta(&schema, &tb).await?;
            Self::build_id_col_values(routed_row.as_ref(), src_tb_meta)
                .context("Failed to build ID col values")?
        } else {
            Self::build_id_col_values(routed_row.as_ref(), tb_meta.basic()).unwrap_or_default()
        };

        let src_row = if ctx.output_full_row {
            Self::clone_row_values(routed_row.as_ref())
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
        src_row_data: &Arc<RowData>,
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
        let mut id_col_values = HashMap::with_capacity(tb_meta.id_cols.len());
        let after = row_data.require_after().ok()?;

        for col in tb_meta.id_cols.iter() {
            let val = after.get(col)?.to_option_string();
            id_col_values.insert(col.to_owned(), val);
        }
        Some(id_col_values)
    }

    fn expand_mongo_doc_diff(
        src_row_data: &Arc<RowData>,
        dst_row_data: &RowData,
        mut diff_col_values: HashMap<String, DiffColValue>,
    ) -> HashMap<String, DiffColValue> {
        // avoid output full mongo document to diff
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
            .cloned()
            .chain(dst_doc.into_iter().flat_map(Document::keys).cloned())
            .collect();

        for key in keys {
            let src_value = src_doc.as_ref().and_then(|d| d.get(&key));
            let dst_value = dst_doc.as_ref().and_then(|d| d.get(&key));
            let src_type_name = src_value.map(mongo_cmd::bson_type_name).unwrap_or("None");
            let dst_type_name = dst_value.map(mongo_cmd::bson_type_name).unwrap_or("None");
            let type_diff = src_type_name != dst_type_name;
            let value_diff = src_value != dst_value;

            if value_diff || type_diff {
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
        }

        diff_col_values
    }

    fn enqueue_retry_rows(&mut self, rows: Vec<Arc<RowData>>) {
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
            log_warn!("Checker retry queue full, dropped {} oldest rows.", dropped);
            self.retry_next_at = None;
        }
    }

    async fn process_due_retries(&mut self) -> anyhow::Result<()> {
        if self.retry_queue.is_empty() {
            return Ok(());
        }

        let now = Instant::now();
        if self
            .retry_next_at
            .is_some_and(|next_retry_at| next_retry_at > now)
        {
            return Ok(());
        }

        let mut next_retry_at: Option<Instant> = None;
        let pending_len = self.retry_queue.len();
        for _ in 0..pending_len {
            let item = match self.retry_queue.pop_front() {
                Some(item) => item,
                None => break,
            };

            if item.next_retry_at > now {
                next_retry_at = match next_retry_at {
                    Some(current) => Some(current.min(item.next_retry_at)),
                    None => Some(item.next_retry_at),
                };
                self.retry_queue.push_back(item);
                continue;
            }

            if let Some(rescheduled) = self.retry_check_item(item).await? {
                next_retry_at = match next_retry_at {
                    Some(current) => Some(current.min(rescheduled.next_retry_at)),
                    None => Some(rescheduled.next_retry_at),
                };
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
            let inconsistent = Self::is_inconsistent(&item.row, dst_row.as_ref())?;
            if !inconsistent {
                return Ok(None);
            }
            item.retries_left -= 1;
            item.next_retry_at = Instant::now() + Duration::from_secs(self.ctx.retry_interval_secs);
            return Ok(Some(item));
        }

        let check_result = Self::compare_src_dst(&item.row, dst_row.as_ref())?;
        let Some(check_result) = check_result else {
            return Ok(None);
        };
        self.log_single_inconsistency(check_result, &item.row, dst_row.as_ref(), tb_meta.as_ref())
            .await?;
        Ok(None)
    }

    async fn flush_retries(&mut self) -> anyhow::Result<()> {
        let mut pending = std::mem::take(&mut self.retry_queue);
        while let Some(mut item) = pending.pop_front() {
            item.retries_left = 0;
            let _ = self.retry_check_item(item).await?;
        }
        self.retry_next_at = None;
        Ok(())
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

    pub async fn check_batch(
        &mut self,
        data: Vec<Arc<RowData>>,
        batch: bool,
    ) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        if !batch {
            return Self::serial_check(self, data).await;
        }

        let batch_size = self.ctx.batch_size;
        if batch_size == 0 {
            return Ok(());
        }

        for chunk in data.chunks(batch_size) {
            Self::batch_check(self, chunk).await?;
        }
        Ok(())
    }

    async fn process_batch(
        &mut self,
        data: &[Arc<RowData>],
        is_serial_mode: bool,
    ) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let fetch_result = self.checker.fetch(data).await?;
        let tb_meta = fetch_result.tb_meta;
        let checkable = fetch_result.src_rows;
        if checkable.is_empty() {
            return Ok(());
        }

        let start_time = tokio::time::Instant::now();

        // 1. batch fetch all dst rows and start metrics
        let dst_rows = fetch_result.dst_rows;
        let mut dst_row_data_map = HashMap::with_capacity(dst_rows.len());
        for row in dst_rows {
            let key = row.get_hash_code(tb_meta.basic())?;
            dst_row_data_map.insert(key, row);
        }
        let mut rts = LimitedQueue::new(1);
        rts.push((start_time.elapsed().as_millis() as u64, 1));
        let max_retries = self.ctx.max_retries;

        // 2. check and generate logs (inconsistencies will be retried later)
        let (miss, diff, sql_count, retry_rows) = Self::check_and_generate_logs(
            &checkable,
            dst_row_data_map,
            &mut self.ctx,
            tb_meta.as_ref(),
            max_retries,
        )
        .await?;
        Self::log_dml(&miss, &diff);
        let summary = &mut self.ctx.summary;
        summary.end_time = chrono::Local::now().to_rfc3339();
        summary.miss_count += miss.len();
        summary.diff_count += diff.len();
        if sql_count > 0 {
            summary.sql_count = Some(summary.sql_count.unwrap_or(0) + sql_count);
        }
        self.enqueue_retry_rows(retry_rows);

        // 3. update monitor metrics
        let monitor = self.ctx.monitor.clone();
        monitor
            .add_counter(CounterType::CheckerMissCount, miss.len() as u64)
            .await
            .add_counter(CounterType::CheckerDiffCount, diff.len() as u64)
            .await
            .add_counter(CounterType::CheckerGenerateSqlCount, sql_count as u64)
            .await;
        if is_serial_mode {
            BaseSinker::update_serial_monitor(&monitor, checkable.len() as u64, 0).await?;
        } else {
            BaseSinker::update_batch_monitor(&monitor, checkable.len() as u64, 0).await?;
        }
        BaseSinker::update_monitor_rt(&monitor, &rts).await
    }

    async fn serial_check(&mut self, data: Vec<Arc<RowData>>) -> anyhow::Result<()> {
        Self::process_batch(self, &data, true).await
    }

    async fn batch_check(&mut self, data: &[Arc<RowData>]) -> anyhow::Result<()> {
        Self::process_batch(self, data, false).await
    }

    async fn finish_summary_and_meta(&mut self) -> anyhow::Result<()> {
        let common = &mut self.ctx;
        let global_summary_opt = common.global_summary.clone();
        let summary = &mut common.summary;
        summary.end_time = chrono::Local::now().to_rfc3339();
        summary.is_consistent = summary.miss_count == 0 && summary.diff_count == 0;
        if let Some(global_summary) = global_summary_opt {
            let mut global_summary = global_summary.lock().await;
            global_summary.merge(summary);
        } else {
            log_summary!("{}", summary);
        }
        if let Some(meta_manager) = common.extractor_meta_manager.as_mut() {
            meta_manager.close().await
        } else {
            Ok(())
        }
    }

    fn is_inconsistent(src_row: &Arc<RowData>, dst_row: Option<&RowData>) -> anyhow::Result<bool> {
        let Some(dst_row) = dst_row else {
            return Ok(true);
        };
        let src = src_row
            .after
            .as_ref()
            .context("src row data after is missing")?;
        let dst = dst_row
            .after
            .as_ref()
            .context("dst row data after is missing")?;

        for (col, src_val) in src {
            match dst.get(col) {
                Some(dst_val) if src_val.is_same_value(dst_val) => {}
                _ => return Ok(true),
            }
        }

        Ok(false)
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        if self.ctx.max_retries > 0 {
            self.drain_retries().await?;
        } else {
            self.flush_retries().await?;
        }
        self.finish_summary_and_meta().await?;
        self.checker.close().await
    }
}

// check if row has null key
pub fn has_null_key(row_data: &Arc<RowData>, id_cols: &[String]) -> bool {
    row_data.require_after().ok().is_some_and(|after| {
        id_cols
            .iter()
            .any(|col| matches!(after.get(col), Some(ColValue::None) | None))
    })
}
