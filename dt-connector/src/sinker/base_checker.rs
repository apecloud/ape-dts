use anyhow::Context;
use async_mutex::Mutex;
use async_trait::async_trait;
use mongodb::bson::Document;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use std::sync::Arc;
use tokio::time::{sleep, Duration};

use crate::{
    check_log::check_log::{CheckLog, CheckSummaryLog, DiffColValue, StructCheckLog},
    rdb_query_builder::RdbQueryBuilder,
    rdb_router::RdbRouter,
    sinker::base_sinker::BaseSinker,
    sinker::mongo::mongo_cmd,
    Sinker,
};
use dt_common::meta::{
    col_value::ColValue, mongo::mongo_constant::MongoConstants, mysql::mysql_tb_meta::MysqlTbMeta,
    pg::pg_tb_meta::PgTbMeta, rdb_meta_manager::RdbMetaManager, rdb_tb_meta::RdbTbMeta,
    row_data::RowData, row_type::RowType,
    struct_meta::statement::struct_statement::StructStatement,
    struct_meta::struct_data::StructData,
};
use dt_common::{
    log_diff, log_extra, log_miss, log_sql, log_summary, monitor::monitor::Monitor,
    rdb_filter::RdbFilter, utils::limit_queue::LimitedQueue,
};

#[async_trait]
impl<T> Sinker for T
where
    T: Checker,
{
    async fn sink_dml(&mut self, data: Vec<RowData>, batch: bool) -> anyhow::Result<()> {
        BaseChecker::sink_dml(self, data, batch).await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        BaseChecker::close(self).await
    }

    async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        BaseChecker::sink_struct(self, data).await
    }
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

        self.build_insert_query(&insert_row)
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

type CheckResult = Option<CheckInconsistency>;

#[derive(Clone)]
pub struct CheckerCommon {
    pub monitor: Arc<Monitor>,
    pub summary: CheckSummaryLog,
    pub output_revise_sql: bool,
    pub extractor_meta_manager: Option<RdbMetaManager>,
    pub reverse_router: RdbRouter,
    pub output_full_row: bool,
    pub revise_match_full_row: bool,
    pub global_summary: Option<Arc<Mutex<CheckSummaryLog>>>,
    pub filter: RdbFilter,
    pub batch_size: usize,
    pub retry_interval_secs: u64,
    pub max_retries: u32,
}

#[async_trait]
pub trait Checker: Clone + Send + Sync + 'static {
    fn common_mut(&mut self) -> &mut CheckerCommon;
    async fn get_tb_meta_by_row(&mut self, row: &RowData) -> anyhow::Result<CheckerTbMeta>;
    async fn fetch_batch(
        &self,
        tb_meta: &CheckerTbMeta,
        data: &[&RowData],
    ) -> anyhow::Result<Vec<RowData>>;
    async fn fetch_dst_struct(&self, _src: &StructStatement) -> anyhow::Result<StructStatement> {
        Ok(StructStatement::Unknown)
    }
}

pub struct BaseChecker {}

impl BaseChecker {
    // check if row has null key
    pub fn has_null_key(row_data: &RowData, id_cols: &[String]) -> bool {
        row_data.require_after().ok().is_some_and(|after| {
            id_cols
                .iter()
                .any(|col| matches!(after.get(col), Some(ColValue::None) | None))
        })
    }
    async fn resolve_inconsistencies_with_retry<B: Checker>(
        checker: &B,
        src_data: &[RowData],
        src_keys: &[u128],
        mut dst_row_data_map: HashMap<u128, RowData>,
        recheck_settings: (u64, u32),
        tb_meta: &CheckerTbMeta,
    ) -> anyhow::Result<HashMap<u128, RowData>> {
        let (retry_interval_secs, max_retries) = recheck_settings;
        if max_retries == 0 {
            return Ok(dst_row_data_map);
        }

        let mut retry_indices: Vec<usize> = src_data
            .iter()
            .enumerate()
            .filter(|(i, src_row)| {
                Self::compare_src_dst(src_row, dst_row_data_map.get(&src_keys[*i]))
                    .is_ok_and(|r| r.is_some())
            })
            .map(|(i, _)| i)
            .collect();

        for _ in 0..max_retries {
            if retry_indices.is_empty() {
                break;
            }

            Self::maybe_sleep(retry_interval_secs).await;

            // only collect references
            let retry_rows: Vec<&RowData> = retry_indices.iter().map(|&i| &src_data[i]).collect();
            let new_dst_rows = checker.fetch_batch(tb_meta, &retry_rows).await?;

            for row in new_dst_rows {
                let key = row.get_hash_code(tb_meta.basic())?;
                dst_row_data_map.insert(key, row);
            }

            retry_indices.retain(|&i| {
                Self::compare_src_dst(&src_data[i], dst_row_data_map.get(&src_keys[i]))
                    .is_ok_and(|r| r.is_some())
            });
        }

        Ok(dst_row_data_map)
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

    async fn maybe_sleep(retry_interval_secs: u64) {
        if retry_interval_secs > 0 {
            sleep(Duration::from_secs(retry_interval_secs)).await;
        }
    }

    async fn handle_inconsistency(
        src_row_data: &RowData,
        dst_row_data: Option<&RowData>,
        common: &mut CheckerCommon,
        tb_meta: &CheckerTbMeta,
        miss: &mut Vec<CheckLog>,
        diff: &mut Vec<CheckLog>,
        sql_count: &mut usize,
    ) -> anyhow::Result<()> {
        let check_result = Self::compare_src_dst(src_row_data, dst_row_data)?;
        let Some(res) = check_result else {
            return Ok(());
        };

        match res {
            CheckInconsistency::Miss => {
                let log = Self::build_miss_log(src_row_data, common, tb_meta).await?;
                miss.push(log);
                let revise_sql = Self::build_revise_sql(
                    common.output_revise_sql,
                    common.revise_match_full_row,
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
                    common.output_revise_sql,
                    common.revise_match_full_row,
                    tb_meta,
                    src_row_data,
                    Some(dst_row),
                    Some(&diff_col_values),
                )?;
                let log =
                    Self::build_diff_log(src_row_data, dst_row, diff_col_values, common, tb_meta)
                        .await?;
                diff.push(log);
                Self::log_revise_sql(revise_sql, sql_count);
            }
        }

        Ok(())
    }

    async fn check_and_generate_logs(
        src_data: &[RowData],
        src_keys: &[u128],
        mut dst_row_data_map: HashMap<u128, RowData>,
        common: &mut CheckerCommon,
        tb_meta: &CheckerTbMeta,
    ) -> anyhow::Result<(Vec<CheckLog>, Vec<CheckLog>, usize)> {
        let mut miss = Vec::new();
        let mut diff = Vec::new();
        let mut sql_count = 0;

        for (src_row_data, key) in src_data.iter().zip(src_keys) {
            let dst_row_data = dst_row_data_map.remove(key);

            Self::handle_inconsistency(
                src_row_data,
                dst_row_data.as_ref(),
                common,
                tb_meta,
                &mut miss,
                &mut diff,
                &mut sql_count,
            )
            .await?;
        }

        Ok((miss, diff, sql_count))
    }

    async fn check_struct_with_retry<B: Checker>(
        checker: &B,
        src_data: Vec<StructData>,
        retry_interval_secs: u64,
        max_retries: u32,
        filter: &RdbFilter,
        output_revise_sql: bool,
    ) -> anyhow::Result<(usize, usize, usize, usize)> {
        let mut total_miss = 0;
        let mut total_diff = 0;
        let mut total_extra = 0;
        let mut total_sql = 0;

        for src_struct in src_data {
            let mut src_statement = src_struct.statement.clone();
            let src_sqls = Self::collect_sqls(&mut src_statement, filter)?;
            let mut dst_statement = checker.fetch_dst_struct(&src_statement).await?;
            let mut dst_sqls = Self::collect_dst_sqls(&mut dst_statement, filter)?;
            let mut inconsistency = dst_sqls.as_ref().map_or(!src_sqls.is_empty(), |dst| {
                !Self::sqls_equal(&src_sqls, dst)
            });

            if inconsistency && max_retries > 0 {
                for _ in 0..max_retries {
                    Self::maybe_sleep(retry_interval_secs).await;

                    dst_statement = checker.fetch_dst_struct(&src_statement).await?;
                    dst_sqls = Self::collect_dst_sqls(&mut dst_statement, filter)?;
                    inconsistency = dst_sqls.as_ref().map_or(!src_sqls.is_empty(), |dst| {
                        !Self::sqls_equal(&src_sqls, dst)
                    });
                    if !inconsistency {
                        break;
                    }
                }
            }

            if inconsistency {
                let (miss_count, diff_count, extra_count, sql_count) =
                    Self::log_struct_diff(&src_sqls, dst_sqls.as_ref(), output_revise_sql);
                total_miss += miss_count;
                total_diff += diff_count;
                total_extra += extra_count;
                total_sql += sql_count;
            }
        }

        Ok((total_miss, total_diff, total_extra, total_sql))
    }

    fn compare_src_dst(
        src_row: &RowData,
        dst_row: Option<&RowData>,
    ) -> anyhow::Result<CheckResult> {
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
        src_row_data: &RowData,
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

        let mut diff_col_values = HashMap::new();
        for (col, src_val) in src {
            let dst_val = dst.get(col);
            let maybe_diff = match dst_val {
                Some(dst_val) if src_val == dst_val => None,
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
                diff_col_values.insert(col.to_owned(), diff_entry);
            }
        }

        let should_expand_doc = diff_col_values.contains_key(MongoConstants::DOC)
            && [src_row_data, dst_row_data].iter().any(|row| {
                matches!(
                    row.after.as_ref().and_then(|m| m.get(MongoConstants::DOC)),
                    Some(ColValue::MongoDoc(_))
                )
            });

        if should_expand_doc {
            diff_col_values =
                Self::expand_mongo_doc_diff(src_row_data, dst_row_data, diff_col_values);
        }

        Ok(diff_col_values)
    }

    fn log_dml(miss: &[CheckLog], diff: &[CheckLog]) {
        for log in miss {
            log_miss!("{}", log);
        }
        for log in diff {
            log_diff!("{}", log);
        }
    }

    fn collect_sqls(
        statement: &mut StructStatement,
        filter: &RdbFilter,
    ) -> anyhow::Result<BTreeMap<String, String>> {
        Ok(statement.to_sqls(filter)?.into_iter().collect())
    }

    fn collect_dst_sqls(
        statement: &mut StructStatement,
        filter: &RdbFilter,
    ) -> anyhow::Result<Option<BTreeMap<String, String>>> {
        if matches!(statement, StructStatement::Unknown) {
            return Ok(None);
        }
        Ok(Some(Self::collect_sqls(statement, filter)?))
    }

    fn sqls_equal(
        src_sqls: &BTreeMap<String, String>,
        dst_sqls: &BTreeMap<String, String>,
    ) -> bool {
        if src_sqls.len() != dst_sqls.len() {
            return false;
        }
        for (key, src_sql) in src_sqls {
            if dst_sqls.get(key) != Some(src_sql) {
                return false;
            }
        }
        true
    }

    fn log_struct_diff(
        src_sqls: &BTreeMap<String, String>,
        dst_sqls: Option<&BTreeMap<String, String>>,
        output_revise_sql: bool,
    ) -> (usize, usize, usize, usize) {
        let mut miss_count = 0;
        let mut diff_count = 0;
        let mut extra_count = 0;
        let mut sql_count = 0;

        match dst_sqls {
            None => {
                for (key, src_sql) in src_sqls {
                    let log = StructCheckLog {
                        key: key.clone(),
                        src_sql: Some(src_sql.clone()),
                        dst_sql: None,
                    };
                    log_miss!("{}", log);
                    miss_count += 1;
                    if output_revise_sql {
                        log_sql!("{}", src_sql);
                        sql_count += 1;
                    }
                }
            }
            Some(dst_sqls) => {
                for (key, src_sql) in src_sqls {
                    if let Some(dst_sql) = dst_sqls.get(key) {
                        if src_sql != dst_sql {
                            let log = StructCheckLog {
                                key: key.clone(),
                                src_sql: Some(src_sql.clone()),
                                dst_sql: Some(dst_sql.clone()),
                            };
                            log_diff!("{}", log);
                            diff_count += 1;
                            if output_revise_sql {
                                log_sql!("{}", src_sql);
                                sql_count += 1;
                            }
                        }
                    } else {
                        let log = StructCheckLog {
                            key: key.clone(),
                            src_sql: Some(src_sql.clone()),
                            dst_sql: None,
                        };
                        log_miss!("{}", log);
                        miss_count += 1;
                        if output_revise_sql {
                            log_sql!("{}", src_sql);
                            sql_count += 1;
                        }
                    }
                }

                for (key, dst_sql) in dst_sqls {
                    if !src_sqls.contains_key(key) {
                        let log = StructCheckLog {
                            key: key.clone(),
                            src_sql: None,
                            dst_sql: Some(dst_sql.clone()),
                        };
                        log_extra!("{}", log);
                        extra_count += 1;
                    }
                }
            }
        }

        (miss_count, diff_count, extra_count, sql_count)
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
            let mapped_col = col_map.get(&col).unwrap_or(&col).to_owned();
            mapped.insert(mapped_col, val);
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
        src_row_data: &RowData,
        common: &mut CheckerCommon,
        tb_meta: &CheckerTbMeta,
    ) -> anyhow::Result<CheckLog> {
        let (mapped_schema, mapped_tb) = common
            .reverse_router
            .get_tb_map(&src_row_data.schema, &src_row_data.tb);
        let has_col_map = common
            .reverse_router
            .get_col_map(&src_row_data.schema, &src_row_data.tb)
            .is_some();
        let schema_changed = src_row_data.schema != mapped_schema || src_row_data.tb != mapped_tb;

        let routed_row = if has_col_map {
            Cow::Owned(common.reverse_router.route_row(src_row_data.clone()))
        } else {
            Cow::Borrowed(src_row_data)
        };
        let (schema, tb) = if has_col_map {
            (routed_row.schema.clone(), routed_row.tb.clone())
        } else {
            (mapped_schema.to_string(), mapped_tb.to_string())
        };

        let id_col_values = if let Some(meta_manager) = common.extractor_meta_manager.as_mut() {
            let src_tb_meta = meta_manager.get_tb_meta(&schema, &tb).await?;
            Self::build_id_col_values(routed_row.as_ref(), src_tb_meta)
                .context("Failed to build ID col values")?
        } else {
            Self::build_id_col_values(routed_row.as_ref(), tb_meta.basic()).unwrap_or_default()
        };

        let src_row = if common.output_full_row {
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
        src_row_data: &RowData,
        dst_row_data: &RowData,
        diff_col_values: HashMap<String, DiffColValue>,
        common: &mut CheckerCommon,
        tb_meta: &CheckerTbMeta,
    ) -> anyhow::Result<CheckLog> {
        let mut log = Self::build_miss_log(src_row_data, common, tb_meta).await?;

        log.diff_col_values =
            Self::map_diff_col_values(&common.reverse_router, src_row_data, diff_col_values);
        log.dst_row =
            Self::maybe_build_dst_row(&common.reverse_router, dst_row_data, common.output_full_row);

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
        let mut id_col_values = HashMap::new();
        let after = row_data.require_after().ok()?;

        for col in tb_meta.id_cols.iter() {
            let val = after.get(col)?.to_option_string();
            id_col_values.insert(col.to_owned(), val);
        }
        Some(id_col_values)
    }

    fn expand_mongo_doc_diff(
        src_row_data: &RowData,
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

    async fn sink_struct<B: Checker>(checker: &mut B, data: Vec<StructData>) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let (recheck_settings, filter, output_revise_sql) = {
            let common = checker.common_mut();
            (
                (common.retry_interval_secs, common.max_retries),
                common.filter.clone(),
                common.output_revise_sql,
            )
        };
        let (miss_count, diff_count, extra_count, sql_count) = Self::check_struct_with_retry(
            checker,
            data,
            recheck_settings.0,
            recheck_settings.1,
            &filter,
            output_revise_sql,
        )
        .await?;

        let summary = &mut checker.common_mut().summary;
        summary.miss_count += miss_count;
        summary.diff_count += diff_count;
        summary.extra_count += extra_count;

        if sql_count > 0 {
            summary.sql_count = Some(summary.sql_count.unwrap_or(0) + sql_count);
        }

        Ok(())
    }

    pub async fn sink_dml<B: Checker>(
        checker: &mut B,
        data: Vec<RowData>,
        batch: bool,
    ) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        if !batch {
            return Self::serial_check(checker, data).await;
        }

        let batch_size = checker.common_mut().batch_size;
        if batch_size == 0 {
            return Ok(());
        }

        for chunk in data.chunks(batch_size) {
            Self::batch_check(checker, chunk).await?;
        }
        Ok(())
    }

    async fn process_batch<B: Checker>(
        checker: &mut B,
        data: &[RowData],
        is_serial_mode: bool,
    ) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let tb_meta = checker.get_tb_meta_by_row(&data[0]).await?;
        let start_time = tokio::time::Instant::now();

        // 1. batch fetch all dst rows and start metrics
        let data_refs: Vec<&RowData> = data.iter().collect();
        let dst_rows = checker.fetch_batch(&tb_meta, &data_refs).await?;
        let mut dst_row_data_map = HashMap::with_capacity(dst_rows.len());
        for row in dst_rows {
            let key = row.get_hash_code(tb_meta.basic())?;
            dst_row_data_map.insert(key, row);
        }
        let mut rts = LimitedQueue::new(1);
        rts.push((start_time.elapsed().as_millis() as u64, 1));
        let (retry_interval_secs, max_retries) = {
            let common = checker.common_mut();
            (common.retry_interval_secs, common.max_retries)
        };

        // 2. compute src keys
        let src_keys: Vec<u128> = data
            .iter()
            .map(|row| row.get_hash_code(tb_meta.basic()))
            .collect::<anyhow::Result<_>>()?;

        // 3. resolve inconsistencies with retry
        let final_dst_map = Self::resolve_inconsistencies_with_retry(
            checker,
            data,
            &src_keys,
            dst_row_data_map,
            (retry_interval_secs, max_retries),
            &tb_meta,
        )
        .await?;

        // 4. check and generate logs
        let (miss, diff, sql_count) = {
            let common = checker.common_mut();
            Self::check_and_generate_logs(data, &src_keys, final_dst_map, common, &tb_meta).await?
        };
        Self::log_dml(&miss, &diff);
        let summary = &mut checker.common_mut().summary;
        summary.end_time = chrono::Local::now().to_rfc3339();
        summary.miss_count += miss.len();
        summary.diff_count += diff.len();
        if sql_count > 0 {
            summary.sql_count = Some(summary.sql_count.unwrap_or(0) + sql_count);
        }

        // 5. update monitor metrics
        let monitor = checker.common_mut().monitor.clone();
        if is_serial_mode {
            BaseSinker::update_serial_monitor(&monitor, data.len() as u64, 0).await?;
        } else {
            BaseSinker::update_batch_monitor(&monitor, data.len() as u64, 0).await?;
        }
        BaseSinker::update_monitor_rt(&monitor, &rts).await
    }

    async fn serial_check<B: Checker>(checker: &mut B, data: Vec<RowData>) -> anyhow::Result<()> {
        Self::process_batch(checker, &data, true).await
    }

    async fn batch_check<B: Checker>(checker: &mut B, data: &[RowData]) -> anyhow::Result<()> {
        Self::process_batch(checker, data, false).await
    }

    pub async fn close<B: Checker>(checker: &mut B) -> anyhow::Result<()> {
        let common = checker.common_mut();
        let global_summary_opt = common.global_summary.clone();
        let summary = &mut common.summary;
        if summary.miss_count > 0 || summary.diff_count > 0 || summary.extra_count > 0 {
            summary.end_time = chrono::Local::now().to_rfc3339();
            if let Some(global_summary) = global_summary_opt {
                let mut global_summary = global_summary.lock().await;
                global_summary.merge(summary);
            } else {
                log_summary!("{}", summary);
            }
        }
        if let Some(meta_manager) = common.extractor_meta_manager.as_mut() {
            meta_manager.close().await
        } else {
            Ok(())
        }
    }
}
