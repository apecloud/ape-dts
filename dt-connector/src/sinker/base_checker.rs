use anyhow::Context;
use async_mutex::Mutex;
use async_trait::async_trait;
use futures::future::BoxFuture;
use mongodb::bson::Document;
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};

use std::sync::Arc;
use tokio::time::{sleep, Duration};

use crate::{
    check_log::check_log::{CheckLog, CheckSummaryLog, DiffColValue, StructCheckLog},
    rdb_query_builder::RdbQueryBuilder,
    rdb_router::RdbRouter,
    sinker::mongo::mongo_cmd,
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
}

pub enum ReviseSqlMeta<'a> {
    Mysql(Cow<'a, MysqlTbMeta>),
    Pg(Cow<'a, PgTbMeta>),
    Mongo(Cow<'a, RdbTbMeta>),
}

pub struct ReviseSqlContext<'a> {
    pub meta: ReviseSqlMeta<'a>,
    pub match_full_row: bool,
}

pub enum CheckInconsistency {
    Miss,
    Diff(HashMap<String, DiffColValue>),
}

pub type CheckResult = Option<CheckInconsistency>;

impl<'a> ReviseSqlContext<'a> {
    pub fn from_checker_meta(meta: &'a CheckerTbMeta, match_full_row: bool) -> Self {
        match meta {
            CheckerTbMeta::Mysql(m) => Self {
                meta: ReviseSqlMeta::Mysql(Cow::Borrowed(m)),
                match_full_row,
            },
            CheckerTbMeta::Pg(m) => Self {
                meta: ReviseSqlMeta::Pg(Cow::Borrowed(m)),
                match_full_row,
            },
            CheckerTbMeta::Mongo(m) => Self {
                meta: ReviseSqlMeta::Mongo(Cow::Borrowed(m)),
                match_full_row,
            },
        }
    }

    pub fn mongo(meta: &'a RdbTbMeta) -> Self {
        Self {
            meta: ReviseSqlMeta::Mongo(Cow::Borrowed(meta)),
            match_full_row: false,
        }
    }

    pub fn build_miss_sql(&self, src_row_data: &RowData) -> anyhow::Result<Option<String>> {
        let after = match &src_row_data.after {
            Some(after) if !after.is_empty() => after.clone(),
            _ => return Ok(None),
        };
        // 1. mongo
        if let ReviseSqlMeta::Mongo(_) = self.meta {
            return Ok(mongo_cmd::build_insert_cmd(src_row_data));
        }
        // 2. rdb
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

    pub fn build_diff_sql(
        &self,
        src_row_data: &RowData,
        dst_row_data: &RowData,
        diff_col_values: &HashMap<String, DiffColValue>,
    ) -> anyhow::Result<Option<String>> {
        if diff_col_values.is_empty() {
            return Ok(None);
        }
        // 1. mongo
        if let ReviseSqlMeta::Mongo(_) = self.meta {
            return Ok(mongo_cmd::build_update_cmd(src_row_data, diff_col_values));
        }
        // 2. rdb
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

        self.build_update_query(&update_row)
    }

    fn build_insert_query(&self, row_data: &RowData) -> anyhow::Result<Option<String>> {
        match &self.meta {
            ReviseSqlMeta::Mysql(meta) => RdbQueryBuilder::new_for_mysql(meta.as_ref(), None)
                .get_query_sql(row_data, false)
                .map(Some),
            ReviseSqlMeta::Pg(meta) => RdbQueryBuilder::new_for_pg(meta.as_ref(), None)
                .get_query_sql(row_data, false)
                .map(Some),
            ReviseSqlMeta::Mongo(_) => unreachable!("Mongo should be handled in build_miss_sql"),
        }
    }

    fn build_update_query(&self, row_data: &RowData) -> anyhow::Result<Option<String>> {
        match &self.meta {
            ReviseSqlMeta::Mysql(meta) => {
                let meta_cow = if self.match_full_row {
                    let mut owned = meta.clone().into_owned();
                    owned.basic.id_cols = owned.basic.cols.clone();
                    Cow::Owned(owned)
                } else {
                    Cow::Borrowed(meta.as_ref())
                };

                RdbQueryBuilder::new_for_mysql(meta_cow.as_ref(), None)
                    .get_query_sql(row_data, false)
                    .map(Some)
            }
            ReviseSqlMeta::Pg(meta) => {
                let meta_cow = if self.match_full_row {
                    let mut owned = meta.clone().into_owned();
                    owned.basic.id_cols = owned.basic.cols.clone();
                    Cow::Owned(owned)
                } else {
                    Cow::Borrowed(meta.as_ref())
                };

                RdbQueryBuilder::new_for_pg(meta_cow.as_ref(), None)
                    .get_query_sql(row_data, false)
                    .map(Some)
            }
            ReviseSqlMeta::Mongo(_) => unreachable!("Mongo should be handled in build_miss_sql"),
        }
    }
}

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
    pub recheck_interval_secs: u64,
    pub recheck_attempts: u32,
}

impl CheckerCommon {
    pub fn recheck_settings(&self) -> (u64, u32) {
        (self.recheck_interval_secs, self.recheck_attempts)
    }
}

pub struct BaseChecker {}

pub struct CheckItemContext<'a> {
    pub extractor_meta_manager: Option<&'a mut RdbMetaManager>,
    pub revise_ctx: Option<&'a ReviseSqlContext<'a>>,
    pub reverse_router: &'a RdbRouter,
    pub output_full_row: bool,
    pub tb_meta: &'a CheckerTbMeta, // Source table meta for RDB, or mock meta for Mongo
}

impl BaseChecker {
    pub async fn resolve_inconsistencies_with_retry<K, F>(
        src_data: &[RowData],
        mut dst_row_data_map: HashMap<K, RowData>,
        recheck_settings: (u64, u32),
        get_key: impl Fn(&RowData) -> anyhow::Result<K>,
        fetch_batch_subset: F,
    ) -> anyhow::Result<(Vec<usize>, HashMap<K, RowData>)>
    where
        K: std::hash::Hash + Eq + Send + Sync + Clone,
        F: Fn(Vec<K>) -> BoxFuture<'static, anyhow::Result<HashMap<K, RowData>>>,
    {
        let (recheck_delay_secs, recheck_attempts) = recheck_settings;
        let mut inconsistent_indices = Vec::new();

        // Initial check
        for (i, src_row) in src_data.iter().enumerate() {
            let key = get_key(src_row)?;
            let dst_row = dst_row_data_map.get(&key);
            if Self::compare_src_dst(src_row, dst_row)?.is_some() {
                inconsistent_indices.push(i);
            }
        }

        if inconsistent_indices.is_empty() || recheck_attempts == 0 {
            return Ok((inconsistent_indices, dst_row_data_map));
        }

        // Retry loop
        for _ in 0..recheck_attempts {
            if inconsistent_indices.is_empty() {
                break;
            }

            if recheck_delay_secs > 0 {
                sleep(Duration::from_secs(recheck_delay_secs)).await;
            }

            let keys_to_fetch: Vec<K> = inconsistent_indices
                .iter()
                .map(|&i| get_key(&src_data[i]))
                .collect::<anyhow::Result<_>>()?;

            let new_dst_rows = fetch_batch_subset(keys_to_fetch).await?;
            dst_row_data_map.extend(new_dst_rows);

            // Re-evaluate inconsistencies
            let mut still_inconsistent = Vec::new();
            for &i in &inconsistent_indices {
                let src_row = &src_data[i];
                let key = get_key(src_row)?;
                let dst_row = dst_row_data_map.get(&key);

                if Self::compare_src_dst(src_row, dst_row)?.is_some() {
                    still_inconsistent.push(i);
                }
            }
            inconsistent_indices = still_inconsistent;
        }

        Ok((inconsistent_indices, dst_row_data_map))
    }

    pub async fn check_and_generate_logs<K>(
        src_data: &[RowData],
        mut dst_row_data_map: HashMap<K, RowData>,
        get_key: impl Fn(&RowData) -> anyhow::Result<K>,
        ctx: &mut CheckItemContext<'_>,
    ) -> anyhow::Result<(Vec<CheckLog>, Vec<CheckLog>, usize)>
    where
        K: std::hash::Hash + Eq + Send + Sync + Clone,
    {
        let mut miss = Vec::new();
        let mut diff = Vec::new();
        let mut sql_count = 0;

        for src_row_data in src_data {
            let key = get_key(src_row_data)?;
            let dst_row_data = dst_row_data_map.remove(&key);

            let check_result = Self::compare_src_dst(src_row_data, dst_row_data.as_ref())?;

            if let Some(res) = check_result {
                let revise_sql = match res {
                    CheckInconsistency::Miss => {
                        let log = Self::build_any_miss_log(src_row_data, ctx).await?;
                        miss.push(log);
                        ctx.revise_ctx
                            .as_ref()
                            .map(|c| c.build_miss_sql(src_row_data))
                            .transpose()?
                            .flatten()
                    }
                    CheckInconsistency::Diff(diff_col_values) => {
                        let dst_row = dst_row_data.as_ref().context("missing dst row in diff")?;
                        let log = Self::build_any_diff_log(
                            src_row_data,
                            dst_row,
                            diff_col_values.clone(),
                            ctx,
                        )
                        .await?;
                        diff.push(log);
                        ctx.revise_ctx
                            .as_ref()
                            .map(|c| c.build_diff_sql(src_row_data, dst_row, &diff_col_values))
                            .transpose()?
                            .flatten()
                    }
                };

                if let Some(sql) = revise_sql {
                    log_sql!("{}", sql);
                    sql_count += 1;
                }
            }
        }

        Ok((miss, diff, sql_count))
    }

    pub async fn check_and_process_item<F>(
        src_row_data: &RowData,
        dst_row_data: Option<RowData>,
        recheck_delay_secs: u64,
        recheck_attempts: u32,
        fetch_latest: F,
        ctx: &mut CheckItemContext<'_>,
    ) -> anyhow::Result<(CheckResult, Option<RowData>, usize)>
    where
        F: Fn(&RowData) -> BoxFuture<'static, anyhow::Result<Option<RowData>>>,
    {
        let (check_result, final_dst_row) = Self::check_row_with_retry(
            src_row_data,
            dst_row_data,
            recheck_delay_secs,
            recheck_attempts,
            fetch_latest,
        )
        .await?;

        let mut sql_count = 0;
        if let Some(res) = &check_result {
            let revise_sql = match res {
                CheckInconsistency::Miss => ctx
                    .revise_ctx
                    .as_ref()
                    .map(|c| c.build_miss_sql(src_row_data))
                    .transpose()?
                    .flatten(),
                CheckInconsistency::Diff(diff_col_values) => ctx
                    .revise_ctx
                    .as_ref()
                    .map(|c| {
                        c.build_diff_sql(
                            src_row_data,
                            final_dst_row.as_ref().unwrap(),
                            diff_col_values,
                        )
                    })
                    .transpose()?
                    .flatten(),
            };

            if let Some(sql) = revise_sql {
                log_sql!("{}", sql);
                sql_count += 1;
            }
        }

        Ok((check_result, final_dst_row, sql_count))
    }

    pub async fn build_any_miss_log(
        src_row_data: &RowData,
        ctx: &mut CheckItemContext<'_>,
    ) -> anyhow::Result<CheckLog> {
        match ctx.extractor_meta_manager.as_deref_mut() {
            Some(meta_manager) => Ok(Self::build_miss_log(
                src_row_data,
                meta_manager,
                ctx.reverse_router,
                ctx.output_full_row,
            )
            .await?),
            None => Self::build_mongo_miss_log(
                src_row_data,
                ctx.tb_meta.basic(),
                ctx.reverse_router,
                ctx.output_full_row,
            ),
        }
    }

    pub async fn build_any_diff_log(
        src_row_data: &RowData,
        dst_row_data: &RowData,
        diff_col_values: HashMap<String, DiffColValue>,
        ctx: &mut CheckItemContext<'_>,
    ) -> anyhow::Result<CheckLog> {
        match ctx.extractor_meta_manager.as_deref_mut() {
            Some(meta_manager) => Ok(Self::build_diff_log(
                src_row_data,
                dst_row_data,
                diff_col_values,
                meta_manager,
                ctx.reverse_router,
                ctx.output_full_row,
            )
            .await?),
            None => Self::build_mongo_diff_log(
                src_row_data,
                dst_row_data,
                diff_col_values,
                ctx.tb_meta.basic(),
                ctx.reverse_router,
                ctx.output_full_row,
            ),
        }
    }

    pub async fn check_struct_with_retry<F>(
        src_data: Vec<StructData>,
        recheck_delay_secs: u64,
        recheck_attempts: u32,
        filter: &RdbFilter,
        output_revise_sql: bool,
        fetch_dst_struct: F,
    ) -> anyhow::Result<(usize, usize, usize, usize)>
    where
        F: Fn(&StructStatement) -> BoxFuture<'_, anyhow::Result<StructStatement>>,
    {
        let mut total_miss = 0;
        let mut total_diff = 0;
        let mut total_extra = 0;
        let mut total_sql = 0;

        for src_struct in src_data {
            let src_statement = &mut src_struct.statement.clone();
            let mut dst_statement = fetch_dst_struct(src_statement).await?;

            let mut inconsistency;
            {
                let mut src_clone = src_statement.clone();
                let mut dst_clone = dst_statement.clone();
                let (miss, diff, extra) =
                    Self::compare_struct_metadata(&mut src_clone, &mut dst_clone, filter)?;
                inconsistency = miss + diff + extra > 0;
            }

            if inconsistency && recheck_attempts > 0 {
                for _ in 0..recheck_attempts {
                    if recheck_delay_secs > 0 {
                        sleep(Duration::from_secs(recheck_delay_secs)).await;
                    }

                    dst_statement = fetch_dst_struct(src_statement).await?;

                    let mut src_clone = src_statement.clone();
                    let mut dst_clone = dst_statement.clone();
                    let (miss, diff, extra) =
                        Self::compare_struct_metadata(&mut src_clone, &mut dst_clone, filter)?;
                    inconsistency = miss + diff + extra > 0;
                    if !inconsistency {
                        break;
                    }
                }
            }

            if inconsistency {
                let (miss_count, diff_count, extra_count, sql_count) = Self::compare_struct_all(
                    src_statement,
                    &mut dst_statement,
                    filter,
                    output_revise_sql,
                )?;
                total_miss += miss_count;
                total_diff += diff_count;
                total_extra += extra_count;
                total_sql += sql_count;
            }
        }

        Ok((total_miss, total_diff, total_extra, total_sql))
    }

    pub async fn check_row_with_retry<F>(
        src_row: &RowData,
        mut dst_row: Option<RowData>,
        recheck_delay_secs: u64,
        recheck_attempts: u32,
        fetch_latest: F,
    ) -> anyhow::Result<(CheckResult, Option<RowData>)>
    where
        F: Fn(&RowData) -> BoxFuture<'static, anyhow::Result<Option<RowData>>>,
    {
        let mut check_result = Self::compare_src_dst(src_row, dst_row.as_ref())?;

        if check_result.is_some() && recheck_attempts > 0 {
            for _ in 0..recheck_attempts {
                if recheck_delay_secs > 0 {
                    sleep(Duration::from_secs(recheck_delay_secs)).await;
                }

                dst_row = fetch_latest(src_row).await?;

                check_result = Self::compare_src_dst(src_row, dst_row.as_ref())?;
                if check_result.is_none() {
                    break;
                }
            }
        }

        Ok((check_result, dst_row))
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

    pub fn compare_row_data(
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
                    let src_type = Self::col_value_type_name(src_val);
                    let dst_type = Self::col_value_type_name(dst_val);
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
                    src_type: Some(Self::col_value_type_name(src_val).to_string()),
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

    pub fn log_dml(miss: &[CheckLog], diff: &[CheckLog]) {
        for log in miss {
            log_miss!("{}", log);
        }
        for log in diff {
            log_diff!("{}", log);
        }
    }

    pub fn compare_struct_all(
        src_statement: &mut StructStatement,
        dst_statement: &mut StructStatement,
        filter: &RdbFilter,
        output_revise_sql: bool,
    ) -> anyhow::Result<(usize, usize, usize, usize)> {
        if matches!(dst_statement, StructStatement::Unknown) {
            let sqls = src_statement.to_sqls(filter)?;
            let count = sqls.len();
            for (key, src_sql) in sqls {
                let log = StructCheckLog {
                    key,
                    src_sql: Some(src_sql.clone()),
                    dst_sql: None,
                };
                log_miss!("{}", log);
                if output_revise_sql {
                    log_sql!("{}", src_sql);
                }
            }
            let sql_count = if output_revise_sql { count } else { 0 };
            return Ok((count, 0, 0, sql_count));
        }

        let src_sqls: HashMap<_, _> = src_statement.to_sqls(filter)?.into_iter().collect();
        let dst_sqls: HashMap<_, _> = dst_statement.to_sqls(filter)?.into_iter().collect();

        let mut miss_count = 0;
        let mut diff_count = 0;
        let mut extra_count = 0;
        let mut sql_count = 0;

        for (key, src_sql) in &src_sqls {
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

        for (key, dst_sql) in &dst_sqls {
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

        Ok((miss_count, diff_count, extra_count, sql_count))
    }

    pub fn compare_struct_metadata(
        src_statement: &mut StructStatement,
        dst_statement: &mut StructStatement,
        filter: &RdbFilter,
    ) -> anyhow::Result<(usize, usize, usize)> {
        let src_sqls: HashMap<_, _> = src_statement.to_sqls(filter)?.into_iter().collect();
        let dst_sqls: HashMap<_, _> = dst_statement.to_sqls(filter)?.into_iter().collect();

        let mut miss_count = 0;
        let mut diff_count = 0;
        let mut extra_count = 0;

        for (key, src_sql) in &src_sqls {
            match dst_sqls.get(key) {
                Some(dst_sql) if src_sql == dst_sql => {}
                Some(_) => diff_count += 1,
                None => miss_count += 1,
            }
        }

        for key in dst_sqls.keys() {
            if !src_sqls.contains_key(key) {
                extra_count += 1;
            }
        }

        Ok((miss_count, diff_count, extra_count))
    }

    pub async fn build_miss_log(
        src_row_data: &RowData,
        extractor_meta_manager: &mut RdbMetaManager,
        reverse_router: &RdbRouter,
        output_full_row: bool,
    ) -> anyhow::Result<CheckLog> {
        let (mapped_schema, mapped_tb) =
            reverse_router.get_tb_map(&src_row_data.schema, &src_row_data.tb);
        let has_col_map = reverse_router
            .get_col_map(&src_row_data.schema, &src_row_data.tb)
            .is_some();
        let schema_changed = src_row_data.schema != mapped_schema || src_row_data.tb != mapped_tb;

        let (routed_row_data, schema_for_meta, tb_for_meta): (Cow<RowData>, String, String) =
            if has_col_map {
                let routed = reverse_router.route_row(src_row_data.clone());
                let schema = routed.schema.clone();
                let tb = routed.tb.clone();
                (Cow::Owned(routed), schema, tb)
            } else {
                (
                    Cow::Borrowed(src_row_data),
                    mapped_schema.to_string(),
                    mapped_tb.to_string(),
                )
            };

        // None if schema/tb not routed
        let target_schema = schema_changed.then(|| src_row_data.schema.clone());
        let target_tb = schema_changed.then(|| src_row_data.tb.clone());

        let src_tb_meta = extractor_meta_manager
            .get_tb_meta(&schema_for_meta, &tb_for_meta)
            .await?;

        let id_col_values = Self::build_id_col_values(&routed_row_data, src_tb_meta)
            .context("Failed to build ID col values")?;

        if output_full_row {
            let src_row = Self::clone_row_values(&routed_row_data);
            Ok(CheckLog {
                schema: schema_for_meta,
                tb: tb_for_meta,
                target_schema,
                target_tb,
                id_col_values,
                diff_col_values: HashMap::new(),
                src_row,
                dst_row: None,
            })
        } else {
            Ok(CheckLog {
                schema: schema_for_meta,
                tb: tb_for_meta,
                target_schema,
                target_tb,
                id_col_values,
                diff_col_values: HashMap::new(),
                src_row: None,
                dst_row: None,
            })
        }
    }

    pub async fn build_diff_log(
        src_row_data: &RowData,
        dst_row_data: &RowData,
        diff_col_values: HashMap<String, DiffColValue>,
        extractor_meta_manager: &mut RdbMetaManager,
        reverse_router: &RdbRouter,
        output_full_row: bool,
    ) -> anyhow::Result<CheckLog> {
        let mut log = Self::build_miss_log(
            src_row_data,
            extractor_meta_manager,
            reverse_router,
            output_full_row,
        )
        .await?;

        let mapped_diff_values = if let Some(col_map) =
            reverse_router.get_col_map(&src_row_data.schema, &src_row_data.tb)
        {
            let mut mapped = HashMap::with_capacity(diff_col_values.len());
            for (col, val) in diff_col_values {
                let mapped_col = col_map.get(&col).unwrap_or(&col).to_owned();
                mapped.insert(mapped_col, val);
            }
            mapped
        } else {
            diff_col_values
        };

        log.diff_col_values = mapped_diff_values;

        if output_full_row {
            let has_col_map = reverse_router
                .get_col_map(&dst_row_data.schema, &dst_row_data.tb)
                .is_some();
            log.dst_row = if has_col_map {
                let reverse_dst_row_data = reverse_router.route_row(dst_row_data.clone());
                Self::clone_row_values(&reverse_dst_row_data)
            } else {
                Self::clone_row_values(dst_row_data)
            };
        }

        Ok(log)
    }

    pub fn build_mongo_miss_log(
        src_row_data: &RowData,
        tb_meta: &RdbTbMeta,
        reverse_router: &RdbRouter,
        output_full_row: bool,
    ) -> anyhow::Result<CheckLog> {
        let (mapped_schema, mapped_tb) =
            reverse_router.get_tb_map(&src_row_data.schema, &src_row_data.tb);
        let has_col_map = reverse_router
            .get_col_map(&src_row_data.schema, &src_row_data.tb)
            .is_some();
        let schema_changed = src_row_data.schema != mapped_schema || src_row_data.tb != mapped_tb;

        let routed_row_data: Cow<RowData> = if has_col_map {
            Cow::Owned(reverse_router.route_row(src_row_data.clone()))
        } else {
            Cow::Borrowed(src_row_data)
        };

        let (schema_for_log, tb_for_log) = if has_col_map {
            (routed_row_data.schema.clone(), routed_row_data.tb.clone())
        } else {
            (mapped_schema.to_string(), mapped_tb.to_string())
        };

        let id_col_values =
            Self::build_id_col_values(&routed_row_data, tb_meta).unwrap_or_default();

        let src_row = if output_full_row {
            Self::clone_row_values(&routed_row_data)
        } else {
            None
        };

        Ok(CheckLog {
            schema: schema_for_log,
            tb: tb_for_log,
            target_schema: schema_changed.then(|| src_row_data.schema.clone()),
            target_tb: schema_changed.then(|| src_row_data.tb.clone()),
            id_col_values,
            diff_col_values: HashMap::new(),
            src_row,
            dst_row: None,
        })
    }

    pub fn build_mongo_diff_log(
        src_row_data: &RowData,
        dst_row_data: &RowData,
        diff_col_values: HashMap<String, DiffColValue>,
        tb_meta: &RdbTbMeta,
        reverse_router: &RdbRouter,
        output_full_row: bool,
    ) -> anyhow::Result<CheckLog> {
        let mut diff_log =
            Self::build_mongo_miss_log(src_row_data, tb_meta, reverse_router, output_full_row)?;

        diff_log.diff_col_values = diff_col_values;

        if output_full_row {
            let has_col_map = reverse_router
                .get_col_map(&dst_row_data.schema, &dst_row_data.tb)
                .is_some();
            diff_log.dst_row = if has_col_map {
                let reverse_dst_row_data = reverse_router.route_row(dst_row_data.clone());
                Self::clone_row_values(&reverse_dst_row_data)
            } else {
                Self::clone_row_values(dst_row_data)
            };
        }

        Ok(diff_log)
    }

    fn clone_row_values(row_data: &RowData) -> Option<HashMap<String, ColValue>> {
        match row_data.row_type {
            RowType::Insert | RowType::Update => row_data.after.clone(),
            RowType::Delete => row_data.before.clone(),
        }
    }

    fn col_value_type_name(value: &ColValue) -> &'static str {
        match value {
            ColValue::None => "None",
            ColValue::Bool(_) => "Bool",
            ColValue::Tiny(_) => "Tiny",
            ColValue::UnsignedTiny(_) => "UnsignedTiny",
            ColValue::Short(_) => "Short",
            ColValue::UnsignedShort(_) => "UnsignedShort",
            ColValue::Long(_) => "Long",
            ColValue::UnsignedLong(_) => "UnsignedLong",
            ColValue::LongLong(_) => "LongLong",
            ColValue::UnsignedLongLong(_) => "UnsignedLongLong",
            ColValue::Float(_) => "Float",
            ColValue::Double(_) => "Double",
            ColValue::Decimal(_) => "Decimal",
            ColValue::Time(_) => "Time",
            ColValue::Date(_) => "Date",
            ColValue::DateTime(_) => "DateTime",
            ColValue::Timestamp(_) => "Timestamp",
            ColValue::Year(_) => "Year",
            ColValue::String(_) => "String",
            ColValue::RawString(_) => "RawString",
            ColValue::Blob(_) => "Blob",
            ColValue::Bit(_) => "Bit",
            ColValue::Set(_) => "Set",
            ColValue::Enum(_) => "Enum",
            ColValue::Set2(_) => "Set2",
            ColValue::Enum2(_) => "Enum2",
            ColValue::Json(_) => "Json",
            ColValue::Json2(_) => "Json2",
            ColValue::Json3(_) => "Json3",
            ColValue::MongoDoc(_) => "MongoDoc",
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

    pub fn expand_mongo_doc_diff(
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

    pub async fn standard_sink_struct<B: CheckerBackend>(
        backend: &mut B,
        common: &mut CheckerCommon,
        data: Vec<StructData>,
    ) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let recheck_settings = common.recheck_settings();
        let filter = common.filter.clone();
        let output_revise_sql = common.output_revise_sql;
        let fetcher = backend.clone();

        let (miss_count, diff_count, extra_count, sql_count) = Self::check_struct_with_retry(
            data,
            recheck_settings.0,
            recheck_settings.1,
            &filter,
            output_revise_sql,
            |src| {
                let fetcher = fetcher.clone();
                let src = src.clone();
                Box::pin(async move { fetcher.fetch_dst_struct(&src).await })
            },
        )
        .await?;

        let summary = &mut common.summary;
        summary.miss_count += miss_count;
        summary.diff_count += diff_count;
        summary.extra_count += extra_count;

        if sql_count > 0 {
            summary.sql_count = Some(summary.sql_count.unwrap_or(0) + sql_count);
        }

        Ok(())
    }

    pub async fn standard_sink_dml<B: CheckerBackend>(
        backend: &mut B,
        common: &mut CheckerCommon,
        mut data: Vec<RowData>,
        batch: bool,
    ) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        if !batch {
            return Self::standard_serial_check(backend, common, data).await;
        }

        let all_count = data.len();
        let mut sinked_count = 0;

        loop {
            let mut batch_size = common.batch_size;
            if all_count - sinked_count < batch_size {
                batch_size = all_count - sinked_count;
            }

            if batch_size == 0 {
                break;
            }

            Self::standard_batch_check(backend, common, &mut data, sinked_count, batch_size)
                .await?;
            sinked_count += batch_size;
        }
        Ok(())
    }

    pub async fn standard_serial_check<B: CheckerBackend>(
        backend: &mut B,
        common: &mut CheckerCommon,
        data: Vec<RowData>,
    ) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let tb_meta = backend.get_tb_meta_by_row(&data[0]).await?;
        let revise_ctx = common
            .output_revise_sql
            .then(|| ReviseSqlContext::from_checker_meta(&tb_meta, common.revise_match_full_row));
        let (recheck_delay_secs, recheck_attempts) = common.recheck_settings();
        let mut miss = Vec::new();
        let mut diff = Vec::new();
        let mut sql_count = 0;
        let mut rts = LimitedQueue::new(std::cmp::min(100, data.len()));

        let fetcher = backend.clone();
        let tb_meta_for_fetch = tb_meta.clone();
        let fetch_latest =
            move |row: &RowData| -> BoxFuture<'static, anyhow::Result<Option<RowData>>> {
                let fetcher = fetcher.clone();
                let tb_meta = tb_meta_for_fetch.clone();
                let row = row.clone();
                Box::pin(async move { fetcher.fetch_single(&tb_meta, &row).await })
            };

        let mut ctx = CheckItemContext {
            extractor_meta_manager: common.extractor_meta_manager.as_mut(),
            revise_ctx: revise_ctx.as_ref(),
            reverse_router: &common.reverse_router,
            output_full_row: common.output_full_row,
            tb_meta: &tb_meta,
        };

        for src_row_data in data.iter() {
            let start_time = tokio::time::Instant::now();
            let dst_row_data = backend.fetch_single(&tb_meta, src_row_data).await?;
            rts.push((start_time.elapsed().as_millis() as u64, 1));

            let (check_result, final_dst_row, item_sql_count) = Self::check_and_process_item(
                src_row_data,
                dst_row_data,
                recheck_delay_secs,
                recheck_attempts,
                &fetch_latest,
                &mut ctx,
            )
            .await?;

            sql_count += item_sql_count;
            if let Some(res) = check_result {
                match res {
                    CheckInconsistency::Miss => {
                        let log = Self::build_any_miss_log(src_row_data, &mut ctx).await?;
                        miss.push(log);
                    }
                    CheckInconsistency::Diff(diff_col_values) => {
                        let dst_row = final_dst_row.as_ref().context("missing dst row in diff")?;
                        let log = Self::build_any_diff_log(
                            src_row_data,
                            dst_row,
                            diff_col_values,
                            &mut ctx,
                        )
                        .await?;
                        diff.push(log);
                    }
                }
            }
        }

        Self::log_dml(&miss, &diff);

        let summary = &mut common.summary;
        summary.miss_count += miss.len();
        summary.diff_count += diff.len();
        if sql_count > 0 {
            summary.sql_count = Some(summary.sql_count.unwrap_or(0) + sql_count);
        }

        use crate::sinker::base_sinker::BaseSinker;
        BaseSinker::update_serial_monitor(&common.monitor, data.len() as u64, 0).await?;
        BaseSinker::update_monitor_rt(&common.monitor, &rts).await
    }

    pub async fn standard_batch_check<B: CheckerBackend>(
        backend: &mut B,
        common: &mut CheckerCommon,
        data: &mut [RowData],
        start: usize,
        batch: usize,
    ) -> anyhow::Result<()> {
        let first_row = &data[start];
        let tb_meta = backend.get_tb_meta_by_row(first_row).await?;
        let schema = first_row.schema.clone();
        let tb = first_row.tb.clone();

        let start_time = tokio::time::Instant::now();
        let data_slice = &data[start..start + batch];
        let dst_row_data_map = backend
            .fetch_batch(&tb_meta, &schema, &tb, data_slice)
            .await?;
        let mut rts = LimitedQueue::new(1);
        rts.push((start_time.elapsed().as_millis() as u64, 1));

        let output_full_row = common.output_full_row;
        let revise_ctx = common
            .output_revise_sql
            .then(|| ReviseSqlContext::from_checker_meta(&tb_meta, common.revise_match_full_row));
        let recheck_settings = common.recheck_settings();

        let (miss, diff, sql_count) = {
            let fetcher = backend.clone();
            let mut ctx = CheckItemContext {
                extractor_meta_manager: common.extractor_meta_manager.as_mut(),
                revise_ctx: revise_ctx.as_ref(),
                reverse_router: &common.reverse_router,
                output_full_row,
                tb_meta: &tb_meta,
            };
            let dst_tb_meta_for_key = tb_meta.clone();

            let get_key = |r: &RowData| {
                r.get_hash_code(dst_tb_meta_for_key.basic())
            };

            let src_map: HashMap<_, _> = data_slice
                .iter()
                .map(|r| (get_key(r).unwrap(), r.clone()))
                .collect();
            let s = schema.to_string();
            let t = tb.to_string();

            let (_, final_dst_map) = Self::resolve_inconsistencies_with_retry(
                data_slice,
                dst_row_data_map,
                recheck_settings,
                get_key,
                |keys| {
                    let f = fetcher.clone();
                    let m = tb_meta.clone();
                    let s_map = src_map.clone();
                    let s_str = s.clone();
                    let t_str = t.clone();

                    Box::pin(async move {
                        let rows: Vec<RowData> =
                            keys.iter().filter_map(|k| s_map.get(k)).cloned().collect();
                        if rows.is_empty() {
                            return Ok(HashMap::new());
                        }
                        f.fetch_batch(&m, &s_str, &t_str, &rows).await
                    })
                },
            )
            .await?;

            Self::check_and_generate_logs(data_slice, final_dst_map, get_key, &mut ctx).await?
        };

        Self::log_dml(&miss, &diff);

        let summary = &mut common.summary;
        summary.end_time = chrono::Local::now().to_rfc3339();
        summary.miss_count += miss.len();
        summary.diff_count += diff.len();
        if sql_count > 0 {
            summary.sql_count = Some(summary.sql_count.unwrap_or(0) + sql_count);
        }

        use crate::sinker::base_sinker::BaseSinker;
        let monitor = common.monitor.clone();
        BaseSinker::update_batch_monitor(&monitor, batch as u64, 0).await?;
        BaseSinker::update_monitor_rt(&monitor, &rts).await
    }

    pub async fn standard_close(common: &mut CheckerCommon) -> anyhow::Result<()> {
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

#[async_trait]
pub trait CheckerBackend: Clone + Send + Sync + 'static {
    async fn get_tb_meta_by_row(&mut self, row: &RowData) -> anyhow::Result<CheckerTbMeta>;
    async fn fetch_single(
        &self,
        tb_meta: &CheckerTbMeta,
        row: &RowData,
    ) -> anyhow::Result<Option<RowData>>;
    async fn fetch_batch(
        &self,
        tb_meta: &CheckerTbMeta,
        schema: &str,
        tb: &str,
        data: &[RowData],
    ) -> anyhow::Result<HashMap<u128, RowData>>;
    async fn fetch_dst_struct(&self, src: &StructStatement) -> anyhow::Result<StructStatement>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use dt_common::meta::{col_value::ColValue, mongo::mongo_constant::MongoConstants};
    use mongodb::bson::{doc, oid::ObjectId};
    use std::collections::HashMap;

    #[test]
    fn mongo_builds_insert_cmd() {
        let meta = RdbTbMeta::default();
        let ctx = ReviseSqlContext::mongo(&meta);
        let object_id = ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap();
        let doc = doc! { MongoConstants::ID: object_id, "name": "mongo_user" };

        let mut after = HashMap::new();
        after.insert(
            MongoConstants::DOC.to_string(),
            ColValue::MongoDoc(doc.clone()),
        );
        let row = RowData::new(
            "mongo_db".into(),
            "mongo_tb".into(),
            RowType::Insert,
            None,
            Some(after),
        );

        let sql = ctx.build_miss_sql(&row).unwrap().unwrap();
        assert!(sql.starts_with("db.mongo_tb.insertOne"));
        assert!(sql.contains("mongo_user"));
    }

    #[test]
    fn mongo_builds_update_cmd() {
        let meta = RdbTbMeta::default();
        let ctx = ReviseSqlContext::mongo(&meta);
        let object_id = ObjectId::parse_str("507f1f77bcf86cd799439012").unwrap();
        let src_doc = doc! { MongoConstants::ID: object_id, "name": "new_name", "age": 18i32 };
        let dst_doc = doc! { MongoConstants::ID: object_id, "name": "old_name" };

        let mut src_after = HashMap::new();
        src_after.insert(
            MongoConstants::DOC.to_string(),
            ColValue::MongoDoc(src_doc.clone()),
        );
        let src_row = RowData::new(
            "mongo_db".into(),
            "mongo_tb".into(),
            RowType::Insert,
            None,
            Some(src_after),
        );

        let mut dst_after = HashMap::new();
        dst_after.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(dst_doc));
        let dst_row = RowData::new(
            "mongo_db".into(),
            "mongo_tb".into(),
            RowType::Insert,
            None,
            Some(dst_after),
        );

        let diff_col_values = BaseChecker::compare_row_data(&src_row, &dst_row).unwrap();

        let sql = ctx
            .build_diff_sql(&src_row, &dst_row, &diff_col_values)
            .unwrap()
            .unwrap();
        assert!(sql.starts_with("db.mongo_tb.updateOne"));
        assert!(sql.contains("'$set'"));
        assert!(sql.contains("'name': 'new_name'"));
    }

    #[test]
    fn verify_mongo_doc_strict_type_diff() {
        use crate::sinker::base_checker::BaseChecker;
        use dt_common::meta::mongo::mongo_constant::MongoConstants;
        use dt_common::meta::{col_value::ColValue, row_data::RowData, row_type::RowType};
        use mongodb::bson::doc;
        use std::collections::HashMap;

        // Create src with Int32(5) and dst with Int64(5)
        let src_doc = doc! { "val": 5i32 };
        let dst_doc = doc! { "val": 5i64 };

        let mut src_after = HashMap::new();
        src_after.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(src_doc));
        let src_row = RowData::new(
            "db".into(),
            "coll".into(),
            RowType::Insert,
            None,
            Some(src_after),
        );

        let mut dst_after = HashMap::new();
        dst_after.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(dst_doc));
        let dst_row = RowData::new(
            "db".into(),
            "coll".into(),
            RowType::Insert,
            None,
            Some(dst_after),
        );

        // Compare row data
        let diff_col_values = BaseChecker::compare_row_data(&src_row, &dst_row).unwrap();
        assert!(diff_col_values.contains_key("val"));
        assert!(!diff_col_values.contains_key(MongoConstants::DOC));

        // Build diff log
        let tb_meta = Default::default();
        let reverse_router = crate::rdb_router::RdbRouter {
            schema_map: HashMap::new(),
            tb_map: HashMap::new(),
            col_map: HashMap::new(),
            topic_map: HashMap::new(),
        };

        let check_log = BaseChecker::build_mongo_diff_log(
            &src_row,
            &dst_row,
            diff_col_values,
            &tb_meta,
            &reverse_router,
            false,
        )
        .unwrap();

        // Verify expansion and type diff
        assert!(check_log.diff_col_values.contains_key("val"));
        assert!(!check_log.diff_col_values.contains_key(MongoConstants::DOC));

        let diff_val = check_log.diff_col_values.get("val").unwrap();
        assert_eq!(diff_val.src_type, Some("Int32".to_string()));
        assert_eq!(diff_val.dst_type, Some("Int64".to_string()));
        // Values might look the same string-wise, but types differ
        assert_eq!(diff_val.src, Some("5".to_string()));
        assert_eq!(diff_val.dst, Some("NumberLong(5)".to_string()));
    }

    #[test]
    fn expand_mongo_doc_diff_strings_unquoted() {
        let object_id = ObjectId::parse_str("507f1f77bcf86cd799439012").unwrap();
        let src_doc = doc! { MongoConstants::ID: object_id, "email": "bob@example.com" };
        let dst_doc = doc! { MongoConstants::ID: object_id, "email": "bob_updated@example.com" };

        let mut src_after = HashMap::new();
        src_after.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(src_doc));
        let src_row = RowData::new(
            "db".into(),
            "coll".into(),
            RowType::Insert,
            None,
            Some(src_after),
        );

        let mut dst_after = HashMap::new();
        dst_after.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(dst_doc));
        let dst_row = RowData::new(
            "db".into(),
            "coll".into(),
            RowType::Insert,
            None,
            Some(dst_after),
        );

        let diff_col_values = BaseChecker::compare_row_data(&src_row, &dst_row).unwrap();
        let diff_val = diff_col_values.get("email").expect("email diff missing");

        assert_eq!(diff_val.src.as_deref(), Some("bob@example.com"));
        assert_eq!(diff_val.dst.as_deref(), Some("bob_updated@example.com"));
        assert!(diff_val.src_type.is_none());
        assert!(diff_val.dst_type.is_none());
    }
}
