use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use async_mutex::Mutex;
use chrono::Local;
use sqlx::{MySql, Pool, Postgres};
use tokio::time::sleep;

use dt_common::{
    config::config_enums::DbType,
    error::{DtError, DtErrorContextExt, Stage},
    log_diff, log_info, log_miss, log_sql, log_summary,
    meta::{
        mssql::mssql_connection_pool::MssqlConnectionPool,
        struct_meta::{struct_data::StructData, structure::structure_type::StructureType},
    },
    monitor::{
        counter_type::CounterType, task_metrics::TaskMetricsType,
        task_monitor_handle::TaskMonitorHandle,
    },
    rdb_filter::RdbFilter,
};

use crate::{
    checker::check_log::{to_json_line, CheckSummaryLog, CheckTableSummaryLog, StructCheckLog},
    meta_fetcher::{
        mssql::mssql_struct_fetcher::MssqlStructFetcher,
        mysql::mysql_struct_fetcher::MysqlStructFetcher, pg::pg_struct_fetcher::PgStructFetcher,
    },
    rdb_router::RdbRouter,
    rdb_struct_filter::RdbStructFilter,
};

pub struct StructCheckerHandle {
    db_type: DbType,
    conn_pool_mysql: Option<Pool<MySql>>,
    conn_pool_pg: Option<Pool<Postgres>>,
    conn_pool_mssql: Option<MssqlConnectionPool>,
    filter: RdbFilter,
    router: Option<RdbRouter>,
    output_revise_sql: bool,
    retry_interval_secs: u64,
    max_retries: u32,
    global_summary: Option<Arc<Mutex<CheckSummaryLog>>>,
    monitor: TaskMonitorHandle,
    monitor_task_id: String,
    src_sql_map: BTreeMap<String, String>,
    dbs: HashSet<String>,
    start_time: String,
}

fn struct_table_summary(
    key: &str,
    checked_count: usize,
    miss: bool,
    diff: bool,
) -> Option<CheckTableSummaryLog> {
    let mut parts = key.splitn(4, '.');
    let object_type = parts.next()?;

    if !matches!(
        object_type,
        "table"
            | "index"
            | "constraint"
            | "column_comment"
            | "table_comment"
            | "sequence_owner"
            | "sequence"
    ) {
        return None;
    }

    Some(CheckTableSummaryLog {
        schema: parts.next()?.to_string(),
        tb: parts.next()?.to_string(),
        checked_count,
        miss_count: usize::from(miss),
        diff_count: usize::from(diff),
        ..Default::default()
    })
}

impl StructCheckerHandle {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db_type: DbType,
        conn_pool_mysql: Option<Pool<MySql>>,
        conn_pool_pg: Option<Pool<Postgres>>,
        conn_pool_mssql: Option<MssqlConnectionPool>,
        filter: RdbFilter,
        router: Option<RdbRouter>,
        output_revise_sql: bool,
        retry_interval_secs: u64,
        max_retries: u32,
        global_summary: Option<Arc<Mutex<CheckSummaryLog>>>,
        monitor: TaskMonitorHandle,
        monitor_task_id: String,
    ) -> Self {
        Self {
            db_type,
            conn_pool_mysql,
            conn_pool_pg,
            conn_pool_mssql,
            filter,
            router,
            output_revise_sql,
            retry_interval_secs,
            max_retries,
            global_summary,
            monitor,
            monitor_task_id,
            src_sql_map: BTreeMap::new(),
            dbs: HashSet::new(),
            start_time: Local::now().to_rfc3339(),
        }
    }

    fn schema_from_key(key: &str) -> Option<&str> {
        let mut parts = key.splitn(5, '.');
        match parts.next()? {
            "rbac" => (parts.next() == Some("privilege"))
                .then(|| parts.nth(1))
                .flatten(),
            _ => parts.next(),
        }
    }

    fn insert_sqls(
        sql_map: &mut BTreeMap<String, String>,
        sqls: Vec<(String, String)>,
        side: &str,
    ) -> anyhow::Result<()> {
        for (key, sql) in sqls {
            if sql_map.insert(key.clone(), sql).is_some() {
                bail!("duplicate {side} structure key after routing: {key}");
            }
        }
        Ok(())
    }

    async fn add_src_sqls(&mut self, struct_data: StructData) -> anyhow::Result<()> {
        let routed = if let Some(router) = &self.router {
            router.route_struct(struct_data)
        } else {
            struct_data
        };
        let mut statement = routed.statement;
        let sqls = statement.to_sqls(&self.filter)?;
        if !sqls.is_empty() {
            self.monitor
                .add_counter(
                    &self.monitor_task_id,
                    CounterType::RecordCount,
                    sqls.len() as u64,
                )
                .await;
        }

        for (key, sql) in sqls {
            if let Some(db) = Self::schema_from_key(&key).filter(|db| !db.is_empty()) {
                self.dbs.insert(db.to_string());
            }
            if self.src_sql_map.insert(key.clone(), sql).is_some() {
                bail!("duplicate source structure key after routing: {key}");
            }
        }
        Ok(())
    }

    async fn build_dst_sql_map(
        &self,
        dbs: &HashSet<String>,
    ) -> anyhow::Result<BTreeMap<String, String>> {
        let mut dst_map = BTreeMap::new();
        let target_filter = RdbStructFilter::for_target(self.filter.clone(), self.router.clone());
        match self.db_type {
            DbType::Mysql => {
                let conn_pool = self
                    .conn_pool_mysql
                    .as_ref()
                    .context("mysql connection pool not found")?
                    .clone();
                let meta_manager =
                    dt_common::meta::mysql::mysql_meta_manager::MysqlMetaManager::new(
                        conn_pool.clone(),
                    )
                    .await?;
                let mut fetcher = MysqlStructFetcher {
                    conn_pool,
                    dbs: dbs.clone(),
                    filter: target_filter,
                    meta_manager,
                };
                for stmt in fetcher.get_create_database_statements("").await? {
                    dst_map.extend(stmt.to_sqls(&self.filter)?);
                }
                for mut stmt in fetcher.get_create_table_statements("", "").await? {
                    dst_map.extend(stmt.to_sqls(&self.filter)?);
                }
            }
            DbType::Pg => {
                let conn_pool = self
                    .conn_pool_pg
                    .as_ref()
                    .context("postgres connection pool not found")?
                    .clone();
                let mut fetcher = PgStructFetcher {
                    conn_pool,
                    schemas: dbs.clone(),
                    filter: target_filter,
                };
                if !self.filter.filter_structure(&StructureType::Udt) {
                    for stmt in fetcher.get_udt_statements().await? {
                        dst_map.extend(stmt.to_sqls(&self.filter)?);
                    }
                }
                if !self.filter.filter_structure(&StructureType::Udf) {
                    for stmt in fetcher.get_udf_statements().await? {
                        dst_map.extend(stmt.to_sqls(&self.filter)?);
                    }
                }
                for stmt in fetcher.get_create_schema_statements("").await? {
                    dst_map.extend(stmt.to_sqls(&self.filter)?);
                }
                for mut stmt in fetcher.get_create_table_statements("", "").await? {
                    dst_map.extend(stmt.to_sqls(&self.filter)?);
                }
                if !self.filter.filter_structure(&StructureType::Rbac) {
                    for stmt in fetcher.get_create_rbac_statements().await? {
                        dst_map.extend(stmt.to_sqls(&self.filter)?);
                    }
                }
            }
            DbType::Mssql => {
                let connection_pool = self
                    .conn_pool_mssql
                    .as_ref()
                    .context("MSSQL connection pool not found")?
                    .clone();
                let mut fetcher = MssqlStructFetcher {
                    connection_pool,
                    schemas: dbs.clone(),
                    filter: target_filter,
                    allow_missing_schemas: true,
                };
                for stmt in fetcher.get_create_schema_statements("").await? {
                    Self::insert_sqls(&mut dst_map, stmt.to_sqls(&self.filter)?, "target")?;
                }
                for mut stmt in fetcher.get_create_table_statements("", "").await? {
                    Self::insert_sqls(&mut dst_map, stmt.to_sqls(&self.filter)?, "target")?;
                }
            }
            _ => bail!(DtError::InvalidConfig(format!(
                "structure checking is not supported for database type: {}",
                self.db_type
            ))
            .stage(Stage::Bootstrap)),
        }
        Ok(dst_map)
    }

    async fn compare_once(
        &self,
        src_sql_map: &BTreeMap<String, String>,
        dbs: &HashSet<String>,
        log_enabled: bool,
    ) -> anyhow::Result<CheckSummaryLog> {
        let dst_map = self.build_dst_sql_map(dbs).await?;
        Ok(Self::compare_sql_maps(
            src_sql_map,
            dst_map,
            &self.start_time,
            log_enabled,
            self.output_revise_sql,
        ))
    }

    fn compare_sql_maps(
        src_sql_map: &BTreeMap<String, String>,
        mut dst_map: BTreeMap<String, String>,
        start_time: &str,
        log_enabled: bool,
        output_revise_sql: bool,
    ) -> CheckSummaryLog {
        let mut summary = CheckSummaryLog {
            start_time: start_time.to_string(),
            checked_count: src_sql_map.len(),
            ..Default::default()
        };
        let mut sql_count = 0usize;

        for (key, src_sql) in src_sql_map {
            let dst_sql = dst_map.remove(key);
            let is_miss = dst_sql.is_none();
            let is_diff = dst_sql.as_ref().is_some_and(|dst_sql| dst_sql != src_sql);
            if let Some(table) = struct_table_summary(key, 1, is_miss, is_diff) {
                summary.merge_table(table);
            }
            if !is_miss && !is_diff {
                continue;
            }

            if is_miss {
                summary.miss_count += 1;
            } else {
                summary.diff_count += 1;
            }

            if log_enabled {
                let log = StructCheckLog::new(key, Some(src_sql.clone()), dst_sql);
                if let Some(log) = to_json_line(&log) {
                    if is_miss {
                        log_miss!("{}", log);
                    } else {
                        log_diff!("{}", log);
                    }
                }
                if output_revise_sql {
                    log_sql!("{}", src_sql);
                    sql_count += 1;
                }
            }
        }

        for (key, dst_sql) in dst_map {
            summary.diff_count += 1;
            if let Some(table) = struct_table_summary(&key, 0, false, true) {
                summary.merge_table(table);
            }
            if log_enabled {
                let log = StructCheckLog::new(&key, None, Some(dst_sql));
                if let Some(log) = to_json_line(&log) {
                    log_diff!("{}", log);
                }
            }
        }

        summary.is_consistent = summary.miss_count == 0 && summary.diff_count == 0;
        if output_revise_sql && sql_count > 0 {
            summary.sql_count = Some(sql_count);
        }
        summary.end_time = Local::now().to_rfc3339();
        summary.sort_tables();
        summary
    }

    pub async fn check_struct(
        &mut self,
        data: Vec<dt_common::meta::struct_meta::struct_data::StructData>,
    ) -> anyhow::Result<()> {
        for struct_data in data {
            self.add_src_sqls(struct_data).await?;
        }
        Ok(())
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        let mut retries_left = self.max_retries;
        let summary = loop {
            let summary = self
                .compare_once(&self.src_sql_map, &self.dbs, false)
                .await?;
            if summary.is_consistent {
                log_info!("Structure check passed - all structures are consistent");
                break summary;
            }
            if retries_left == 0 {
                break self
                    .compare_once(&self.src_sql_map, &self.dbs, true)
                    .await?;
            }
            retries_left -= 1;
            if self.retry_interval_secs > 0 {
                sleep(Duration::from_secs(self.retry_interval_secs)).await;
            }
        };

        if summary.miss_count > 0 {
            self.monitor.add_no_window_metrics(
                TaskMetricsType::CheckerMissCount,
                summary.miss_count as u64,
            );
            self.monitor
                .add_counter(
                    &self.monitor_task_id,
                    CounterType::CheckerMissCount,
                    summary.miss_count as u64,
                )
                .await;
        }
        if summary.diff_count > 0 {
            self.monitor.add_no_window_metrics(
                TaskMetricsType::CheckerDiffCount,
                summary.diff_count as u64,
            );
            self.monitor
                .add_counter(
                    &self.monitor_task_id,
                    CounterType::CheckerDiffCount,
                    summary.diff_count as u64,
                )
                .await;
        }
        if let Some(global_summary) = &self.global_summary {
            global_summary.lock().await.merge(&summary);
        } else if let Some(log) = to_json_line(&summary) {
            log_summary!("{}", log);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compare_sql_maps_reports_miss_diff_and_target_only_objects() {
        let src_sql_map = BTreeMap::from([
            ("schema.s1".to_string(), "create schema s1".to_string()),
            ("table.s1.t1".to_string(), "create table source".to_string()),
            (
                "table_comment.s1.t1".to_string(),
                "comment source".to_string(),
            ),
        ]);
        let dst_sql_map = BTreeMap::from([
            ("schema.s1".to_string(), "create schema s1".to_string()),
            ("table.s1.t1".to_string(), "create table target".to_string()),
            (
                "index.s1.extra.i1".to_string(),
                "create index target".to_string(),
            ),
        ]);

        let summary =
            StructCheckerHandle::compare_sql_maps(&src_sql_map, dst_sql_map, "start", false, false);

        assert!(!summary.is_consistent);
        assert_eq!(summary.checked_count, 3);
        assert_eq!(summary.miss_count, 1);
        assert_eq!(summary.diff_count, 2);
        assert_eq!(summary.tables.len(), 2);
        assert_eq!(summary.tables[0].schema, "s1");
        assert_eq!(summary.tables[0].tb, "extra");
        assert_eq!(summary.tables[0].checked_count, 0);
        assert_eq!(summary.tables[0].diff_count, 1);
        assert_eq!(summary.tables[1].tb, "t1");
        assert_eq!(summary.tables[1].checked_count, 2);
        assert_eq!(summary.tables[1].miss_count, 1);
        assert_eq!(summary.tables[1].diff_count, 1);
    }

    #[test]
    fn duplicate_structure_keys_are_rejected() {
        let mut sql_map = BTreeMap::new();
        let result = StructCheckerHandle::insert_sqls(
            &mut sql_map,
            vec![
                ("table.s1.t1".to_string(), "sql 1".to_string()),
                ("table.s1.t1".to_string(), "sql 2".to_string()),
            ],
            "source",
        );

        assert!(result.is_err());
    }
}
