use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{bail, Context};
use async_mutex::Mutex;
use chrono::Local;
use dt_common::{
    config::config_enums::DbType,
    error::{DtError, DtErrorContextExt, Stage},
    log_diff, log_info, log_miss, log_sql, log_summary,
    meta::{
        mssql::mssql_connection_pool::MssqlConnectionPool,
        struct_meta::{
            statement::struct_statement::StructStatement, struct_data::StructData,
            structure::structure_type::StructureType,
        },
    },
    monitor::{
        counter_type::CounterType, task_metrics::TaskMetricsType,
        task_monitor_handle::TaskMonitorHandle,
    },
    rdb_filter::RdbFilter,
};
use sqlx::{MySql, Pool, Postgres};
use tokio::time::sleep;

use crate::{
    checker::check_log::{to_json_line, CheckSummaryLog, CheckTableSummaryLog, StructCheckLog},
    meta_fetcher::{
        mssql::mssql_struct_fetcher::MssqlStructFetcher,
        mysql::mysql_struct_fetcher::MysqlStructFetcher, pg::pg_struct_fetcher::PgStructFetcher,
    },
    rdb_router::RdbRouter,
    rdb_struct_filter::RdbStructFilter,
};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct StructCheckKey {
    key: String,
    db: String,
    schema: String,
    tb: String,
}

impl StructCheckKey {
    fn new(key: String, db: &str, schema: &str, tb: &str) -> Self {
        Self {
            key,
            db: db.to_string(),
            schema: schema.to_string(),
            tb: tb.to_string(),
        }
    }
}

type StructSqlMap = BTreeMap<StructCheckKey, String>;

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
    src_sql_map: StructSqlMap,
    namespaces: HashSet<String>,
    start_time: String,
}

fn struct_table_summary(
    db_type: &DbType,
    key: &StructCheckKey,
    checked_count: usize,
    miss: bool,
    diff: bool,
) -> Option<CheckTableSummaryLog> {
    let mut parts = key.key.splitn(4, '.');
    let object_type = parts.next()?;
    if !matches!(
        object_type,
        "table"
            | "index"
            | "constraint"
            | "column_comment"
            | "table_comment"
            | "constraint_comment"
            | "index_comment"
            | "sequence_owner"
            | "sequence"
            | "sequence_comment"
    ) {
        return None;
    }

    let (db, schema, tb) = if matches!(db_type, DbType::Mssql) {
        (key.db.clone(), key.schema.clone(), key.tb.clone())
    } else {
        (
            String::new(),
            parts.next()?.to_string(),
            parts.next()?.to_string(),
        )
    };
    Some(CheckTableSummaryLog {
        db,
        schema,
        tb,
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
            namespaces: HashSet::new(),
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
        sql_map: &mut StructSqlMap,
        sqls: Vec<(String, String)>,
        side: &str,
        db: &str,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<()> {
        for (key, sql) in sqls {
            let check_key = StructCheckKey::new(key.clone(), db, schema, tb);
            if sql_map.insert(check_key, sql).is_some() {
                bail!("duplicate {side} structure key after routing: {key}");
            }
        }
        Ok(())
    }

    fn insert_statement(
        sql_map: &mut StructSqlMap,
        mut statement: StructStatement,
        filter: &RdbFilter,
        side: &str,
    ) -> anyhow::Result<()> {
        let (db, schema, tb) = statement.statement_path();
        let sqls = statement.to_sqls(filter)?;
        Self::insert_sqls(sql_map, sqls, side, &db, &schema, &tb)
    }

    async fn add_src_sqls(&mut self, struct_data: StructData) -> anyhow::Result<()> {
        let routed = if let Some(router) = &self.router {
            router.route_struct(struct_data)
        } else {
            struct_data
        };
        let mut statement = routed.statement;
        let (db, schema, tb) = statement.statement_path();
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

        for (key, _) in &sqls {
            let namespace = if matches!(self.db_type, DbType::Mssql) {
                (!db.is_empty()).then_some(db.as_str())
            } else {
                (!schema.is_empty())
                    .then_some(schema.as_str())
                    .or_else(|| Self::schema_from_key(key).filter(|schema| !schema.is_empty()))
            };
            if let Some(namespace) = namespace {
                self.namespaces.insert(namespace.to_string());
            }
        }
        Self::insert_sqls(&mut self.src_sql_map, sqls, "source", &db, &schema, &tb)?;
        Ok(())
    }

    async fn build_dst_sql_map(
        &self,
        namespaces: &HashSet<String>,
    ) -> anyhow::Result<StructSqlMap> {
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
                    dbs: namespaces.clone(),
                    filter: target_filter,
                    meta_manager,
                    allow_missing_databases: true,
                };
                for stmt in fetcher.get_create_database_statements("").await? {
                    Self::insert_statement(
                        &mut dst_map,
                        StructStatement::MysqlCreateDatabase(stmt),
                        &self.filter,
                        "target",
                    )?;
                }
                for stmt in fetcher.get_create_table_statements("", "").await? {
                    Self::insert_statement(
                        &mut dst_map,
                        StructStatement::MysqlCreateTable(stmt),
                        &self.filter,
                        "target",
                    )?;
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
                    schemas: namespaces.clone(),
                    filter: target_filter,
                    allow_missing_schemas: true,
                };
                if !self.filter.filter_structure(&StructureType::Udt) {
                    for stmt in fetcher.get_udt_statements().await? {
                        Self::insert_statement(
                            &mut dst_map,
                            StructStatement::PgCreateUdt(stmt),
                            &self.filter,
                            "target",
                        )?;
                    }
                }
                if !self.filter.filter_structure(&StructureType::Udf) {
                    for stmt in fetcher.get_udf_statements().await? {
                        Self::insert_statement(
                            &mut dst_map,
                            StructStatement::PgCreateUdf(stmt),
                            &self.filter,
                            "target",
                        )?;
                    }
                }
                for stmt in fetcher.get_create_schema_statements("").await? {
                    Self::insert_statement(
                        &mut dst_map,
                        StructStatement::PgCreateSchema(stmt),
                        &self.filter,
                        "target",
                    )?;
                }
                for stmt in fetcher.get_create_table_statements("", "").await? {
                    Self::insert_statement(
                        &mut dst_map,
                        StructStatement::PgCreateTable(stmt),
                        &self.filter,
                        "target",
                    )?;
                }
                if !self.filter.filter_structure(&StructureType::Rbac) {
                    for stmt in fetcher.get_create_rbac_statements().await? {
                        Self::insert_statement(
                            &mut dst_map,
                            StructStatement::PgCreateRbac(stmt),
                            &self.filter,
                            "target",
                        )?;
                    }
                }
            }
            DbType::Mssql => {
                let connection_pool = self
                    .conn_pool_mssql
                    .as_ref()
                    .context("MSSQL connection pool not found")?
                    .clone();
                let mut databases = namespaces.iter().cloned().collect::<Vec<_>>();
                databases.sort();
                for db in databases {
                    let mut fetcher = MssqlStructFetcher {
                        connection_pool: connection_pool.clone(),
                        db,
                        filter: target_filter.clone(),
                        allow_missing_database: true,
                    };
                    for stmt in fetcher.get_create_database_statements().await? {
                        Self::insert_statement(
                            &mut dst_map,
                            StructStatement::MssqlCreateDatabase(stmt),
                            &self.filter,
                            "target",
                        )?;
                    }
                    for stmt in fetcher.get_create_schema_statements("").await? {
                        Self::insert_statement(
                            &mut dst_map,
                            StructStatement::MssqlCreateSchema(stmt),
                            &self.filter,
                            "target",
                        )?;
                    }
                    for stmt in fetcher.get_create_table_statements("", "").await? {
                        Self::insert_statement(
                            &mut dst_map,
                            StructStatement::MssqlCreateTable(stmt),
                            &self.filter,
                            "target",
                        )?;
                    }
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
        src_sql_map: &StructSqlMap,
        namespaces: &HashSet<String>,
        log_enabled: bool,
    ) -> anyhow::Result<CheckSummaryLog> {
        let dst_map = self.build_dst_sql_map(namespaces).await?;
        Ok(Self::compare_sql_maps(
            &self.db_type,
            src_sql_map,
            dst_map,
            &self.start_time,
            log_enabled,
            self.output_revise_sql,
        ))
    }

    fn compare_sql_maps(
        db_type: &DbType,
        src_sql_map: &StructSqlMap,
        mut dst_map: StructSqlMap,
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
            if let Some(table) = struct_table_summary(db_type, key, 1, is_miss, is_diff) {
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
                let log = StructCheckLog::new(&key.key, Some(src_sql.clone()), dst_sql);
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
            if let Some(table) = struct_table_summary(db_type, &key, 0, false, true) {
                summary.merge_table(table);
            }
            if log_enabled {
                let log = StructCheckLog::new(&key.key, None, Some(dst_sql));
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
                .compare_once(&self.src_sql_map, &self.namespaces, false)
                .await?;
            if summary.is_consistent {
                log_info!("Structure check passed - all structures are consistent");
                break summary;
            }
            if retries_left == 0 {
                break self
                    .compare_once(&self.src_sql_map, &self.namespaces, true)
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

    fn sql_map(entries: &[(&str, &str, &str, &str)]) -> StructSqlMap {
        let mut sql_map = BTreeMap::new();
        for (key, schema, tb, sql) in entries {
            StructCheckerHandle::insert_sqls(
                &mut sql_map,
                vec![(key.to_string(), sql.to_string())],
                "test",
                "",
                schema,
                tb,
            )
            .unwrap();
        }
        sql_map
    }

    #[test]
    fn missing_target_database_and_table_are_reported_as_miss() {
        let src_sql_map = sql_map(&[
            (
                "database.test_db",
                "test_db",
                "",
                "CREATE DATABASE IF NOT EXISTS `test_db`",
            ),
            (
                "table.test_db.test_tb",
                "test_db",
                "test_tb",
                "CREATE TABLE IF NOT EXISTS `test_db`.`test_tb` (`id` int)",
            ),
        ]);

        let summary = StructCheckerHandle::compare_sql_maps(
            &DbType::Mysql,
            &src_sql_map,
            BTreeMap::new(),
            "start",
            false,
            false,
        );

        assert!(!summary.is_consistent);
        assert_eq!(summary.checked_count, 2);
        assert_eq!(summary.miss_count, 2);
        assert_eq!(summary.diff_count, 0);
        assert_eq!(summary.tables.len(), 1);
        assert_eq!(summary.tables[0].schema, "test_db");
        assert_eq!(summary.tables[0].tb, "test_tb");
        assert_eq!(summary.tables[0].miss_count, 1);
    }

    #[test]
    fn compare_sql_maps_reports_miss_diff_and_target_only_objects() {
        let src_sql_map = sql_map(&[
            ("schema.s1", "s1", "", "create schema s1"),
            ("table.s1.t1", "s1", "t1", "create table source"),
            ("table_comment.s1.t1", "s1", "t1", "comment source"),
        ]);
        let dst_sql_map = sql_map(&[
            ("schema.s1", "s1", "", "create schema s1"),
            ("table.s1.t1", "s1", "t1", "create table target"),
            ("index.s1.extra.i1", "s1", "extra", "create index target"),
        ]);

        let summary = StructCheckerHandle::compare_sql_maps(
            &DbType::Pg,
            &src_sql_map,
            dst_sql_map,
            "start",
            false,
            false,
        );

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
            "",
            "",
            "",
        );

        assert!(result.is_err());
    }

    #[test]
    fn equal_display_keys_in_different_mssql_paths_do_not_collide() {
        let mut sql_map = BTreeMap::new();
        StructCheckerHandle::insert_sqls(
            &mut sql_map,
            vec![(
                "table.test_db.schema.with.dot".to_string(),
                "sql 1".to_string(),
            )],
            "source",
            "test_db",
            "schema.with",
            "dot",
        )
        .unwrap();
        StructCheckerHandle::insert_sqls(
            &mut sql_map,
            vec![(
                "table.test_db.schema.with.dot".to_string(),
                "sql 2".to_string(),
            )],
            "source",
            "test_db",
            "schema",
            "with.dot",
        )
        .unwrap();

        assert_eq!(sql_map.len(), 2);
        let keys = sql_map.keys().collect::<Vec<_>>();
        assert_eq!(keys[0].schema, "schema");
        assert_eq!(keys[0].tb, "with.dot");
        assert_eq!(keys[1].schema, "schema.with");
        assert_eq!(keys[1].tb, "dot");
    }

    #[test]
    fn old_keys_keep_statement_paths() {
        let mysql_key = StructCheckKey::new(
            "constraint.db.with.dot.table.with.dot.constraint.with.dot".to_string(),
            "",
            "db.with.dot",
            "table.with.dot",
        );
        assert_eq!(
            mysql_key.key,
            "constraint.db.with.dot.table.with.dot.constraint.with.dot"
        );
        assert_eq!(mysql_key.schema, "db.with.dot");
        assert_eq!(mysql_key.tb, "table.with.dot");

        let pg_key = StructCheckKey::new(
            "column_comment.schema.with.dot.table.with.dot.column.with.dot".to_string(),
            "",
            "schema.with.dot",
            "table.with.dot",
        );
        assert_eq!(
            pg_key.key,
            "column_comment.schema.with.dot.table.with.dot.column.with.dot"
        );
        assert_eq!(pg_key.schema, "schema.with.dot");
        assert_eq!(pg_key.tb, "table.with.dot");
    }

    #[test]
    fn non_mssql_summary_uses_legacy_key_path() {
        for db_type in [DbType::Mysql, DbType::Pg] {
            let key = StructCheckKey::new(
                "table.key_schema.key_table".to_string(),
                "path_db",
                "path_schema",
                "path_table",
            );
            let summary = struct_table_summary(&db_type, &key, 1, false, false).unwrap();
            assert!(summary.db.is_empty());
            assert_eq!(summary.schema, "key_schema");
            assert_eq!(summary.tb, "key_table");
        }
    }

    #[test]
    fn mssql_sequence_and_comment_use_schema_level_summary() {
        for object_type in ["sequence", "sequence_comment"] {
            let key = StructCheckKey::new(
                format!("{object_type}.test_db.schema.with.dot.sequence.with.dot"),
                "test_db",
                "schema.with.dot",
                "",
            );
            let summary = struct_table_summary(&DbType::Mssql, &key, 1, false, false).unwrap();
            assert_eq!(summary.db, "test_db");
            assert_eq!(summary.schema, "schema.with.dot");
            assert!(summary.tb.is_empty());
        }
    }
}
