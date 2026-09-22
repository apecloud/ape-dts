use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{bail, ensure};
use async_mutex::Mutex;
use chrono::Local;
use dt_common::{
    config::config_enums::DbType,
    error::{DtError, DtErrorContextExt, Stage},
    log_diff, log_info, log_miss, log_sql, log_summary,
    meta::{
        mssql::mssql_connection_pool::MssqlConnectionPool,
        struct_meta::{
            statement::struct_statement::{StructKey, StructStatement},
            struct_data::StructData,
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
    checker::check_log::{
        to_json_line, CheckSummaryLog, CheckTableSummaryLog, StructCheckKey, StructCheckLog,
    },
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
    src_sql_map: BTreeMap<StructKey, StructCheckItem>,
    namespaces: HashSet<String>,
    start_time: String,
}

#[derive(Clone)]
struct StructCheckItem {
    source_key: StructCheckKey,
    target_key: StructCheckKey,
    sql: String,
}

impl StructCheckItem {
    #[cfg(test)]
    fn unrouted(key: StructKey, sql: &str) -> Self {
        let key = StructCheckKey::new("", key);
        Self {
            source_key: key.clone(),
            target_key: key,
            sql: sql.to_string(),
        }
    }
}

fn struct_table_summary(
    key: &StructCheckKey,
    target_key: &StructCheckKey,
    checked_count: usize,
    miss: bool,
    diff: bool,
) -> Option<CheckTableSummaryLog> {
    if !key.is_table_scoped() && !key.is_sequence() {
        return None;
    }

    let db_changed = key.db != target_key.db;
    let target_changed = key.schema != target_key.schema || key.tb != target_key.tb;

    Some(CheckTableSummaryLog {
        db: key.db.clone(),
        schema: key.schema.clone(),
        tb: key.tb.clone(),
        target_db: db_changed.then(|| target_key.db.clone()),
        target_schema: target_changed.then(|| target_key.schema.clone()),
        target_tb: target_changed.then(|| target_key.tb.clone()),
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

    async fn add_src_sqls(&mut self, struct_data: StructData) -> anyhow::Result<()> {
        let source_db = struct_data.db.clone();
        let mut source_statement = struct_data.statement.clone();
        let source_sqls = source_statement.to_sqls(&self.filter)?;
        let routed = if let Some(router) = &self.router {
            router.route_struct(struct_data)
        } else {
            struct_data
        };
        let target_db = routed.db.clone();
        let mut statement = routed.statement;
        let target_sqls = statement.to_sqls(&self.filter)?;
        ensure!(
            source_sqls.len() == target_sqls.len(),
            "source and routed structure SQL counts differ: source={}, target={}",
            source_sqls.len(),
            target_sqls.len()
        );
        if !target_sqls.is_empty() {
            self.monitor
                .add_counter(
                    &self.monitor_task_id,
                    CounterType::RecordCount,
                    target_sqls.len() as u64,
                )
                .await;
        }

        for ((source_key, _), (target_key, sql)) in source_sqls.into_iter().zip(target_sqls) {
            let source_key = StructCheckKey::new(&source_db, source_key);
            let target_check_key = StructCheckKey::new(&target_db, target_key.clone());
            let namespace = if matches!(self.db_type, DbType::Mssql) {
                &target_check_key.db
            } else {
                &target_check_key.schema
            };
            if !namespace.is_empty() {
                self.namespaces.insert(namespace.clone());
            }
            if self.src_sql_map.contains_key(&target_key) {
                bail!(DtError::InvariantViolated(format!(
                    "duplicate source structure key after routing: {target_key}"
                )));
            }
            self.src_sql_map.insert(
                target_key,
                StructCheckItem {
                    source_key,
                    target_key: target_check_key,
                    sql,
                },
            );
        }
        Ok(())
    }

    fn source_key_from_target(
        target_key: &StructCheckKey,
        router: Option<&RdbRouter>,
    ) -> StructCheckKey {
        let Some(router) = router else {
            return target_key.clone();
        };
        if target_key.is_table_scoped() {
            let (db, schema, tb) = router.reverse_get_tb_map_with_db(
                &target_key.db,
                &target_key.schema,
                &target_key.tb,
            );
            return target_key.with_location(db, schema, tb);
        }
        if target_key.key.database().is_some() {
            let db = router.reverse_get_schema_map(&target_key.db);
            return target_key.with_location(db, &target_key.schema, &target_key.tb);
        }
        if !target_key.schema.is_empty() {
            let schema = router.reverse_get_schema_map(&target_key.schema);
            return target_key.with_location(&target_key.db, schema, &target_key.tb);
        }
        target_key.clone()
    }

    fn insert_statement(
        sql_map: &mut BTreeMap<StructKey, String>,
        mut statement: StructStatement,
        filter: &RdbFilter,
        side: &str,
    ) -> anyhow::Result<()> {
        for (key, sql) in statement.to_sqls(filter)? {
            if sql_map.contains_key(&key) {
                bail!(DtError::InvariantViolated(format!(
                    "duplicate {side} structure key after routing: {key}"
                )));
            }
            sql_map.insert(key, sql);
        }
        Ok(())
    }

    async fn build_dst_sql_map(
        &self,
        namespaces: &HashSet<String>,
    ) -> anyhow::Result<BTreeMap<StructKey, String>> {
        let mut dst_map = BTreeMap::new();
        let target_filter = RdbStructFilter::for_target(self.filter.clone(), self.router.clone());
        match self.db_type {
            DbType::Mysql => {
                let conn_pool = self
                    .conn_pool_mysql
                    .as_ref()
                    .ok_or(DtError::MissingTaskClient(DbType::Mysql))?
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
                    .ok_or(DtError::MissingTaskClient(DbType::Pg))?
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
                    .ok_or(DtError::MissingTaskClient(DbType::Mssql))?
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
        src_sql_map: &BTreeMap<StructKey, StructCheckItem>,
        namespaces: &HashSet<String>,
        log_enabled: bool,
    ) -> anyhow::Result<CheckSummaryLog> {
        let dst_map = self.build_dst_sql_map(namespaces).await?;
        Ok(Self::compare_sql_maps(
            src_sql_map,
            dst_map,
            self.router.as_ref(),
            &self.start_time,
            log_enabled,
            self.output_revise_sql,
        ))
    }

    fn compare_sql_maps(
        src_sql_map: &BTreeMap<StructKey, StructCheckItem>,
        mut dst_map: BTreeMap<StructKey, String>,
        router: Option<&RdbRouter>,
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

        for (target_key, item) in src_sql_map {
            let dst_sql = dst_map.remove(target_key);
            let is_miss = dst_sql.is_none();
            let is_diff = dst_sql.as_ref().is_some_and(|dst_sql| dst_sql != &item.sql);
            if let Some(table) =
                struct_table_summary(&item.source_key, &item.target_key, 1, is_miss, is_diff)
            {
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
                let log = StructCheckLog::new(
                    &item.source_key,
                    &item.target_key,
                    Some(item.sql.clone()),
                    dst_sql,
                );
                if let Some(log) = to_json_line(&log) {
                    if is_miss {
                        log_miss!("{}", log);
                    } else {
                        log_diff!("{}", log);
                    }
                }
                if output_revise_sql {
                    log_sql!("{}", item.sql);
                    sql_count += 1;
                }
            }
        }

        for (key, dst_sql) in dst_map {
            summary.diff_count += 1;
            let target_key = StructCheckKey::new("", key);
            let source_key = Self::source_key_from_target(&target_key, router);
            if let Some(table) = struct_table_summary(&source_key, &target_key, 0, false, true) {
                summary.merge_table(table);
            }
            if log_enabled {
                let log = StructCheckLog::new(&source_key, &target_key, None, Some(dst_sql));
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
    use dt_common::{
        config::{filter_config::FilterConfig, router_config::RouterConfig},
        meta::struct_meta::statement::{
            mssql_comment_statement::MssqlComment,
            mssql_create_database_statement::MssqlCreateDatabaseStatement,
            mssql_create_table_statement::{
                MssqlColumn, MssqlColumnDefinition, MssqlCreateTableStatement, MssqlTable,
            },
            struct_statement::StructKeyType,
        },
    };

    use super::*;

    fn mssql_router() -> RdbRouter {
        RdbRouter::from_config(
            &RouterConfig::Rdb {
                schema_map: "src_db:dst_db".to_string(),
                tb_map: "src_db.src_schema.src_tb:dst_db.dst_schema.dst_tb".to_string(),
                col_map: String::new(),
                topic_map: String::new(),
            },
            &DbType::Mssql,
        )
        .unwrap()
        .unwrap()
    }

    #[tokio::test]
    async fn mssql_routed_objects_keep_source_locations_and_reject_duplicates() {
        let filter = RdbFilter::from_config(
            &FilterConfig {
                do_structures: "table,comment".to_string(),
                ..Default::default()
            },
            &DbType::Mssql,
        )
        .unwrap();
        let mut checker = StructCheckerHandle::new(
            DbType::Mssql,
            None,
            None,
            None,
            filter,
            Some(mssql_router()),
            false,
            0,
            0,
            None,
            TaskMonitorHandle::default(),
            String::new(),
        );
        let data = StructData {
            db: "src_db".to_string(),
            schema: "src_schema".to_string(),
            tb: "src_tb".to_string(),
            statement: StructStatement::MssqlCreateTable(MssqlCreateTableStatement {
                table: MssqlTable {
                    database_name: "src_db".to_string(),
                    schema_name: "src_schema".to_string(),
                    table_name: "src_tb".to_string(),
                    is_memory_optimized: false,
                    durability_desc: String::new(),
                    columns: vec![MssqlColumn {
                        column_name: "id".to_string(),
                        ordinal_position: 1,
                        definition: MssqlColumnDefinition::Regular {
                            column_type: "INT".to_string(),
                            collation_name: String::new(),
                            is_nullable: false,
                            identity: None,
                        },
                    }],
                    constraints: Vec::new(),
                    indexes: Vec::new(),
                    comments: vec![MssqlComment::Table {
                        comment: "description".to_string(),
                    }],
                },
            }),
        };
        checker.add_src_sqls(data.clone()).await.unwrap();
        assert_eq!(checker.namespaces, HashSet::from(["dst_db".to_string()]));
        let table = StructKey::new(
            DbType::Mssql,
            StructKeyType::Table,
            ["dst_db", "dst_schema", "dst_tb"],
        );
        let item = &checker.src_sql_map[&table];
        assert!(item.sql.contains("[dst_db].[dst_schema].[dst_tb]"));
        let log = StructCheckLog::new(
            &item.source_key,
            &item.target_key,
            Some(item.sql.clone()),
            None,
        );
        assert_eq!(log.key, "table.src_db.src_schema.src_tb");
        assert_eq!(
            (log.db.as_str(), log.schema.as_str(), log.tb.as_str()),
            ("src_db", "src_schema", "src_tb")
        );
        assert_eq!(log.target_db.as_deref(), Some("dst_db"));
        assert_eq!(log.target_schema.as_deref(), Some("dst_schema"));
        assert_eq!(log.target_tb.as_deref(), Some("dst_tb"));

        let extra = StructKey::new(
            DbType::Mssql,
            StructKeyType::Index,
            ["dst_db", "dst_schema", "dst_tb", "extra.index"],
        );
        let summary = StructCheckerHandle::compare_sql_maps(
            &checker.src_sql_map,
            BTreeMap::from([
                (table, "different table".to_string()),
                (extra, "extra index".to_string()),
            ]),
            checker.router.as_ref(),
            "start",
            false,
            false,
        );
        assert_eq!(
            (
                summary.checked_count,
                summary.miss_count,
                summary.diff_count
            ),
            (2, 1, 2)
        );
        assert_eq!(summary.tables.len(), 1);
        let table = &summary.tables[0];
        assert_eq!(
            (table.db.as_str(), table.schema.as_str(), table.tb.as_str()),
            ("src_db", "src_schema", "src_tb")
        );
        assert_eq!(table.target_db.as_deref(), Some("dst_db"));
        assert_eq!(table.target_schema.as_deref(), Some("dst_schema"));
        assert_eq!(table.target_tb.as_deref(), Some("dst_tb"));
        assert_eq!(
            (table.checked_count, table.miss_count, table.diff_count),
            (2, 1, 2)
        );
        assert!(checker.add_src_sqls(data).await.is_err());
        assert_eq!(checker.src_sql_map.len(), 2);
    }

    #[test]
    fn mssql_target_only_objects_reverse_database_and_table_routes() {
        let router = mssql_router();
        for (kind, segments, source_location, summarized) in [
            (StructKeyType::Database, vec![], ("", ""), false),
            (
                StructKeyType::Schema,
                vec!["schema.with.dot"],
                ("schema.with.dot", ""),
                false,
            ),
            (
                StructKeyType::SequenceComment,
                vec!["schema.with.dot", "sequence.with.dot"],
                ("schema.with.dot", "sequence.with.dot"),
                true,
            ),
            (
                StructKeyType::IndexComment,
                vec!["dst_schema", "dst_tb", "index.with.dot"],
                ("src_schema", "src_tb"),
                true,
            ),
        ] {
            let target = StructCheckKey::new(
                "",
                StructKey::new(
                    DbType::Mssql,
                    kind,
                    std::iter::once("dst_db").chain(segments),
                ),
            );
            let source = StructCheckerHandle::source_key_from_target(&target, Some(&router));
            assert_eq!(source.db, "src_db", "{kind:?}");
            assert_eq!(source.key.database(), Some("src_db"), "{kind:?}");
            assert_eq!(
                (source.schema.as_str(), source.tb.as_str()),
                source_location,
                "{kind:?}"
            );
            let summary = struct_table_summary(&source, &target, 0, false, true);
            assert_eq!(
                summary.as_ref().map(|s| (s.schema.as_str(), s.tb.as_str())),
                summarized.then_some(source_location),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn duplicate_target_statements_are_rejected() {
        let filter = RdbFilter::from_config(
            &FilterConfig {
                do_structures: "database".to_string(),
                ..Default::default()
            },
            &DbType::Mssql,
        )
        .unwrap();
        let statement = StructStatement::MssqlCreateDatabase(MssqlCreateDatabaseStatement {
            database_name: "db.with.dot".to_string(),
            collation_name: String::new(),
            comments: Vec::new(),
        });
        let mut map = BTreeMap::new();
        StructCheckerHandle::insert_statement(&mut map, statement.clone(), &filter, "target")
            .unwrap();
        assert!(
            StructCheckerHandle::insert_statement(&mut map, statement, &filter, "target").is_err()
        );
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn missing_target_database_and_table_are_reported_as_miss() {
        let database_key = StructKey::new(DbType::Mysql, StructKeyType::Database, ["test_db"]);
        let table_key = StructKey::new(DbType::Mysql, StructKeyType::Table, ["test_db", "test_tb"]);
        let src_sql_map = BTreeMap::from([
            (
                database_key.clone(),
                StructCheckItem::unrouted(database_key, "CREATE DATABASE IF NOT EXISTS `test_db`"),
            ),
            (
                table_key.clone(),
                StructCheckItem::unrouted(
                    table_key,
                    "CREATE TABLE IF NOT EXISTS `test_db`.`test_tb` (`id` int)",
                ),
            ),
        ]);

        let summary = StructCheckerHandle::compare_sql_maps(
            &src_sql_map,
            BTreeMap::new(),
            None,
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
    fn keys_with_same_display_but_different_identifier_boundaries_match_independently() {
        let dotted_schema = StructKey::new(DbType::Pg, StructKeyType::Table, ["a.b", "c"]);
        let dotted_table = StructKey::new(DbType::Pg, StructKeyType::Table, ["a", "b.c"]);
        assert_eq!(dotted_schema.to_string(), dotted_table.to_string());
        assert_ne!(dotted_schema, dotted_table);

        let src_sql_map = BTreeMap::from([
            (
                dotted_schema.clone(),
                StructCheckItem::unrouted(dotted_schema.clone(), "CREATE TABLE dotted_schema"),
            ),
            (
                dotted_table.clone(),
                StructCheckItem::unrouted(dotted_table.clone(), "CREATE TABLE dotted_table"),
            ),
        ]);
        let dst_sql_map = BTreeMap::from([
            (dotted_schema, "CREATE TABLE dotted_schema".to_string()),
            (dotted_table, "CREATE TABLE dotted_table".to_string()),
        ]);

        let summary = StructCheckerHandle::compare_sql_maps(
            &src_sql_map,
            dst_sql_map,
            None,
            "start",
            false,
            false,
        );

        assert!(summary.is_consistent);
        assert_eq!(summary.checked_count, 2);
        assert_eq!(summary.tables.len(), 2);
    }
}
