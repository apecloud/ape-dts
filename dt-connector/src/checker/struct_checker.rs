use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{bail, ensure, Context};
use async_mutex::Mutex;
use chrono::Local;
use sqlx::{MySql, Pool, Postgres};
use tokio::time::sleep;

use dt_common::{
    config::config_enums::DbType,
    log_diff, log_info, log_miss, log_sql, log_summary,
    meta::struct_meta::{
        statement::struct_statement::StructKey, struct_data::StructData,
        structure::structure_type::StructureType,
    },
    monitor::{
        counter_type::CounterType, task_metrics::TaskMetricsType,
        task_monitor_handle::TaskMonitorHandle,
    },
    rdb_filter::RdbFilter,
};

use crate::{
    checker::check_log::{
        to_json_line, CheckSummaryLog, CheckTableSummaryLog, StructCheckKey, StructCheckLog,
    },
    meta_fetcher::{
        mysql::mysql_struct_fetcher::MysqlStructFetcher, pg::pg_struct_fetcher::PgStructFetcher,
    },
    rdb_router::RdbRouter,
};

pub struct StructCheckerHandle {
    db_type: DbType,
    conn_pool_mysql: Option<Pool<MySql>>,
    conn_pool_pg: Option<Pool<Postgres>>,
    filter: RdbFilter,
    router: Option<RdbRouter>,
    output_revise_sql: bool,
    retry_interval_secs: u64,
    max_retries: u32,
    global_summary: Option<Arc<Mutex<CheckSummaryLog>>>,
    monitor: TaskMonitorHandle,
    monitor_task_id: String,
    src_sql_map: BTreeMap<StructKey, StructCheckItem>,
    schemas: HashSet<String>,
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
        let key = StructCheckKey::new(key);
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

    let target_changed = key.schema != target_key.schema || key.tb != target_key.tb;

    Some(CheckTableSummaryLog {
        schema: key.schema.clone(),
        tb: key.tb.clone(),
        target_schema: target_changed.then(|| target_key.schema.clone()),
        target_tb: target_changed.then(|| target_key.tb.clone()),
        checked_count,
        miss_count: usize::from(miss),
        diff_count: usize::from(diff),
        ..Default::default()
    })
}

fn source_contains_parent_table(
    src_sql_map: &BTreeMap<StructKey, StructCheckItem>,
    key: &StructKey,
) -> bool {
    key.parent_table_key()
        .is_some_and(|parent_key| src_sql_map.contains_key(&parent_key))
}

impl StructCheckerHandle {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db_type: DbType,
        conn_pool_mysql: Option<Pool<MySql>>,
        conn_pool_pg: Option<Pool<Postgres>>,
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
            filter,
            router,
            output_revise_sql,
            retry_interval_secs,
            max_retries,
            global_summary,
            monitor,
            monitor_task_id,
            src_sql_map: BTreeMap::new(),
            schemas: HashSet::new(),
            start_time: Local::now().to_rfc3339(),
        }
    }

    async fn add_src_sqls(&mut self, struct_data: StructData) -> anyhow::Result<()> {
        let mut source_statement = struct_data.statement.clone();
        let source_sqls = source_statement.to_sqls(&self.filter)?;
        let routed = if let Some(router) = &self.router {
            router.route_struct(struct_data)
        } else {
            struct_data
        };
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
            let source_key = StructCheckKey::new(source_key);
            let target_check_key = StructCheckKey::new(target_key.clone());
            if !target_check_key.schema.is_empty() {
                self.schemas.insert(target_check_key.schema.clone());
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
            let (schema, tb) = router.reverse_get_tb_map(&target_key.schema, &target_key.tb);
            return target_key.with_location(schema, tb);
        }
        if !target_key.schema.is_empty() {
            let schema = router.reverse_get_schema_map(&target_key.schema);
            return target_key.with_location(schema, &target_key.tb);
        }
        target_key.clone()
    }

    async fn build_dst_sql_map(
        &self,
        schemas: &HashSet<String>,
    ) -> anyhow::Result<BTreeMap<StructKey, String>> {
        let mut dst_map = BTreeMap::new();
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
                    dbs: schemas.clone(),
                    filter: Some(self.filter.clone()),
                    meta_manager,
                    allow_missing_databases: true,
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
                    schemas: schemas.clone(),
                    filter: Some(self.filter.clone()),
                    allow_missing_schemas: true,
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
            _ => bail!("struct check not supported for db_type: {}", self.db_type),
        }
        Ok(dst_map)
    }

    async fn compare_once(
        &self,
        src_sql_map: &BTreeMap<StructKey, StructCheckItem>,
        schemas: &HashSet<String>,
        log_enabled: bool,
    ) -> anyhow::Result<CheckSummaryLog> {
        let dst_map = self.build_dst_sql_map(schemas).await?;
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
            if !source_contains_parent_table(src_sql_map, &key) {
                continue;
            }

            summary.diff_count += 1;
            let target_key = StructCheckKey::new(key);
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
                .compare_once(&self.src_sql_map, &self.schemas, false)
                .await?;
            if summary.is_consistent {
                log_info!("Structure check passed - all structures are consistent");
                break summary;
            }
            if retries_left == 0 {
                break self
                    .compare_once(&self.src_sql_map, &self.schemas, true)
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
    use dt_common::meta::struct_meta::statement::struct_statement::StructKeyType;

    use super::*;

    #[test]
    fn missing_target_database_and_table_are_reported_as_miss() {
        let database_key = StructKey::new(StructKeyType::Database, ["test_db"]);
        let table_key = StructKey::new(StructKeyType::Table, ["test_db", "test_tb"]);
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
        let dotted_schema = StructKey::new(StructKeyType::Table, ["a.b", "c"]);
        let dotted_table = StructKey::new(StructKeyType::Table, ["a", "b.c"]);
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

    #[test]
    fn target_only_independent_structures_are_ignored() {
        let source_role = StructKey::new(StructKeyType::RbacRole, ["app_user"]);
        let source_privilege = StructKey::new(
            StructKeyType::RbacPrivilegeSchema,
            ["app", "app_user", "NO"],
        );
        let src_sql_map = BTreeMap::from([
            (
                source_role.clone(),
                StructCheckItem::unrouted(source_role.clone(), "CREATE ROLE app_user"),
            ),
            (
                source_privilege.clone(),
                StructCheckItem::unrouted(
                    source_privilege.clone(),
                    "GRANT USAGE ON SCHEMA app TO app_user",
                ),
            ),
        ]);
        let dst_sql_map = BTreeMap::from([
            (source_role, "CREATE ROLE app_user".to_string()),
            (
                source_privilege,
                "GRANT USAGE ON SCHEMA app TO app_user".to_string(),
            ),
            (
                StructKey::new(StructKeyType::RbacRole, ["target_only"]),
                "CREATE ROLE target_only".to_string(),
            ),
            (
                StructKey::new(
                    StructKeyType::RbacPrivilegeSchema,
                    ["app", "target_only", "NO"],
                ),
                "GRANT USAGE ON SCHEMA app TO target_only".to_string(),
            ),
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
        assert_eq!(summary.miss_count, 0);
        assert_eq!(summary.diff_count, 0);
    }

    #[test]
    fn target_only_children_of_source_table_are_differences() {
        let source_table = StructKey::new(StructKeyType::Table, ["app", "users"]);
        let src_sql_map = BTreeMap::from([(
            source_table.clone(),
            StructCheckItem::unrouted(source_table.clone(), "CREATE TABLE app.users (id bigint)"),
        )]);
        let dst_sql_map = BTreeMap::from([
            (
                source_table,
                "CREATE TABLE app.users (id bigint, col1 text)".to_string(),
            ),
            (
                StructKey::new(StructKeyType::Index, ["app", "users", "target_only_idx"]),
                "CREATE INDEX target_only_idx ON app.users (id)".to_string(),
            ),
            (
                StructKey::new(StructKeyType::Table, ["app", "target_only_table"]),
                "CREATE TABLE app.target_only_table (id bigint)".to_string(),
            ),
        ]);

        let summary = StructCheckerHandle::compare_sql_maps(
            &src_sql_map,
            dst_sql_map,
            None,
            "start",
            false,
            false,
        );

        assert!(!summary.is_consistent);
        assert_eq!(summary.checked_count, 1);
        assert_eq!(summary.miss_count, 0);
        assert_eq!(summary.diff_count, 2);
        assert_eq!(summary.tables.len(), 1);
        assert_eq!(summary.tables[0].schema, "app");
        assert_eq!(summary.tables[0].tb, "users");
        assert_eq!(summary.tables[0].diff_count, 2);
    }
}
