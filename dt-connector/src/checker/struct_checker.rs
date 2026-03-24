use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use async_mutex::Mutex;
use chrono::Local;
use sqlx::{MySql, Pool, Postgres};
use tokio::time::sleep;

use dt_common::{
    config::config_enums::DbType,
    log_diff, log_info, log_miss, log_sql, log_summary,
    meta::struct_meta::struct_data::StructData,
    monitor::{
        counter_type::CounterType, monitor::Monitor, task_metrics::TaskMetricsType,
        task_monitor::TaskMonitor,
    },
    rdb_filter::RdbFilter,
};

use crate::{
    checker::check_log::{CheckSummaryLog, StructCheckLog},
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
    router: RdbRouter,
    output_revise_sql: bool,
    retry_interval_secs: u64,
    max_retries: u32,
    global_summary: Option<Arc<Mutex<CheckSummaryLog>>>,
    monitor: Arc<Monitor>,
    task_monitor: Option<Arc<TaskMonitor>>,
    src_sql_map: HashMap<String, String>,
    dbs: HashSet<String>,
    start_time: String,
}

impl StructCheckerHandle {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db_type: DbType,
        conn_pool_mysql: Option<Pool<MySql>>,
        conn_pool_pg: Option<Pool<Postgres>>,
        filter: RdbFilter,
        router: RdbRouter,
        output_revise_sql: bool,
        retry_interval_secs: u64,
        max_retries: u32,
        global_summary: Option<Arc<Mutex<CheckSummaryLog>>>,
        monitor: Arc<Monitor>,
        task_monitor: Option<Arc<TaskMonitor>>,
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
            task_monitor,
            src_sql_map: HashMap::new(),
            dbs: HashSet::new(),
            start_time: Local::now().to_rfc3339(),
        }
    }

    /// Extracts database/schema name from a key in format "type.db.table"
    ///
    /// # Examples
    /// - "table.mydb.users" -> Some("mydb")
    /// - "index.testdb.orders.idx_name" -> Some("testdb")
    fn collect_db_from_key(key: &str) -> Option<String> {
        let mut parts = key.split('.');
        parts.next()?;
        parts.next().map(|s| s.to_string())
    }

    async fn add_src_sqls(&mut self, struct_data: StructData) -> anyhow::Result<()> {
        let routed = self.router.route_struct(struct_data);
        let mut statement = routed.statement;
        let sqls = statement.to_sqls(&self.filter)?;
        if !sqls.is_empty() {
            self.monitor
                .add_counter(CounterType::RecordCount, sqls.len() as u64)
                .await;
        }

        for (key, sql) in sqls {
            if let Some(db) = Self::collect_db_from_key(&key) {
                self.dbs.insert(db);
            }
            self.src_sql_map.insert(key, sql);
        }
        Ok(())
    }

    fn collect_sqls(sqls: Vec<(String, String)>, dst_map: &mut HashMap<String, String>) {
        for (key, sql) in sqls {
            dst_map.insert(key, sql);
        }
    }

    async fn build_dst_sql_map(
        &self,
        dbs: &HashSet<String>,
    ) -> anyhow::Result<HashMap<String, String>> {
        let mut dst_map = HashMap::new();
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
                    filter: Some(self.filter.clone()),
                    meta_manager,
                };
                for stmt in fetcher.get_create_database_statements("").await? {
                    Self::collect_sqls(stmt.to_sqls(&self.filter)?, &mut dst_map);
                }
                for mut stmt in fetcher.get_create_table_statements("", "").await? {
                    Self::collect_sqls(stmt.to_sqls(&self.filter)?, &mut dst_map);
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
                    filter: Some(self.filter.clone()),
                };
                for stmt in fetcher.get_create_schema_statements("").await? {
                    Self::collect_sqls(stmt.to_sqls(&self.filter)?, &mut dst_map);
                }
                for mut stmt in fetcher.get_create_table_statements("", "").await? {
                    Self::collect_sqls(stmt.to_sqls(&self.filter)?, &mut dst_map);
                }
                for stmt in fetcher.get_create_rbac_statements().await? {
                    Self::collect_sqls(stmt.to_sqls(&self.filter)?, &mut dst_map);
                }
            }
            _ => bail!("struct check not supported for db_type: {}", self.db_type),
        }
        Ok(dst_map)
    }

    async fn compare_once(
        &self,
        src_sql_map: &HashMap<String, String>,
        dbs: &HashSet<String>,
        start_time: &str,
        log_enabled: bool,
    ) -> anyhow::Result<(CheckSummaryLog, usize)> {
        let dst_map = self.build_dst_sql_map(dbs).await?;
        Ok(Self::compare_sql_maps(
            src_sql_map,
            dst_map,
            start_time,
            log_enabled,
            self.output_revise_sql,
        ))
    }

    fn compare_sql_maps(
        src_sql_map: &HashMap<String, String>,
        mut dst_map: HashMap<String, String>,
        start_time: &str,
        log_enabled: bool,
        output_revise_sql: bool,
    ) -> (CheckSummaryLog, usize) {
        let mut summary = CheckSummaryLog {
            start_time: start_time.to_string(),
            end_time: Local::now().to_rfc3339(),
            ..Default::default()
        };
        let mut sql_count = 0usize;

        for (key, src_sql) in src_sql_map.iter() {
            match dst_map.remove(key) {
                None => {
                    let log = StructCheckLog {
                        key: key.clone(),
                        src_sql: Some(src_sql.clone()),
                        dst_sql: None,
                    };
                    summary.miss_count += 1;
                    if log_enabled {
                        log_miss!("{}", log);
                    }
                    if output_revise_sql && log_enabled {
                        log_sql!("{}", src_sql);
                        sql_count += 1;
                    }
                }
                Some(dst_sql) => {
                    if src_sql != &dst_sql {
                        let log = StructCheckLog {
                            key: key.clone(),
                            src_sql: Some(src_sql.clone()),
                            dst_sql: Some(dst_sql),
                        };
                        summary.diff_count += 1;
                        if log_enabled {
                            log_diff!("{}", log);
                        }
                        if output_revise_sql && log_enabled {
                            log_sql!("{}", src_sql);
                            sql_count += 1;
                        }
                    }
                }
            }
        }

        for (key, dst_sql) in dst_map {
            let log = StructCheckLog {
                key,
                src_sql: None,
                dst_sql: Some(dst_sql),
            };
            summary.diff_count += 1;
            if log_enabled {
                log_diff!("{}", log);
            }
        }

        summary.is_consistent = summary.miss_count == 0 && summary.diff_count == 0;
        if output_revise_sql && sql_count > 0 {
            summary.sql_count = Some(sql_count);
        }
        summary.end_time = Local::now().to_rfc3339();

        (summary, sql_count)
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
        loop {
            let (summary, _) = self
                .compare_once(&self.src_sql_map, &self.dbs, &self.start_time, false)
                .await?;
            if summary.is_consistent {
                log_info!("Structure check passed - all structures are consistent");
                return Ok(());
            }
            if retries_left == 0 {
                break;
            }
            retries_left -= 1;
            if self.retry_interval_secs > 0 {
                sleep(Duration::from_secs(self.retry_interval_secs)).await;
            }
        }

        let (summary, _sql_count) = self
            .compare_once(&self.src_sql_map, &self.dbs, &self.start_time, true)
            .await?;
        if summary.miss_count > 0 {
            if let Some(task_monitor) = &self.task_monitor {
                task_monitor.add_no_window_metrics(
                    TaskMetricsType::CheckerMissCount,
                    summary.miss_count as u64,
                );
            }
            self.monitor
                .add_counter(CounterType::CheckerMissCount, summary.miss_count as u64)
                .await;
        }
        if summary.diff_count > 0 {
            if let Some(task_monitor) = &self.task_monitor {
                task_monitor.add_no_window_metrics(
                    TaskMetricsType::CheckerDiffCount,
                    summary.diff_count as u64,
                );
            }
            self.monitor
                .add_counter(CounterType::CheckerDiffCount, summary.diff_count as u64)
                .await;
        }
        if summary.miss_count > 0 || summary.diff_count > 0 {
            if let Some(global_summary) = &self.global_summary {
                let mut global_summary = global_summary.lock().await;
                global_summary.merge(&summary);
            } else {
                log_summary!("{}", summary);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compare_sql_maps_counts_target_only_object_as_diff() {
        let src_sql_map = HashMap::new();
        let mut dst_map = HashMap::new();
        dst_map.insert(
            "table.test_db.extra_tb".to_string(),
            "CREATE TABLE extra_tb(id int)".to_string(),
        );

        let (summary, sql_count) =
            StructCheckerHandle::compare_sql_maps(&src_sql_map, dst_map, "start", false, false);
        assert_eq!(summary.miss_count, 0);
        assert_eq!(summary.diff_count, 1);
        assert!(!summary.is_consistent);
        assert_eq!(sql_count, 0);
    }

    #[test]
    fn compare_sql_maps_counts_missing_target_object_as_miss() {
        let src_sql_map = HashMap::from([(
            "table.test_db.src_tb".to_string(),
            "CREATE TABLE src_tb(id int)".to_string(),
        )]);
        let dst_map = HashMap::new();

        let (summary, sql_count) =
            StructCheckerHandle::compare_sql_maps(&src_sql_map, dst_map, "start", false, false);
        assert_eq!(summary.miss_count, 1);
        assert_eq!(summary.diff_count, 0);
        assert!(!summary.is_consistent);
        assert_eq!(sql_count, 0);
    }
}
