use std::collections::{HashMap, HashSet};

use anyhow::{bail, Context};
use async_trait::async_trait;
use chrono::Local;
use sqlx::{MySql, Pool, Postgres};

use dt_common::{
    config::config_enums::DbType,
    log_diff, log_extra, log_miss, log_sql, log_summary,
    meta::struct_meta::struct_data::StructData,
    rdb_filter::RdbFilter,
};

use crate::{
    checker::check_log::{CheckSummaryLog, StructCheckLog},
    meta_fetcher::{
        mysql::mysql_struct_fetcher::MysqlStructFetcher, pg::pg_struct_fetcher::PgStructFetcher,
    },
    rdb_router::RdbRouter,
    sinker::base_struct_sinker::DBConnPool,
    Sinker,
};

pub struct StructCheckSinker {
    db_type: DbType,
    conn_pool_mysql: Option<Pool<MySql>>,
    conn_pool_pg: Option<Pool<Postgres>>,
    filter: RdbFilter,
    router: RdbRouter,
    output_revise_sql: bool,
    src_sql_map: HashMap<String, String>,
    dbs: HashSet<String>,
    start_time: String,
}

impl StructCheckSinker {
    pub fn new(
        db_type: DbType,
        conn_pool: DBConnPool,
        filter: RdbFilter,
        router: RdbRouter,
        output_revise_sql: bool,
    ) -> Self {
        let (conn_pool_mysql, conn_pool_pg) = match conn_pool {
            DBConnPool::MySQL(pool) => (Some(pool), None),
            DBConnPool::PostgreSQL(pool) => (None, Some(pool)),
        };
        Self {
            db_type,
            conn_pool_mysql,
            conn_pool_pg,
            filter,
            router,
            output_revise_sql,
            src_sql_map: HashMap::new(),
            dbs: HashSet::new(),
            start_time: Local::now().to_rfc3339(),
        }
    }

    fn collect_db_from_key(key: &str) -> Option<String> {
        let mut parts = key.split('.');
        parts.next()?;
        parts.next().map(|s| s.to_string())
    }

    fn add_src_sqls(&mut self, struct_data: StructData) -> anyhow::Result<()> {
        let routed = self.router.route_struct(struct_data);
        let mut statement = routed.statement;
        for (key, sql) in statement.to_sqls(&self.filter)? {
            if let Some(db) = Self::collect_db_from_key(&key) {
                self.dbs.insert(db);
            }
            self.src_sql_map.insert(key, sql);
        }
        Ok(())
    }

    async fn build_dst_sql_map(&self) -> anyhow::Result<HashMap<String, String>> {
        let mut dst_map = HashMap::new();
        match self.db_type {
            DbType::Mysql => {
                let conn_pool = self
                    .conn_pool_mysql
                    .as_ref()
                    .context("mysql connection pool not found")?
                    .clone();
                let meta_manager = dt_common::meta::mysql::mysql_meta_manager::MysqlMetaManager::new(
                    conn_pool.clone(),
                )
                .await?;
                let mut fetcher = MysqlStructFetcher {
                    conn_pool,
                    dbs: self.dbs.clone(),
                    filter: Some(self.filter.clone()),
                    meta_manager,
                };
                for stmt in fetcher.get_create_database_statements("").await? {
                    for (key, sql) in stmt.to_sqls(&self.filter)? {
                        dst_map.insert(key, sql);
                    }
                }
                for mut stmt in fetcher.get_create_table_statements("", "").await? {
                    for (key, sql) in stmt.to_sqls(&self.filter)? {
                        dst_map.insert(key, sql);
                    }
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
                    schemas: self.dbs.clone(),
                    filter: Some(self.filter.clone()),
                };
                for stmt in fetcher.get_create_schema_statements("").await? {
                    for (key, sql) in stmt.to_sqls(&self.filter)? {
                        dst_map.insert(key, sql);
                    }
                }
                for mut stmt in fetcher.get_create_table_statements("", "").await? {
                    for (key, sql) in stmt.to_sqls(&self.filter)? {
                        dst_map.insert(key, sql);
                    }
                }
                for stmt in fetcher.get_create_rbac_statements().await? {
                    for (key, sql) in stmt.to_sqls(&self.filter)? {
                        dst_map.insert(key, sql);
                    }
                }
            }
            _ => bail!("struct check not supported for db_type: {}", self.db_type),
        }
        Ok(dst_map)
    }
}

#[async_trait]
impl Sinker for StructCheckSinker {
    async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        for struct_data in data {
            self.add_src_sqls(struct_data)?;
        }
        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        let mut dst_map = self.build_dst_sql_map().await?;
        let mut summary = CheckSummaryLog {
            start_time: self.start_time.clone(),
            end_time: Local::now().to_rfc3339(),
            ..Default::default()
        };

        let mut sql_count = 0usize;
        for (key, src_sql) in self.src_sql_map.iter() {
            match dst_map.remove(key) {
                None => {
                    let log = StructCheckLog {
                        key: key.clone(),
                        src_sql: Some(src_sql.clone()),
                        dst_sql: None,
                    };
                    log_miss!("{}", log);
                    summary.miss_count += 1;
                    if self.output_revise_sql {
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
                        log_diff!("{}", log);
                        summary.diff_count += 1;
                        if self.output_revise_sql {
                            log_sql!("{}", src_sql);
                            sql_count += 1;
                        }
                    }
                }
            }
        }

        for (key, dst_sql) in dst_map.into_iter() {
            let log = StructCheckLog {
                key,
                src_sql: None,
                dst_sql: Some(dst_sql),
            };
            log_extra!("{}", log);
            summary.extra_count += 1;
        }

        summary.is_consistent =
            summary.miss_count == 0 && summary.diff_count == 0 && summary.extra_count == 0;
        if self.output_revise_sql && sql_count > 0 {
            summary.sql_count = Some(sql_count);
        }
        summary.end_time = Local::now().to_rfc3339();
        log_summary!("{}", summary);
        Ok(())
    }
}
