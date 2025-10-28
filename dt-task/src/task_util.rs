use std::{str::FromStr, sync::Arc, time::Duration};

use anyhow::bail;
use futures::TryStreamExt;
use mongodb::{bson::doc, options::ClientOptions};
use rusoto_core::Region;
use rusoto_s3::S3Client;
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
    postgres::{PgConnectOptions, PgPoolOptions},
    ConnectOptions, Executor, MySql, Pool, Postgres, Row,
};

use dt_common::{
    config::{
        config_enums::{DbType, TaskType},
        extractor_config::ExtractorConfig,
        global_config::GlobalConfig,
        meta_center_config::MetaCenterConfig,
        resumer_config::ResumerConfig,
        s3_config::S3Config,
        sinker_config::SinkerConfig,
        task_config::TaskConfig,
    },
    error::Error,
    log_info,
    meta::{
        mysql::{
            mysql_dbengine_meta_center::MysqlDbEngineMetaCenter,
            mysql_meta_manager::MysqlMetaManager,
        },
        pg::pg_meta_manager::PgMetaManager,
        rdb_meta_manager::RdbMetaManager,
    },
    rdb_filter::RdbFilter,
};
use dt_connector::extractor::resumer::{
    build_recorder, build_recovery, recorder::Recorder, recovery::Recovery, utils::ResumerUtil,
};

const MYSQL_SYS_DBS: [&str; 4] = ["information_schema", "mysql", "performance_schema", "sys"];
const PG_SYS_SCHEMAS: [&str; 2] = ["pg_catalog", "information_schema"];

pub struct TaskUtil {}

impl TaskUtil {
    pub async fn create_mysql_conn_pool(
        url: &str,
        max_connections: u32,
        enable_sqlx_log: bool,
        disable_foreign_key_checks: bool,
    ) -> anyhow::Result<Pool<MySql>> {
        let mut conn_options = MySqlConnectOptions::from_str(url)?;
        // The default character set is `utf8mb4`
        conn_options
            .log_statements(log::LevelFilter::Debug)
            .log_slow_statements(log::LevelFilter::Debug, Duration::from_secs(1));

        if !enable_sqlx_log {
            conn_options.disable_statement_logging();
        }

        let conn_pool = MySqlPoolOptions::new()
            .max_connections(max_connections)
            .after_connect(move |conn, _meta| {
                Box::pin(async move {
                    if disable_foreign_key_checks {
                        conn.execute(sqlx::query("SET foreign_key_checks = 0;"))
                            .await?;
                    }
                    Ok(())
                })
            })
            .connect_with(conn_options)
            .await?;
        Ok(conn_pool)
    }

    pub async fn create_pg_conn_pool(
        url: &str,
        max_connections: u32,
        enable_sqlx_log: bool,
        disable_foreign_key_checks: bool,
    ) -> anyhow::Result<Pool<Postgres>> {
        let mut conn_options = PgConnectOptions::from_str(url)?;
        conn_options
            .log_statements(log::LevelFilter::Debug)
            .log_slow_statements(log::LevelFilter::Debug, Duration::from_secs(1));

        if !enable_sqlx_log {
            conn_options.disable_statement_logging();
        }

        let conn_pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .after_connect(move |conn, _meta| {
                Box::pin(async move {
                    if disable_foreign_key_checks {
                        // disable foreign key checks
                        conn.execute("SET session_replication_role = 'replica';")
                            .await?;
                    }
                    Ok(())
                })
            })
            .connect_with(conn_options)
            .await?;
        Ok(conn_pool)
    }

    pub async fn create_rdb_meta_manager(
        config: &TaskConfig,
    ) -> anyhow::Result<Option<RdbMetaManager>> {
        let log_level = &config.runtime.log_level;
        let meta_manager = match &config.sinker {
            SinkerConfig::Mysql { url, .. } | SinkerConfig::MysqlCheck { url, .. } => {
                let mysql_meta_manager =
                    Self::create_mysql_meta_manager(url, log_level, DbType::Mysql, None, None)
                        .await?;
                RdbMetaManager::from_mysql(mysql_meta_manager)
            }

            // In Doris/Starrocks, you can NOT get UNIQUE KEY by "SHOW INDEXES" or from "information_schema.STATISTICS",
            // as a workaround, for MySQL/Postgres -> Doris/Starrocks, we use extractor meta manager instead.
            SinkerConfig::StarRocks { .. } | SinkerConfig::Doris { .. } => {
                match &config.extractor {
                    ExtractorConfig::MysqlCdc { url, .. } => {
                        let mysql_meta_manager = Self::create_mysql_meta_manager(
                            url,
                            log_level,
                            DbType::Mysql,
                            None,
                            None,
                        )
                        .await?;
                        RdbMetaManager::from_mysql(mysql_meta_manager)
                    }
                    ExtractorConfig::PgCdc { url, .. } => {
                        let pg_meta_manager = Self::create_pg_meta_manager(url, log_level).await?;
                        RdbMetaManager::from_pg(pg_meta_manager)
                    }
                    _ => return Ok(None),
                }
            }

            SinkerConfig::Pg { url, .. } | SinkerConfig::PgCheck { url, .. } => {
                let pg_meta_manager = Self::create_pg_meta_manager(url, log_level).await?;
                RdbMetaManager::from_pg(pg_meta_manager)
            }

            _ => {
                return Ok(None);
            }
        };
        Ok(Some(meta_manager))
    }

    pub async fn create_mysql_meta_manager(
        url: &str,
        log_level: &str,
        db_type: DbType,
        meta_center_config: Option<MetaCenterConfig>,
        conn_pool_opt: Option<Pool<MySql>>,
    ) -> anyhow::Result<MysqlMetaManager> {
        let enable_sqlx_log = Self::check_enable_sqlx_log(log_level);
        let conn_pool = match &conn_pool_opt {
            Some(conn_pool) => conn_pool.clone(),
            None => Self::create_mysql_conn_pool(url, 1, enable_sqlx_log, false).await?,
        };
        let mut meta_manager = MysqlMetaManager::new_mysql_compatible(conn_pool, db_type).await?;

        if let Some(MetaCenterConfig::MySqlDbEngine {
            url,
            ddl_conflict_policy,
            ..
        }) = &meta_center_config
        {
            let meta_center_conn_pool = match &conn_pool_opt {
                Some(conn_pool) => conn_pool.clone(),
                None => Self::create_mysql_conn_pool(url, 1, enable_sqlx_log, false).await?,
            };
            let meta_center = MysqlDbEngineMetaCenter::new(
                url.clone(),
                meta_center_conn_pool,
                ddl_conflict_policy.clone(),
            )
            .await?;
            meta_manager.meta_center = Some(meta_center);
        }
        Ok(meta_manager)
    }

    pub async fn create_pg_meta_manager(
        url: &str,
        log_level: &str,
    ) -> anyhow::Result<PgMetaManager> {
        let enable_sqlx_log = Self::check_enable_sqlx_log(log_level);
        let conn_pool = Self::create_pg_conn_pool(url, 1, enable_sqlx_log, false).await?;
        PgMetaManager::new(conn_pool.clone()).await
    }

    pub async fn create_mongo_client(
        url: &str,
        app_name: &str,
        max_pool_size: Option<u32>,
    ) -> anyhow::Result<mongodb::Client> {
        let mut client_options = ClientOptions::parse_async(url).await?;
        // app_name only for debug usage
        client_options.app_name = Some(app_name.to_string());
        client_options.direct_connection = Some(true);
        client_options.max_pool_size = max_pool_size;
        Ok(mongodb::Client::with_options(client_options)?)
    }

    pub fn check_enable_sqlx_log(log_level: &str) -> bool {
        log_level == "debug" || log_level == "trace"
    }

    pub async fn list_schemas(
        conn_pool: &ConnClient,
        db_type: &DbType,
    ) -> anyhow::Result<Vec<String>> {
        let mut dbs = match db_type {
            DbType::Mysql => Self::list_mysql_dbs(conn_pool).await?,
            DbType::Pg => Self::list_pg_schemas(conn_pool).await?,
            DbType::Mongo => Self::list_mongo_dbs(conn_pool).await?,
            _ => Vec::new(),
        };
        dbs.sort();
        Ok(dbs)
    }

    pub async fn list_tbs(
        conn_client: &ConnClient,
        schema: &str,
        db_type: &DbType,
    ) -> anyhow::Result<Vec<String>> {
        let mut tbs = match db_type {
            DbType::Mysql => Self::list_mysql_tbs(conn_client, schema).await?,
            DbType::Pg => Self::list_pg_tbs(conn_client, schema).await?,
            DbType::Mongo => Self::list_mongo_tbs(conn_client, schema).await?,
            _ => Vec::new(),
        };
        tbs.sort();
        Ok(tbs)
    }

    pub async fn estimate_record_count(
        task_type: &TaskType,
        conn_pool: &ConnClient,
        db_type: &DbType,
        schemas: &[String],
        filter: &RdbFilter,
    ) -> anyhow::Result<u64> {
        match task_type {
            TaskType::Snapshot => match db_type {
                DbType::Mysql => Self::estimate_mysql_snapshot(conn_pool, schemas, filter).await,
                DbType::Pg => Self::estimate_pg_snapshot(conn_pool, schemas, filter).await,
                _ => Ok(0),
            },
            _ => Ok(0),
        }
    }

    async fn estimate_mysql_snapshot(
        conn_pool: &ConnClient,
        schemas: &[String],
        filter: &RdbFilter,
    ) -> anyhow::Result<u64> {
        let conn_pool = match conn_pool {
            ConnClient::MySQL(conn_pool) => conn_pool,
            _ => {
                bail!("conn_pool is not found")
            }
        };

        let mut sql = String::from("select table_schema, table_name, TABLE_ROWS from information_schema.TABLES where table_type = 'BASE TABLE'");
        if schemas.len() <= 100 {
            let sql_with_filter = format!(
                "{} and table_schema in ({})",
                sql,
                schemas
                    .iter()
                    .filter(|s| !MYSQL_SYS_DBS.contains(&s.as_str()))
                    .map(|s| format!("'{}'", s))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            sql = sql_with_filter;
        }

        let mut total_records = 0;
        let mut rows = sqlx::query(&sql).fetch(conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let schema: String = row.try_get(0)?;
            let tb: String = row.try_get(1)?;
            let records: u64 = row.try_get(2)?;
            if filter.filter_tb(&schema, &tb) {
                continue;
            }
            total_records += records;
        }

        Ok(total_records)
    }

    async fn estimate_pg_snapshot(
        conn_pool: &ConnClient,
        schemas: &[String],
        filter: &RdbFilter,
    ) -> anyhow::Result<u64> {
        let conn_pool = match conn_pool {
            ConnClient::PostgreSQL(conn_pool) => conn_pool,
            _ => {
                bail!("conn_pool is not found")
            }
        };

        let mut sql = String::from(
            "SELECT
    n.nspname AS schemaname,
    c.relname AS tablename,
    c.reltuples::bigint AS row_count
FROM
    pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE
    c.relkind = 'r'
    AND n.nspname NOT IN ('information_schema', 'pg_catalog')",
        );

        if schemas.len() <= 100 {
            let sql_with_filter = format!(
                "{} AND n.nspname IN ({})",
                sql,
                schemas
                    .iter()
                    .filter(|s| !PG_SYS_SCHEMAS.contains(&s.as_str()))
                    .map(|s| format!("'{}'", s))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            sql = sql_with_filter;
        }

        let mut total_length = 0;
        let mut rows = sqlx::query(&sql).fetch(conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let schema: String = row.try_get(0)?;
            let table_name: String = row.try_get(1)?;
            let row_count: i64 = row.try_get(2)?;
            if filter.filter_tb(&schema, &table_name) {
                continue;
            }
            // Convert to u64, handling negative values (which shouldn't happen but just in case)
            let row_count_u64 = if row_count < 0 { 0 } else { row_count as u64 };
            total_length += row_count_u64;
        }

        Ok(total_length)
    }

    pub async fn check_tb_exist(
        conn_client: &ConnClient,
        schema: &str,
        tb: &str,
        db_type: &DbType,
    ) -> anyhow::Result<bool> {
        let schemas = Self::list_schemas(conn_client, db_type).await?;
        if !schemas.contains(&schema.to_string()) {
            return Ok(false);
        }

        let tbs = Self::list_tbs(conn_client, schema, db_type).await?;
        Ok(tbs.contains(&tb.to_string()))
    }

    pub async fn check_and_create_tb(
        conn_client: &ConnClient,
        schema: &str,
        tb: &str,
        schema_sql: &str,
        tb_sql: &str,
        db_type: &DbType,
    ) -> anyhow::Result<()> {
        log_info!(
            "schema: {}, tb: {}, schema_sql: {}, tb_sql: {}",
            schema,
            tb,
            schema_sql,
            tb_sql
        );
        if TaskUtil::check_tb_exist(conn_client, schema, tb, db_type).await? {
            return Ok(());
        }

        match conn_client {
            ConnClient::MySQL(conn_pool) => {
                sqlx::query(schema_sql).execute(conn_pool).await?;
                sqlx::query(tb_sql).execute(conn_pool).await?;
            }
            ConnClient::PostgreSQL(conn_pool) => {
                sqlx::query(schema_sql).execute(conn_pool).await?;
                sqlx::query(tb_sql).execute(conn_pool).await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn list_pg_schemas(conn_client: &ConnClient) -> anyhow::Result<Vec<String>> {
        let mut schemas = Vec::new();
        let conn_pool = match conn_client {
            ConnClient::PostgreSQL(conn_pool) => conn_pool,
            _ => {
                bail!("conn_pool is not found")
            }
        };

        let sql = "SELECT schema_name
            FROM information_schema.schemata
            WHERE catalog_name = current_database()";
        let mut rows = sqlx::query(sql).fetch(conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let schema: String = row.try_get(0)?;
            if PG_SYS_SCHEMAS.contains(&schema.as_str()) {
                continue;
            }
            schemas.push(schema);
        }

        Ok(schemas)
    }

    async fn list_pg_tbs(conn_client: &ConnClient, schema: &str) -> anyhow::Result<Vec<String>> {
        let mut tbs = Vec::new();
        let conn_pool = match conn_client {
            ConnClient::PostgreSQL(conn_pool) => conn_pool,
            _ => {
                bail!("conn_pool is not found")
            }
        };

        let sql = format!(
            "SELECT table_name 
            FROM information_schema.tables
            WHERE table_catalog = current_database() 
            AND table_schema = '{}' 
            AND table_type = 'BASE TABLE'",
            schema
        );
        let mut rows = sqlx::query(&sql).fetch(conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let tb: String = row.try_get(0)?;
            tbs.push(tb);
        }

        Ok(tbs)
    }

    async fn list_mysql_dbs(conn_client: &ConnClient) -> anyhow::Result<Vec<String>> {
        let mut dbs = Vec::new();
        let conn_pool = match conn_client {
            ConnClient::MySQL(conn_pool) => conn_pool,
            _ => {
                bail!("conn_pool is not found")
            }
        };

        let sql = "SHOW DATABASES";
        let mut rows = sqlx::query(sql).fetch(conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let db: String = row.try_get(0)?;
            if MYSQL_SYS_DBS.contains(&db.as_str()) {
                continue;
            }
            dbs.push(db);
        }

        Ok(dbs)
    }

    async fn list_mysql_tbs(conn_client: &ConnClient, db: &str) -> anyhow::Result<Vec<String>> {
        let mut tbs = Vec::new();
        let conn_pool = match conn_client {
            ConnClient::MySQL(conn_pool) => conn_pool,
            _ => {
                bail!("conn_pool is not found")
            }
        };

        let sql = format!("SHOW FULL TABLES IN `{}`", db);
        let mut rows = sqlx::query(&sql).fetch(conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let tb: String = row.try_get(0)?;
            let tb_type: String = row.try_get(1)?;
            if tb_type == "BASE TABLE" {
                tbs.push(tb);
            }
        }

        Ok(tbs)
    }

    async fn list_mongo_dbs(conn_client: &ConnClient) -> anyhow::Result<Vec<String>> {
        let client = match conn_client {
            ConnClient::MongoDB(client) => client,
            _ => {
                bail!("client is not found")
            }
        };
        let dbs = client.list_database_names(None, None).await?;
        Ok(dbs)
    }

    async fn list_mongo_tbs(conn_client: &ConnClient, db: &str) -> anyhow::Result<Vec<String>> {
        let client = match conn_client {
            ConnClient::MongoDB(client) => client,
            _ => {
                bail!("client is not found")
            }
        };
        // filter views and system tables
        let tbs = client
            .database(db)
            .list_collection_names(Some(doc! { "type": "collection" }))
            .await?
            .into_iter()
            .filter(|name| !name.starts_with("system."))
            .collect();
        Ok(tbs)
    }

    pub fn create_s3_client(s3_config: &S3Config) -> S3Client {
        let region = if s3_config.endpoint.is_empty() {
            Region::from_str(&s3_config.region).unwrap()
        } else {
            Region::Custom {
                name: s3_config.region.clone(),
                endpoint: s3_config.endpoint.clone(),
            }
        };

        let credentials = rusoto_credential::StaticProvider::new_minimal(
            s3_config.access_key.to_owned(),
            s3_config.secret_key.to_owned(),
        );

        S3Client::new_with(rusoto_core::HttpClient::new().unwrap(), credentials, region)
    }

    pub async fn build_resumer(
        task_type: TaskType,
        global_config: &GlobalConfig,
        resumer_config: &ResumerConfig,
    ) -> anyhow::Result<(
        Option<Arc<dyn Recorder + Send + Sync>>,
        Option<Arc<dyn Recovery + Send + Sync>>,
    )> {
        let recorder_pool = match resumer_config {
            ResumerConfig::FromDB {
                url,
                db_type,
                max_connections,
                ..
            } => {
                let pool = ResumerUtil::create_pool(url, db_type, *max_connections as u32).await?;
                Some(pool)
            }
            _ => None,
        };
        let recovery_pool = recorder_pool.clone();
        let recorder =
            build_recorder(&global_config.task_id, resumer_config, recorder_pool).await?;
        let recovery = build_recovery(
            &global_config.task_id,
            task_type,
            resumer_config,
            recovery_pool,
        )
        .await?;
        Ok((recorder, recovery))
    }
}

#[derive(Default, Clone)]
pub enum ConnClient {
    MySQL(Pool<MySql>),
    PostgreSQL(Pool<Postgres>),
    MongoDB(mongodb::Client),
    S3(S3Client),
    #[default]
    None,
}

impl ConnClient {
    pub async fn from_config(task_config: &TaskConfig) -> anyhow::Result<(Self, Self)> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(&task_config.runtime.log_level);
        let extractor_max_connections = task_config.extractor_basic.max_connections;
        let sinker_max_connections = task_config.sinker_basic.max_connections;
        if extractor_max_connections < 1 {
            bail!(Error::ConfigError(
                "`extractor.max_connections` must be greater than 0".into()
            ));
        }
        if sinker_max_connections < 1 {
            bail!(Error::ConfigError(
                "`sinker.max_connections` must be greater than 0".into()
            ));
        }

        let extractor_client = match &task_config.extractor {
            ExtractorConfig::MysqlSnapshot { url, .. }
            | ExtractorConfig::MysqlStruct { url, .. }
            | ExtractorConfig::MysqlCheck { url, .. } => ConnClient::MySQL(
                TaskUtil::create_mysql_conn_pool(
                    url,
                    extractor_max_connections,
                    enable_sqlx_log,
                    false,
                )
                .await?,
            ),
            ExtractorConfig::PgSnapshot { url, .. }
            | ExtractorConfig::PgStruct { url, .. }
            | ExtractorConfig::PgCheck { url, .. } => ConnClient::PostgreSQL(
                TaskUtil::create_pg_conn_pool(
                    url,
                    extractor_max_connections,
                    enable_sqlx_log,
                    false,
                )
                .await?,
            ),
            ExtractorConfig::MongoSnapshot { url, app_name, .. } => ConnClient::MongoDB(
                TaskUtil::create_mongo_client(url, app_name, Some(extractor_max_connections))
                    .await?,
            ),
            _ => ConnClient::None,
        };
        let sinker_client = match &task_config.sinker {
            SinkerConfig::Mysql { url, .. } => ConnClient::MySQL(
                TaskUtil::create_mysql_conn_pool(
                    url,
                    sinker_max_connections,
                    enable_sqlx_log,
                    false,
                )
                .await?,
            ),
            SinkerConfig::Pg { url, .. } => ConnClient::PostgreSQL(
                TaskUtil::create_pg_conn_pool(url, sinker_max_connections, enable_sqlx_log, false)
                    .await?,
            ),
            SinkerConfig::Mongo { url, app_name, .. } => ConnClient::MongoDB(
                TaskUtil::create_mongo_client(url, app_name, Some(sinker_max_connections)).await?,
            ),
            _ => ConnClient::None,
        };
        Ok((extractor_client, sinker_client))
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        match self {
            ConnClient::MySQL(pool) => {
                if !pool.is_closed() {
                    pool.close().await;
                }
            }
            ConnClient::PostgreSQL(pool) => {
                if !pool.is_closed() {
                    pool.close().await;
                }
            }
            ConnClient::MongoDB(client) => {
                client.clone().shutdown().await;
            }
            _ => {}
        }
        Ok(())
    }
}
