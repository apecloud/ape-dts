use std::{str::FromStr, time::Duration};

use dt_common::config::extractor_config::ExtractorConfig;
use dt_common::config::s3_config::S3Config;
use dt_common::config::{
    config_enums::DbType, meta_center_config::MetaCenterConfig, sinker_config::SinkerConfig,
    task_config::TaskConfig,
};
use dt_common::log_info;
use dt_common::meta::mysql::mysql_dbengine_meta_center::MysqlDbEngineMetaCenter;
use dt_common::meta::{
    mysql::mysql_meta_manager::MysqlMetaManager, pg::pg_meta_manager::PgMetaManager,
    rdb_meta_manager::RdbMetaManager,
};
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::options::ClientOptions;
use rusoto_core::Region;
use rusoto_s3::S3Client;
use sqlx::Executor;
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
    postgres::{PgConnectOptions, PgPoolOptions},
    ConnectOptions, MySql, Pool, Postgres, Row,
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
        let mut conn_options = MySqlConnectOptions::from_str(url)?
            .log_statements(log::LevelFilter::Debug)
            .log_slow_statements(log::LevelFilter::Debug, Duration::from_secs(1));

        if !enable_sqlx_log {
            conn_options = conn_options.disable_statement_logging();
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
        let mut conn_options = PgConnectOptions::from_str(url)?
            .log_statements(log::LevelFilter::Debug)
            .log_slow_statements(log::LevelFilter::Debug, Duration::from_secs(1));

        if !enable_sqlx_log {
            conn_options = conn_options.disable_statement_logging();
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
                    Self::create_mysql_meta_manager(url, log_level, DbType::Mysql, None).await?;
                RdbMetaManager::from_mysql(mysql_meta_manager)
            }

            // In Doris/Starrocks, you can NOT get UNIQUE KEY by "SHOW INDEXES" or from "information_schema.STATISTICS",
            // as a workaround, for MySQL/Postgres -> Doris/Starrocks, we use extractor meta manager instead.
            SinkerConfig::StarRocks { .. } | SinkerConfig::Doris { .. } => {
                match &config.extractor {
                    ExtractorConfig::MysqlCdc { url, .. } => {
                        let mysql_meta_manager =
                            Self::create_mysql_meta_manager(url, log_level, DbType::Mysql, None)
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
    ) -> anyhow::Result<MysqlMetaManager> {
        let enable_sqlx_log = Self::check_enable_sqlx_log(log_level);
        let conn_pool = Self::create_mysql_conn_pool(url, 1, enable_sqlx_log, false).await?;
        let mut meta_manager = MysqlMetaManager::new_mysql_compatible(conn_pool, db_type).await?;

        if let Some(MetaCenterConfig::MySqlDbEngine {
            url,
            ddl_conflict_policy,
            ..
        }) = &meta_center_config
        {
            let meta_center_conn_pool =
                Self::create_mysql_conn_pool(url, 1, enable_sqlx_log, false).await?;
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

    pub async fn create_mongo_client(url: &str, app_name: &str) -> anyhow::Result<mongodb::Client> {
        let mut client_options = ClientOptions::parse_async(url).await?;
        // app_name only for debug usage
        client_options.app_name = Some(app_name.to_string());
        client_options.direct_connection = Some(true);
        Ok(mongodb::Client::with_options(client_options)?)
    }

    pub fn check_enable_sqlx_log(log_level: &str) -> bool {
        log_level == "debug" || log_level == "trace"
    }

    pub async fn list_schemas(url: &str, db_type: &DbType) -> anyhow::Result<Vec<String>> {
        let mut dbs = match db_type {
            DbType::Mysql => Self::list_mysql_dbs(url).await?,
            DbType::Pg => Self::list_pg_schemas(url).await?,
            DbType::Mongo => Self::list_mongo_dbs(url).await?,
            _ => Vec::new(),
        };
        dbs.sort();
        Ok(dbs)
    }

    pub async fn list_tbs(
        url: &str,
        schema: &str,
        db_type: &DbType,
    ) -> anyhow::Result<Vec<String>> {
        let mut tbs = match db_type {
            DbType::Mysql => Self::list_mysql_tbs(url, schema).await?,
            DbType::Pg => Self::list_pg_tbs(url, schema).await?,
            DbType::Mongo => Self::list_mongo_tbs(url, schema).await?,
            _ => Vec::new(),
        };
        tbs.sort();
        Ok(tbs)
    }

    pub async fn check_tb_exist(
        url: &str,
        schema: &str,
        tb: &str,
        db_type: &DbType,
    ) -> anyhow::Result<bool> {
        let schemas = Self::list_schemas(url, db_type).await?;
        if !schemas.contains(&schema.to_string()) {
            return Ok(false);
        }

        let tbs = Self::list_tbs(url, schema, db_type).await?;
        Ok(tbs.contains(&tb.to_string()))
    }

    pub async fn check_and_create_tb(
        url: &str,
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
        if TaskUtil::check_tb_exist(url, schema, tb, db_type).await? {
            return Ok(());
        }

        match db_type {
            DbType::Mysql => {
                let conn_pool = Self::create_mysql_conn_pool(url, 1, true, false).await?;
                sqlx::query(schema_sql).execute(&conn_pool).await?;
                sqlx::query(tb_sql).execute(&conn_pool).await?;
                conn_pool.close().await
            }

            DbType::Pg => {
                let conn_pool = Self::create_pg_conn_pool(url, 1, true, false).await?;
                sqlx::query(schema_sql).execute(&conn_pool).await?;
                sqlx::query(tb_sql).execute(&conn_pool).await?;
                conn_pool.close().await
            }

            _ => {}
        }
        Ok(())
    }

    async fn list_pg_schemas(url: &str) -> anyhow::Result<Vec<String>> {
        let mut schemas = Vec::new();
        let conn_pool = TaskUtil::create_pg_conn_pool(url, 1, false, false).await?;

        let sql = "SELECT schema_name
            FROM information_schema.schemata
            WHERE catalog_name = current_database()";
        let mut rows = sqlx::query(sql).fetch(&conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let schema: String = row.try_get(0)?;
            if PG_SYS_SCHEMAS.contains(&schema.as_str()) {
                continue;
            }
            schemas.push(schema);
        }
        conn_pool.close().await;
        Ok(schemas)
    }

    async fn list_pg_tbs(url: &str, schema: &str) -> anyhow::Result<Vec<String>> {
        let mut tbs = Vec::new();
        let conn_pool = TaskUtil::create_pg_conn_pool(url, 1, false, false).await?;

        let sql = format!(
            "SELECT table_name 
            FROM information_schema.tables
            WHERE table_catalog = current_database() 
            AND table_schema = '{}' 
            AND table_type = 'BASE TABLE'",
            schema
        );
        let mut rows = sqlx::query(&sql).fetch(&conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let tb: String = row.try_get(0)?;
            tbs.push(tb);
        }
        conn_pool.close().await;
        Ok(tbs)
    }

    async fn list_mysql_dbs(url: &str) -> anyhow::Result<Vec<String>> {
        let mut dbs = Vec::new();
        let conn_pool = TaskUtil::create_mysql_conn_pool(url, 1, false, false).await?;

        let sql = "SHOW DATABASES";
        let mut rows = sqlx::query(sql).fetch(&conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let db: String = row.try_get(0)?;
            if MYSQL_SYS_DBS.contains(&db.as_str()) {
                continue;
            }
            dbs.push(db);
        }
        conn_pool.close().await;
        Ok(dbs)
    }

    async fn list_mysql_tbs(url: &str, db: &str) -> anyhow::Result<Vec<String>> {
        let mut tbs = Vec::new();
        let conn_pool = Self::create_mysql_conn_pool(url, 1, false, false).await?;

        let sql = format!("SHOW FULL TABLES IN `{}`", db);
        let mut rows = sqlx::query(&sql).fetch(&conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let tb: String = row.try_get(0)?;
            let tb_type: String = row.try_get(1)?;
            if tb_type == "BASE TABLE" {
                tbs.push(tb);
            }
        }
        conn_pool.close().await;
        Ok(tbs)
    }

    async fn list_mongo_dbs(url: &str) -> anyhow::Result<Vec<String>> {
        let client = Self::create_mongo_client(url, "").await?;
        let dbs = client.list_database_names(None, None).await?;
        client.shutdown().await;
        Ok(dbs)
    }

    async fn list_mongo_tbs(url: &str, db: &str) -> anyhow::Result<Vec<String>> {
        let client = Self::create_mongo_client(url, "").await?;
        // filter views and system tables
        let tbs = client
            .database(db)
            .list_collection_names(Some(doc! { "type": "collection" }))
            .await?
            .into_iter()
            .filter(|name| !name.starts_with("system."))
            .collect();
        client.shutdown().await;
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
}
