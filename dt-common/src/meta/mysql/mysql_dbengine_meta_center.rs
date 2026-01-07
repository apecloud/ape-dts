use std::str::FromStr;

use anyhow::{bail, Ok};
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
    MySql, Pool,
};

use crate::{
    config::{config_enums::ConflictPolicyEnum, connection_auth_config::ConnectionAuthConfig},
    log_error, log_info,
    meta::ddl_meta::{ddl_data::DdlData, ddl_type::DdlType},
};

use super::mysql_meta_fetcher::MysqlMetaFetcher;

#[derive(Clone)]
pub struct MysqlDbEngineMetaCenter {
    pub meta_fetcher: MysqlMetaFetcher,
    pub url: String,
    pub connection_auth: ConnectionAuthConfig,
    pub ddl_conflict_policy: ConflictPolicyEnum,
}

impl MysqlDbEngineMetaCenter {
    pub async fn new(
        url: String,
        connection_auth: ConnectionAuthConfig,
        conn_pool: Pool<MySql>,
        ddl_conflict_policy: ConflictPolicyEnum,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            url,
            connection_auth,
            meta_fetcher: MysqlMetaFetcher::new(conn_pool).await?,
            ddl_conflict_policy,
        })
    }

    pub async fn sync_from_ddl(&mut self, ddl_data: &DdlData) -> anyhow::Result<()> {
        let (db, tb) = ddl_data.get_schema_tb();
        self.meta_fetcher.invalidate_cache(&db, &tb);
        log_info!(
            "sync ddl to meta_center, db: {}, query: {}",
            &db,
            &ddl_data.query
        );
        // create a tmp connection with database since sqlx conn pool does NOT support `USE db`
        let mut conn_options = MySqlConnectOptions::from_str(&self.url)?;
        if !db.is_empty() {
            match ddl_data.ddl_type {
                DdlType::CreateDatabase | DdlType::DropDatabase | DdlType::AlterDatabase => {}
                _ => {
                    conn_options = conn_options.database(&db);
                }
            }
        }

        if let ConnectionAuthConfig::Basic { username, password } = &self.connection_auth {
            conn_options = conn_options.username(username);
            if let Some(password_real) = password {
                conn_options = conn_options.password(password_real);
            }
        }

        let conn_pool = MySqlPoolOptions::new()
            .max_connections(1)
            .connect_with(conn_options)
            .await?;
        let query = sqlx::query(&ddl_data.query);
        if let Err(error) = query.execute(&conn_pool).await {
            if self.ddl_conflict_policy == ConflictPolicyEnum::Ignore {
                log_error!("failed to sync dll to meta_center: {}", error);
            } else {
                conn_pool.close().await;
                bail!(error);
            }
        }
        conn_pool.close().await;
        Ok(())
    }
}
