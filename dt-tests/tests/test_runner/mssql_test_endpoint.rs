use std::collections::HashSet;

use anyhow::{bail, Context};
use dt_common::{
    config::{
        config_enums::DbType, connection_auth_config::ConnectionAuthConfig, task_config::TaskConfig,
    },
    meta::{
        mssql::{mssql_connection_pool::MssqlConnectionPool, mssql_meta_manager::MssqlMetaManager},
        row_data::RowData,
    },
    utils::sql_util::SqlUtil,
};
use dt_connector::rdb_query_builder::RdbQueryBuilder;
use tiberius::Client;
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::test_config_util::TestConfigUtil;

type MssqlTestTdsClient = Client<Compat<TcpStream>>;

#[derive(Clone, Copy)]
pub enum TaskConfigEndpoint {
    Extractor,
    Sinker,
}

#[derive(Clone)]
pub struct MssqlTestEndpoint {
    config: MssqlEndpointConfig,
}

#[derive(Clone)]
struct MssqlEndpointConfig {
    connection_string: String,
    connection_auth: ConnectionAuthConfig,
    app_name: Option<String>,
    max_connections: u32,
    connection_timeout_secs: u64,
}

impl MssqlTestEndpoint {
    pub fn from_config_file(
        relative_config_file: &str,
        endpoint: TaskConfigEndpoint,
    ) -> anyhow::Result<Self> {
        let config = TestConfigUtil::load_task_config(relative_config_file)?;
        Self::from_task_config(&config, endpoint)
    }

    pub fn from_task_config(
        config: &TaskConfig,
        endpoint: TaskConfigEndpoint,
    ) -> anyhow::Result<Self> {
        let (db_type, connection_string, connection_auth, app_name, max_connections, timeout) =
            match endpoint {
                TaskConfigEndpoint::Extractor => (
                    config.extractor_basic.db_type.clone(),
                    config.extractor_basic.url.clone(),
                    config.extractor_basic.connection_auth.clone(),
                    config.extractor_basic.app_name.clone(),
                    config.extractor_basic.max_connections,
                    config.extractor_basic.connection_timeout_secs,
                ),
                TaskConfigEndpoint::Sinker => {
                    let target = config
                        .destination_target()
                        .context("MSSQL test destination endpoint is not configured")?;
                    (
                        target.db_type,
                        target.url,
                        target.connection_auth,
                        target.app_name,
                        target.max_connections,
                        target.connection_timeout_secs,
                    )
                }
            };
        anyhow::ensure!(
            db_type == DbType::Mssql,
            "MSSQL test endpoint requires an MSSQL endpoint, got {db_type:?}"
        );
        MssqlConnectionPool::build_client_config(
            &connection_string,
            &connection_auth,
            app_name.as_deref(),
        )?;

        Ok(Self {
            config: MssqlEndpointConfig {
                connection_string,
                connection_auth,
                app_name,
                max_connections,
                connection_timeout_secs: timeout,
            },
        })
    }

    pub fn from_connection_string_and_auth(
        connection_string: &str,
        connection_auth: ConnectionAuthConfig,
    ) -> anyhow::Result<Self> {
        MssqlConnectionPool::build_client_config(connection_string, &connection_auth, None)?;

        Ok(Self {
            config: MssqlEndpointConfig {
                connection_string: connection_string.to_string(),
                connection_auth,
                app_name: None,
                max_connections: 2,
                connection_timeout_secs: 15,
            },
        })
    }

    pub fn connection_string(&self) -> &str {
        &self.config.connection_string
    }

    pub fn connection_auth(&self) -> &ConnectionAuthConfig {
        &self.config.connection_auth
    }

    pub fn username(&self) -> anyhow::Result<&str> {
        match &self.config.connection_auth {
            ConnectionAuthConfig::Basic { username, .. } => Ok(username),
            ConnectionAuthConfig::BasicSsl { username, .. } => username
                .as_deref()
                .context("MSSQL test endpoint username is not configured"),
            ConnectionAuthConfig::NoAuth => {
                anyhow::bail!("MSSQL test endpoint authentication is not configured")
            }
        }
    }

    pub fn password(&self) -> anyhow::Result<&str> {
        match &self.config.connection_auth {
            ConnectionAuthConfig::Basic { password, .. }
            | ConnectionAuthConfig::BasicSsl { password, .. } => password
                .as_deref()
                .context("MSSQL test endpoint password is not configured"),
            ConnectionAuthConfig::NoAuth => {
                anyhow::bail!("MSSQL test endpoint authentication is not configured")
            }
        }
    }

    async fn connect_to(&self, database: &str) -> anyhow::Result<MssqlTestTdsClient> {
        let mut config = MssqlConnectionPool::build_client_config(
            &self.config.connection_string,
            &self.config.connection_auth,
            self.config.app_name.as_deref(),
        )?;
        config.database(database);

        let tcp = TcpStream::connect(config.get_addr()).await?;
        tcp.set_nodelay(true)?;
        Ok(Client::connect(config, tcp.compat_write()).await?)
    }

    async fn connect(&self) -> anyhow::Result<MssqlTestTdsClient> {
        let config = MssqlConnectionPool::build_client_config(
            &self.config.connection_string,
            &self.config.connection_auth,
            self.config.app_name.as_deref(),
        )?;

        let tcp = TcpStream::connect(config.get_addr()).await?;
        tcp.set_nodelay(true)?;
        Ok(Client::connect(config, tcp.compat_write()).await?)
    }

    pub async fn ensure_database(&self, database: &str) -> anyhow::Result<()> {
        let mut client = self.connect_to("master").await?;
        let database_literal = database.replace('\'', "''");
        let database_identifier = format!("[{}]", database.replace(']', "]]"));
        let sql = format!(
            "IF DB_ID(N'{database_literal}') IS NULL CREATE DATABASE {database_identifier}"
        );
        client.simple_query(sql).await?.into_results().await?;
        Ok(())
    }

    pub async fn check_connection(&self) -> anyhow::Result<()> {
        let mut client = self.connect().await?;
        client
            .simple_query("SELECT 1")
            .await?
            .into_results()
            .await?;
        Ok(())
    }

    pub async fn execute_batches(&self, batches: &[String]) -> anyhow::Result<()> {
        let mut client = self.connect().await?;
        for batch in batches {
            client
                .simple_query(batch.as_str())
                .await?
                .into_results()
                .await?;
        }
        Ok(())
    }

    pub async fn execute_batch(pool: &MssqlConnectionPool, sql: &str) -> anyhow::Result<()> {
        let mut connection = pool.get().await?;
        connection
            .client_mut()
            .simple_query(sql)
            .await?
            .into_results()
            .await?;
        Ok(())
    }

    pub async fn create_pool(&self) -> anyhow::Result<MssqlConnectionPool> {
        self.create_pool_with(
            self.config.max_connections,
            self.config.connection_timeout_secs,
        )
        .await
    }

    pub async fn create_pool_with(
        &self,
        max_connections: u32,
        connection_timeout_secs: u64,
    ) -> anyhow::Result<MssqlConnectionPool> {
        MssqlConnectionPool::from_config(
            &self.config.connection_string,
            &self.config.connection_auth,
            self.config.app_name.as_deref(),
            max_connections,
            connection_timeout_secs,
        )
        .await
    }

    pub async fn create_meta_manager(
        pool: MssqlConnectionPool,
    ) -> anyhow::Result<MssqlMetaManager> {
        MssqlMetaManager::new(pool).await
    }

    pub async fn fetch_table(
        &self,
        db: &str,
        schema: &str,
        tb: &str,
        ignore_cols: Option<&HashSet<String>>,
        where_sql: &str,
    ) -> anyhow::Result<Vec<RowData>> {
        let pool = self.create_pool().await?;
        let mut meta_manager = Self::create_meta_manager(pool).await?;
        let tb_meta = meta_manager.get_tb_meta(db, schema, tb).await?.clone();
        let mut compare_ignore_cols = tb_meta.non_comparable_cols();
        if let Some(ignore_cols) = ignore_cols {
            compare_ignore_cols.extend(ignore_cols.iter().cloned());
        }
        let query_builder = RdbQueryBuilder::new_for_mssql(&tb_meta, Some(&compare_ignore_cols));
        let cols = query_builder.build_extract_cols_str()?;
        if cols.is_empty() {
            bail!("MSSQL compare has no comparable columns for {db}.{schema}.{tb}");
        }
        let order_col = tb_meta
            .basic
            .order_cols
            .iter()
            .chain(tb_meta.basic.cols.iter())
            .find(|col| !compare_ignore_cols.contains(*col))
            .context("MSSQL compare has no column available for ordering")?;
        let sql = format!(
            "SELECT {cols} FROM {} {where_sql} ORDER BY {} ASC",
            SqlUtil::render_rdb_table(&DbType::Mssql, db, schema, tb),
            Self::quote(order_col),
        );

        let mut client = self.connect().await?;
        let rows = client.simple_query(sql).await?.into_first_result().await?;
        rows.iter()
            .map(|row| RowData::from_mssql_row(row, &tb_meta, &Some(&compare_ignore_cols), None))
            .collect()
    }

    pub async fn get_table_columns(
        &self,
        db: &str,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<Vec<String>> {
        let pool = self.create_pool().await?;
        let mut meta_manager = Self::create_meta_manager(pool).await?;
        Ok(meta_manager
            .get_tb_meta(db, schema, tb)
            .await?
            .basic
            .cols
            .clone())
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }

    fn quote(identifier: &str) -> String {
        SqlUtil::escape_by_db_type(identifier, &DbType::Mssql)
    }
}
