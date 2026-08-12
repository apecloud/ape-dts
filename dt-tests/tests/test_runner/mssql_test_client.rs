use anyhow::Context;
use connection_string::{AdoNetString, JdbcString};
use dt_common::{
    config::config_enums::DbType,
    config::{connection_auth_config::ConnectionAuthConfig, task_config::TaskConfig},
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

type MssqlTestTdsClient = Client<Compat<TcpStream>>;

#[derive(Clone, Copy)]
pub enum TestSide {
    Source,
    Destination,
}

#[derive(Clone)]
pub struct MssqlTestClient {
    endpoint: MssqlTestEndpoint,
}

#[derive(Clone)]
struct MssqlTestEndpoint {
    connection_string: String,
    connection_auth: ConnectionAuthConfig,
    database: Option<String>,
}

impl MssqlTestClient {
    pub fn from_task_config(config: &TaskConfig, side: TestSide) -> anyhow::Result<Self> {
        let (connection_string, connection_auth) = match side {
            TestSide::Source => (
                config.extractor_basic.url.clone(),
                config.extractor_basic.connection_auth.clone(),
            ),
            TestSide::Destination => {
                let target = config
                    .destination_target()
                    .context("MSSQL test destination endpoint is not configured")?;
                (target.url, target.connection_auth)
            }
        };
        let database = Self::database_from_connection_string(&connection_string)?;
        let mut client =
            Self::from_connection_string_and_auth(&connection_string, connection_auth)?;
        client.endpoint.database = Some(database);
        Ok(client)
    }

    pub fn from_connection_string_and_auth(
        connection_string: &str,
        connection_auth: ConnectionAuthConfig,
    ) -> anyhow::Result<Self> {
        MssqlConnectionPool::build_client_config(connection_string, &connection_auth, None)?;

        Ok(Self {
            endpoint: MssqlTestEndpoint {
                connection_string: connection_string.to_string(),
                connection_auth,
                database: None,
            },
        })
    }

    pub fn database(&self) -> &str {
        self.endpoint
            .database
            .as_deref()
            .expect("MSSQL task test client must have a database")
    }

    async fn connect_to(&self, database: &str) -> anyhow::Result<MssqlTestTdsClient> {
        let mut config = MssqlConnectionPool::build_client_config(
            &self.endpoint.connection_string,
            &self.endpoint.connection_auth,
            None,
        )?;
        config.database(database);

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

    pub async fn check_connection(&self, database: &str) -> anyhow::Result<()> {
        let mut client = self.connect_to(database).await?;
        client
            .simple_query("SELECT 1")
            .await?
            .into_results()
            .await?;
        Ok(())
    }

    pub async fn execute_batches(&self, batches: &[String]) -> anyhow::Result<()> {
        let mut client = self.connect_to(self.database()).await?;
        for batch in batches {
            client
                .simple_query(batch.as_str())
                .await?
                .into_results()
                .await?;
        }
        Ok(())
    }

    pub async fn fetch_table(
        &self,
        schema: &str,
        tb: &str,
        where_sql: &str,
    ) -> anyhow::Result<Vec<RowData>> {
        let pool = self.create_pool().await?;
        let mut meta_manager = MssqlMetaManager::new(pool).await?;
        let tb_meta = meta_manager.get_tb_meta(schema, tb).await?.clone();
        let query_builder = RdbQueryBuilder::new_for_mssql(&tb_meta, None);
        let cols = query_builder.build_extract_cols_str()?;
        let sql = format!(
            "SELECT {cols} FROM {}.{} {where_sql} ORDER BY {} ASC",
            Self::quote(schema),
            Self::quote(tb),
            Self::quote(&tb_meta.basic.cols[0]),
        );

        let mut client = self.connect_to(self.database()).await?;
        let rows = client.simple_query(sql).await?.into_first_result().await?;
        rows.iter()
            .map(|row| RowData::from_mssql_row(row, &tb_meta, &None, None))
            .collect()
    }

    pub async fn get_table_columns(&self, schema: &str, tb: &str) -> anyhow::Result<Vec<String>> {
        let pool = self.create_pool().await?;
        let mut meta_manager = MssqlMetaManager::new(pool).await?;
        Ok(meta_manager
            .get_tb_meta(schema, tb)
            .await?
            .basic
            .cols
            .clone())
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }

    pub async fn create_pool(&self) -> anyhow::Result<MssqlConnectionPool> {
        MssqlConnectionPool::from_config(
            &self.endpoint.connection_string,
            &self.endpoint.connection_auth,
            None,
            2,
            15,
        )
        .await
    }

    fn quote(identifier: &str) -> String {
        SqlUtil::escape_by_db_type(identifier, &DbType::Mssql)
    }

    fn database_from_connection_string(connection_string: &str) -> anyhow::Result<String> {
        if let Ok(ado) = connection_string.parse::<AdoNetString>() {
            return ado
                .get("database")
                .cloned()
                .context("MSSQL ADO.NET connection string must include database");
        }

        connection_string
            .parse::<JdbcString>()
            .context("failed to parse MSSQL JDBC connection string")?
            .properties()
            .get("database")
            .cloned()
            .context("MSSQL JDBC connection string must include database")
    }
}
