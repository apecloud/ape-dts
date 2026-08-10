use dt_common::config::connection_auth_config::ConnectionAuthConfig;
use dt_common::meta::mssql::mssql_connection_pool::MssqlConnectionPool;
use tiberius::Client;
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

type MssqlTestTdsClient = Client<Compat<TcpStream>>;

#[derive(Clone)]
pub struct MssqlTestClient {
    endpoint: MssqlTestEndpoint,
}

#[derive(Clone)]
struct MssqlTestEndpoint {
    connection_string: String,
    connection_auth: ConnectionAuthConfig,
}

impl MssqlTestClient {
    pub fn from_connection_string_and_auth(
        connection_string: &str,
        connection_auth: ConnectionAuthConfig,
    ) -> anyhow::Result<Self> {
        MssqlConnectionPool::build_client_config(connection_string, &connection_auth, None)?;

        Ok(Self {
            endpoint: MssqlTestEndpoint {
                connection_string: connection_string.to_string(),
                connection_auth,
            },
        })
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
}
