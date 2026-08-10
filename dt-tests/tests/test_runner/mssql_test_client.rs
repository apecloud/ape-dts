use anyhow::{bail, Context};
use dt_common::config::connection_auth_config::ConnectionAuthConfig;
use tiberius::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use url::Url;

type MssqlTestTdsClient = Client<Compat<TcpStream>>;

#[derive(Clone)]
pub struct MssqlTestClient {
    endpoint: MssqlTestEndpoint,
}

#[derive(Clone)]
struct MssqlTestEndpoint {
    host: String,
    port: u16,
    database: String,
    connection_auth: ConnectionAuthConfig,
}

impl MssqlTestClient {
    pub fn from_url_and_auth(
        url: &str,
        connection_auth: ConnectionAuthConfig,
    ) -> anyhow::Result<Self> {
        let url = Url::parse(url).context("failed to parse MSSQL test endpoint URL")?;
        let host = url
            .host_str()
            .context("MSSQL test endpoint URL must include a host")?
            .to_string();
        let port = url.port().unwrap_or(1433);
        let database = url.path().trim_matches('/').to_string();
        if database.is_empty() || database.contains('/') {
            bail!("MSSQL test endpoint URL must include exactly one database");
        }

        Ok(Self {
            endpoint: MssqlTestEndpoint {
                host,
                port,
                database,
                connection_auth,
            },
        })
    }

    pub fn database(&self) -> &str {
        &self.endpoint.database
    }

    async fn connect_to(&self, database: &str) -> anyhow::Result<MssqlTestTdsClient> {
        let (username, password) = Self::credentials(&self.endpoint.connection_auth)?;
        let mut config = Config::new();
        config.host(&self.endpoint.host);
        config.port(self.endpoint.port);
        config.database(database);
        config.authentication(AuthMethod::sql_server(username, password));
        if let Some(ssl_config) = self.endpoint.connection_auth.ssl_config() {
            ssl_config.apply_mssql(&mut config)?;
        }

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

    fn credentials(auth: &ConnectionAuthConfig) -> anyhow::Result<(String, String)> {
        match auth {
            ConnectionAuthConfig::Basic { username, password } => Ok((
                username.clone(),
                password
                    .clone()
                    .context("MSSQL test password is required")?,
            )),
            ConnectionAuthConfig::BasicSsl {
                username, password, ..
            } => Ok((
                username
                    .clone()
                    .context("MSSQL test username is required")?,
                password
                    .clone()
                    .context("MSSQL test password is required")?,
            )),
            ConnectionAuthConfig::NoAuth => bail!("MSSQL test SQL authentication is required"),
        }
    }
}
