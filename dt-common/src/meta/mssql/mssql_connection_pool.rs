use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::Context;
use bb8::ManageConnection;
use bb8_tiberius::ConnectionManager;
use tiberius::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::Compat;
use url::Url;

use crate::{config::connection_auth_config::ConnectionAuthConfig, error::DtError};

const DEFAULT_MSSQL_PORT: u16 = 1433;
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(15);
const APPLICATION_NAME: &str = "ape-dts";

pub type MssqlClient = Client<Compat<TcpStream>>;

pub struct MssqlManagedConnection {
    client: MssqlClient,
    reusable: bool,
    identity_insert_table: Option<(String, String)>,
}

impl MssqlManagedConnection {
    pub fn client_mut(&mut self) -> &mut MssqlClient {
        self.begin_operation();
        &mut self.client
    }

    pub fn begin_operation(&mut self) {
        self.reusable = false;
    }

    pub fn mark_reusable(&mut self) -> anyhow::Result<()> {
        if let Some((schema, tb)) = &self.identity_insert_table {
            return Err(DtError::InvariantViolated(format!(
                "cannot reuse MSSQL connection while IDENTITY_INSERT is enabled for {schema}.{tb}"
            ))
            .into());
        }
        self.reusable = true;
        Ok(())
    }

    pub fn mark_identity_insert_enabled(&mut self, schema: &str, tb: &str) -> anyhow::Result<()> {
        if let Some(active_table) = &self.identity_insert_table {
            return Err(DtError::InvariantViolated(format!(
                "MSSQL IDENTITY_INSERT is already enabled for {}.{}",
                active_table.0, active_table.1
            ))
            .into());
        }
        self.reusable = false;
        self.identity_insert_table = Some((schema.to_string(), tb.to_string()));
        Ok(())
    }

    pub fn mark_identity_insert_disabled(&mut self, schema: &str, tb: &str) -> anyhow::Result<()> {
        let expected = (schema.to_string(), tb.to_string());
        if self.identity_insert_table.as_ref() != Some(&expected) {
            return Err(DtError::InvariantViolated(format!(
                "MSSQL IDENTITY_INSERT cleanup does not match {schema}.{tb}"
            ))
            .into());
        }
        self.identity_insert_table = None;
        Ok(())
    }

    pub fn identity_insert_table(&self) -> Option<&(String, String)> {
        self.identity_insert_table.as_ref()
    }
}

pub struct MssqlConnectionManager {
    inner: ConnectionManager,
    closed: Arc<AtomicBool>,
}

impl ManageConnection for MssqlConnectionManager {
    type Connection = MssqlManagedConnection;
    type Error = bb8_tiberius::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        if self.closed.load(Ordering::Acquire) {
            return Err(pool_closed_error());
        }

        let client = self.inner.connect().await?;
        Ok(MssqlManagedConnection {
            client,
            reusable: true,
            identity_insert_table: None,
        })
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        if self.closed.load(Ordering::Acquire)
            || !conn.reusable
            || conn.identity_insert_table.is_some()
        {
            return Err(pool_closed_error());
        }

        conn.client
            .simple_query("SELECT 1")
            .await?
            .into_results()
            .await?;
        Ok(())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        self.closed.load(Ordering::Acquire) || !conn.reusable
    }
}

pub type MssqlPooledConnection<'a> = bb8::PooledConnection<'a, MssqlConnectionManager>;

#[derive(Clone, Debug)]
pub struct MssqlConnectionPool {
    inner: bb8::Pool<MssqlConnectionManager>,
    closed: Arc<AtomicBool>,
}

impl MssqlConnectionPool {
    pub async fn from_config(
        url: &str,
        auth: &ConnectionAuthConfig,
        max_connections: u32,
    ) -> anyhow::Result<Self> {
        if max_connections == 0 {
            return Err(
                DtError::invalid_config("MSSQL max_connections must be greater than 0").into(),
            );
        }

        let url =
            Url::parse(url).context(DtError::invalid_config("MSSQL connection URL is invalid"))?;
        let database_segments = url
            .path_segments()
            .map(|segments| {
                segments
                    .filter(|segment| !segment.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if url.scheme() != "mssql" || url.host_str().is_none() || database_segments.len() != 1 {
            return Err(DtError::invalid_config(
                "MSSQL connection URL must use mssql://host[:port]/database with exactly one database",
            )
            .into());
        }

        let (username, password) = match auth {
            ConnectionAuthConfig::Basic { username, password } => {
                (Some(username.as_str()), password.as_deref())
            }
            ConnectionAuthConfig::BasicSsl {
                username, password, ..
            } => (username.as_deref(), password.as_deref()),
            ConnectionAuthConfig::NoAuth => (None, None),
        };
        let username = username.filter(|value| !value.is_empty()).ok_or_else(|| {
            DtError::invalid_config("MSSQL SQL Server authentication requires a username")
        })?;
        let password = password.filter(|value| !value.is_empty()).ok_or_else(|| {
            DtError::invalid_config("MSSQL SQL Server authentication requires a password")
        })?;

        let database = urlencoding::decode(database_segments[0])
            .context(DtError::invalid_config(
                "MSSQL database name contains invalid URL encoding",
            ))?
            .into_owned();
        let mut config = Config::new();
        config.host(url.host_str().expect("MSSQL host was validated"));
        config.port(url.port().unwrap_or(DEFAULT_MSSQL_PORT));
        config.database(database);
        config.application_name(APPLICATION_NAME);
        config.authentication(AuthMethod::sql_server(username, password));
        if let Some(ssl_config) = auth.ssl_config() {
            ssl_config.apply_mssql(&mut config)?;
        }

        let closed = Arc::new(AtomicBool::new(false));
        let manager = MssqlConnectionManager {
            inner: ConnectionManager::new(config),
            closed: closed.clone(),
        };
        let inner = bb8::Pool::builder()
            .max_size(max_connections)
            .connection_timeout(CONNECTION_TIMEOUT)
            .build(manager)
            .await
            .context("failed to create MSSQL connection pool")?;
        let pool = Self { inner, closed };
        pool.check_connection().await?;
        Ok(pool)
    }

    pub async fn get(&self) -> anyhow::Result<MssqlPooledConnection<'_>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(
                DtError::ConnectionFailed("MSSQL connection pool is closed".to_string()).into(),
            );
        }

        let mut connection = self.inner.get().await?;
        if self.closed.load(Ordering::Acquire) {
            connection.begin_operation();
            return Err(
                DtError::ConnectionFailed("MSSQL connection pool is closed".to_string()).into(),
            );
        }
        Ok(connection)
    }

    pub async fn check_connection(&self) -> anyhow::Result<()> {
        drop(self.get().await?);
        Ok(())
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        self.closed.store(true, Ordering::Release);
        Ok(())
    }

    pub fn max_size(&self) -> u32 {
        self.inner.config().max_size
    }
}

fn pool_closed_error() -> bb8_tiberius::Error {
    bb8_tiberius::Error::Tiberius(tiberius::error::Error::Protocol(
        "MSSQL connection pool is closed or the session is not reusable".into(),
    ))
}

#[allow(dead_code)]
fn assert_mssql_client_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<MssqlClient>();
}
