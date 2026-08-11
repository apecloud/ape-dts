use std::time::Duration;

use anyhow::Context;
use bb8::ManageConnection;
use bb8_tiberius::ConnectionManager;
use tiberius::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::Compat;

use crate::{
    config::connection_auth_config::ConnectionAuthConfig,
    error::{DtError, DtResultExt},
};

pub type MssqlClient = Client<Compat<TcpStream>>;

pub struct MssqlManagedConnection {
    client: MssqlClient,
    discard_on_return: bool,
}

impl MssqlManagedConnection {
    pub fn client_mut(&mut self) -> &mut MssqlClient {
        &mut self.client
    }

    pub fn mark_for_discard(&mut self) {
        self.discard_on_return = true;
    }

    pub fn clear_discard_mark(&mut self) {
        self.discard_on_return = false;
    }

    pub fn will_discard(&self) -> bool {
        self.discard_on_return
    }
}

pub struct MssqlConnectionManager {
    inner: ConnectionManager,
}

impl ManageConnection for MssqlConnectionManager {
    type Connection = MssqlManagedConnection;
    type Error = bb8_tiberius::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let client = self.inner.connect().await?;
        Ok(MssqlManagedConnection {
            client,
            discard_on_return: false,
        })
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        self.inner.is_valid(conn.client_mut()).await
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        // bb8 uses has_broken as its synchronous discard-on-return hook. An
        // unfinished transaction makes the session unsafe to reuse even when
        // the underlying TCP connection is still alive.
        conn.will_discard()
    }
}

pub type MssqlPooledConnection<'a> = bb8::PooledConnection<'a, MssqlConnectionManager>;

#[derive(Clone)]
pub struct MssqlConnectionPool {
    inner: bb8::Pool<MssqlConnectionManager>,
}

impl MssqlConnectionPool {
    pub async fn from_config(
        connection_string: &str,
        auth: &ConnectionAuthConfig,
        application_name: Option<&str>,
        max_connections: u32,
        connection_timeout_secs: u64,
    ) -> anyhow::Result<Self> {
        if max_connections == 0 {
            return Err(
                DtError::invalid_config("MSSQL max_connections must be greater than 0").into(),
            );
        }
        if connection_timeout_secs == 0 {
            return Err(DtError::invalid_config(
                "MSSQL connection_timeout_secs must be greater than 0",
            )
            .into());
        }

        let config = Self::build_client_config(connection_string, auth, application_name)?;

        let manager = MssqlConnectionManager {
            inner: ConnectionManager::new(config),
        };
        let inner = bb8::Pool::builder()
            .max_size(max_connections)
            .connection_timeout(Duration::from_secs(connection_timeout_secs))
            .build(manager)
            .await
            .context("failed to create MSSQL connection pool")?;
        let pool = Self { inner };
        pool.check_connection().await?;
        Ok(pool)
    }

    pub fn build_client_config(
        connection_string: &str,
        auth: &ConnectionAuthConfig,
        application_name: Option<&str>,
    ) -> anyhow::Result<Config> {
        if connection_string.trim().is_empty() {
            return Err(
                DtError::invalid_config("MSSQL connection string must not be empty").into(),
            );
        }

        // Keep ADO.NET as the primary format and only try JDBC after the ADO
        // parser rejects the input.
        let mut config = match Config::from_ado_string(connection_string) {
            Ok(config) => config,
            Err(_) => {
                Config::from_jdbc_string(connection_string).dt_error(DtError::invalid_config(
                    "MSSQL connection string must be a valid ADO.NET or JDBC string",
                ))?
            }
        };

        // Values supplied as dedicated task fields take precedence over the
        // same values embedded in the connection string.
        let auth_override = match auth {
            ConnectionAuthConfig::Basic { username, password } => {
                (Some(username.as_str()), password.as_deref())
            }
            ConnectionAuthConfig::BasicSsl {
                username, password, ..
            } => (username.as_deref(), password.as_deref()),
            ConnectionAuthConfig::NoAuth => (None, None),
        };
        match auth_override {
            (None, None) => {}
            (Some(username), Some(password)) if !username.is_empty() && !password.is_empty() => {
                config.authentication(AuthMethod::sql_server(username, password));
            }
            _ => {
                return Err(DtError::invalid_config(
                    "MSSQL authentication override requires both username and password",
                )
                .into());
            }
        }

        if let Some(ssl_config) = auth.ssl_config() {
            ssl_config.apply_mssql(&mut config)?;
        }
        if let Some(application_name) = application_name.filter(|value| !value.is_empty()) {
            config.application_name(application_name);
        }
        Ok(config)
    }

    pub async fn get(&self) -> anyhow::Result<MssqlPooledConnection<'_>> {
        Ok(self.inner.get().await?)
    }

    pub async fn check_connection(&self) -> anyhow::Result<()> {
        drop(self.get().await?);
        Ok(())
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }

    pub fn max_size(&self) -> u32 {
        self.inner.config().max_size
    }

    pub fn connection_timeout(&self) -> Duration {
        self.inner.config().connection_timeout
    }
}

#[allow(dead_code)]
fn assert_mssql_client_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<MssqlClient>();
}
