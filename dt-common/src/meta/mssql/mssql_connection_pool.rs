use bb8::ManageConnection;
use bb8_tiberius::ConnectionManager;
use tiberius::Client;
use tokio::net::TcpStream;
use tokio_util::compat::Compat;

use crate::{config::connection_auth_config::ConnectionAuthConfig, error::DtError};

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
    _inner: ConnectionManager,
}

impl ManageConnection for MssqlConnectionManager {
    type Connection = MssqlManagedConnection;
    type Error = bb8_tiberius::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        todo!("mssql pooled connection creation is not implemented")
    }

    async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), Self::Error> {
        todo!("mssql pooled connection health check is not implemented")
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        !conn.reusable
    }
}

pub type MssqlPooledConnection<'a> = bb8::PooledConnection<'a, MssqlConnectionManager>;

#[derive(Clone)]
pub struct MssqlConnectionPool {
    inner: bb8::Pool<MssqlConnectionManager>,
}

impl MssqlConnectionPool {
    pub async fn from_config(
        _url: &str,
        _auth: &ConnectionAuthConfig,
        _max_connections: u32,
    ) -> anyhow::Result<Self> {
        todo!("mssql bb8 pool construction is not implemented")
    }

    pub async fn get(&self) -> anyhow::Result<MssqlPooledConnection<'_>> {
        Ok(self.inner.get().await?)
    }

    pub async fn check_connection(&self) -> anyhow::Result<()> {
        todo!("mssql connection health check is not implemented")
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        todo!("mssql bb8 pool shutdown is not implemented")
    }

    pub fn max_size(&self) -> u32 {
        self.inner.config().max_size
    }
}

#[allow(dead_code)]
fn assert_mssql_client_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<MssqlClient>();
}
