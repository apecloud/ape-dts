use async_trait::async_trait;
use dt_common::meta::{
    ddl_meta::ddl_data::DdlData,
    mssql::{
        mssql_connection_pool::{MssqlConnectionPool, MssqlPooledConnection},
        mssql_meta_manager::MssqlMetaManager,
    },
    row_data::RowData,
};
use tiberius::Query as MssqlQuery;

use crate::{rdb_router::RdbRouter, sinker::base_sinker::BaseSinker, Sinker};

#[derive(Clone)]
pub struct MssqlSinker {
    pub connection_pool: MssqlConnectionPool,
    pub meta_manager: MssqlMetaManager,
    pub router: Option<RdbRouter>,
    pub batch_size: usize,
    pub base_sinker: BaseSinker,
}

// A checked-out connection remains poisoned until commit/rollback and all
// session-scoped settings have been cleaned up successfully.
#[allow(dead_code)]
struct MssqlSinkSession<'pool> {
    connection: MssqlPooledConnection<'pool>,
    transaction_open: bool,
}

#[allow(dead_code)]
impl<'pool> MssqlSinkSession<'pool> {
    async fn begin(_pool: &'pool MssqlConnectionPool) -> anyhow::Result<Self> {
        todo!("mssql sink transaction begin is not implemented")
    }

    async fn enable_identity_insert(&mut self, _schema: &str, _tb: &str) -> anyhow::Result<()> {
        todo!("mssql IDENTITY_INSERT enable is not implemented")
    }

    async fn disable_identity_insert(&mut self, _schema: &str, _tb: &str) -> anyhow::Result<()> {
        todo!("mssql IDENTITY_INSERT disable is not implemented")
    }

    async fn execute<'query>(&mut self, _query: MssqlQuery<'query>) -> anyhow::Result<u64> {
        todo!("mssql parameterized INSERT execution is not implemented")
    }

    async fn commit(self) -> anyhow::Result<()> {
        todo!("mssql sink transaction commit and connection cleanup is not implemented")
    }

    async fn rollback(self) -> anyhow::Result<()> {
        todo!("mssql sink transaction rollback and connection cleanup is not implemented")
    }
}

impl MssqlSinker {
    pub fn new(
        connection_pool: MssqlConnectionPool,
        meta_manager: MssqlMetaManager,
        router: Option<RdbRouter>,
        batch_size: usize,
        base_sinker: BaseSinker,
    ) -> Self {
        Self {
            connection_pool,
            meta_manager,
            router,
            batch_size,
            base_sinker,
        }
    }
}

#[async_trait]
impl Sinker for MssqlSinker {
    async fn sink_dml(&mut self, _data: Vec<RowData>, _batch: bool) -> anyhow::Result<()> {
        todo!("mssql snapshot INSERT sinker is not implemented")
    }

    async fn sink_ddl(&mut self, _data: Vec<DdlData>, _batch: bool) -> anyhow::Result<()> {
        todo!("mssql DDL sink is not implemented")
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
