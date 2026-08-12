use async_trait::async_trait;
use dt_common::{
    config::config_enums::ConflictPolicyEnum,
    meta::{
        mssql::mssql_connection_pool::MssqlConnectionPool, struct_meta::struct_data::StructData,
    },
    rdb_filter::RdbFilter,
};

use crate::{rdb_router::RdbRouter, sinker::base_sinker::BaseSinker, Sinker};

#[derive(Clone)]
pub struct MssqlStructSinker {
    pub connection_pool: MssqlConnectionPool,
    pub conflict_policy: ConflictPolicyEnum,
    pub filter: RdbFilter,
    pub router: Option<RdbRouter>,
    pub base_sinker: BaseSinker,
}

#[async_trait]
impl Sinker for MssqlStructSinker {
    async fn sink_struct(&mut self, _data: Vec<StructData>) -> anyhow::Result<()> {
        todo!("execute MSSQL struct statements with conflict policy handling")
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
