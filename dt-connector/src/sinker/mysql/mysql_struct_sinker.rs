use async_trait::async_trait;
use sqlx::{MySql, Pool};

use crate::{
    rdb_router::RdbRouter,
    sinker::{
        base_sinker::BaseSinker,
        base_struct_sinker::{BaseStructSinker, DBConnPool},
        sqlx_error,
    },
    Sinker,
};
use dt_common::{
    config::config_enums::ConflictPolicyEnum, error::ErrorCode,
    meta::struct_meta::struct_data::StructData, rdb_filter::RdbFilter,
};

#[derive(Clone)]
pub struct MysqlStructSinker {
    pub conn_pool: Pool<MySql>,
    pub conflict_policy: ConflictPolicyEnum,
    pub filter: RdbFilter,
    pub router: Option<RdbRouter>,
    pub base_sinker: BaseSinker,
}

#[async_trait]
impl Sinker for MysqlStructSinker {
    async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        BaseStructSinker::sink_structs(
            &DBConnPool::MySQL(self.conn_pool.clone()),
            &self.conflict_policy,
            data,
            &self.filter,
            &self.base_sinker,
        )
        .await
        .map_err(|error| {
            sqlx_error::mysql_from_anyhow(error, ErrorCode::StatementFailed, "sink_struct")
        })
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
