use anyhow::Context;
use async_trait::async_trait;
use dt_common::router::DtRouter;
use dt_common::{
    config::config_enums::ConflictPolicyEnum, filter::DtFilter,
    meta::struct_meta::struct_data::StructData,
};
use sqlx::{Pool, Postgres};

use crate::{
    sinker::{
        base_sinker::BaseSinker,
        base_struct_sinker::{BaseStructSinker, DBConnPool},
    },
    Sinker,
};

#[derive(Clone)]
pub struct PgStructSinker {
    pub conn_pool: Pool<Postgres>,
    pub conflict_policy: ConflictPolicyEnum,
    pub filter: DtFilter,
    pub router: Option<DtRouter>,
    pub base_sinker: BaseSinker,
}

#[async_trait]
impl Sinker for PgStructSinker {
    async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        BaseStructSinker::sink_structs(
            &DBConnPool::PostgreSQL(self.conn_pool.clone()),
            &self.conflict_policy,
            data,
            &self.filter,
            &self.base_sinker,
        )
        .await
        .context("sink_struct")
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl PgStructSinker {}
