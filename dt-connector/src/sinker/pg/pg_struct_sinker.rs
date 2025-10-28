use std::sync::Arc;

use async_trait::async_trait;
use sqlx::{Pool, Postgres};

use crate::{
    rdb_router::RdbRouter,
    sinker::base_struct_sinker::{BaseStructSinker, DBConnPool},
    Sinker,
};
use dt_common::{
    config::config_enums::ConflictPolicyEnum, meta::struct_meta::struct_data::StructData,
    monitor::monitor::Monitor, rdb_filter::RdbFilter,
};

#[derive(Clone)]
pub struct PgStructSinker {
    pub conn_pool: Pool<Postgres>,
    pub conflict_policy: ConflictPolicyEnum,
    pub filter: RdbFilter,
    pub router: RdbRouter,
    pub monitor: Arc<Monitor>,
    pub monitor_interval: u64,
}

#[async_trait]
impl Sinker for PgStructSinker {
    async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        BaseStructSinker::sink_structs(
            &DBConnPool::PostgreSQL(self.conn_pool.clone()),
            &self.conflict_policy,
            data,
            &self.filter,
            &self.monitor,
            self.monitor_interval,
        )
        .await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl PgStructSinker {}
