use std::sync::Arc;

use async_trait::async_trait;
use sqlx::{MySql, Pool};

use crate::{
    close_conn_pool,
    rdb_router::RdbRouter,
    sinker::base_struct_sinker::{BaseStructSinker, DBConnPool},
    Sinker,
};
use dt_common::{
    config::config_enums::ConflictPolicyEnum,
    meta::struct_meta::{statement::struct_statement::StructStatement, struct_data::StructData},
    monitor::monitor::Monitor,
    rdb_filter::RdbFilter,
};

#[derive(Clone)]
pub struct FoxlakeStructSinker {
    pub conn_pool: Pool<MySql>,
    pub conflict_policy: ConflictPolicyEnum,
    pub filter: RdbFilter,
    pub router: RdbRouter,
    pub engine: String,
    pub monitor: Arc<Monitor>,
    pub monitor_interval: u64,
}

#[async_trait]
impl Sinker for FoxlakeStructSinker {
    async fn sink_struct(&mut self, mut data: Vec<StructData>) -> anyhow::Result<()> {
        for struct_data in data.iter_mut() {
            match &mut struct_data.statement {
                StructStatement::MysqlCreateTable(s) => {
                    s.table.table_collation = String::new();
                    s.table.engine_name = format!("'{}'", self.engine);
                    for column in s.table.columns.iter_mut() {
                        column.collation_name = String::new();
                    }
                }

                StructStatement::MysqlCreateDatabase(s) => {
                    s.database.default_collation_name = String::new();
                }

                _ => {}
            }
        }

        BaseStructSinker::sink_structs(
            &DBConnPool::MySQL(self.conn_pool.clone()),
            &self.conflict_policy,
            data,
            &self.filter,
            &self.monitor,
            self.monitor_interval,
        )
        .await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        return close_conn_pool!(self);
    }
}
