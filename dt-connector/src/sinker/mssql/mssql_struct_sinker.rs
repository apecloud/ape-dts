use async_trait::async_trait;
use dt_common::{
    config::config_enums::ConflictPolicyEnum,
    log_error, log_info,
    meta::{
        mssql::mssql_connection_pool::{MssqlClient, MssqlConnectionPool},
        struct_meta::struct_data::StructData,
    },
    rdb_filter::RdbFilter,
    utils::limit_queue::LimitedQueue,
};
use tokio::time::Instant;

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
    async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        let mut rts = LimitedQueue::new(data.len().clamp(1, 100));
        let mut struct_count = 0_u64;

        for mut struct_data in data {
            let sqls = struct_data
                .statement
                .to_sqls(&self.filter)?
                .into_iter()
                .map(|(_, sql)| sql)
                .collect::<Vec<_>>();
            if sqls.is_empty() {
                continue;
            }
            struct_count += 1;

            match self.conflict_policy {
                ConflictPolicyEnum::Interrupt => {
                    let start = Instant::now();
                    self.execute_sqls(&sqls).await?;
                    rts.push((start.elapsed().as_millis() as u64, sqls.len() as u64));
                }
                ConflictPolicyEnum::Ignore => {
                    for sql in sqls {
                        let start = Instant::now();
                        let result = self.execute_sqls(std::slice::from_ref(&sql)).await;
                        if let Err(error) = result {
                            log_error!("ddl ignored, error: {}", error);
                        }
                        rts.push((start.elapsed().as_millis() as u64, 1));
                    }
                }
            }
        }

        if struct_count > 0 {
            self.base_sinker
                .update_serial_monitor(struct_count, 0)
                .await?;
            self.base_sinker.update_monitor_rt(&rts).await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl MssqlStructSinker {
    async fn execute_sqls(&self, sqls: &[String]) -> anyhow::Result<()> {
        let mut connection = self.connection_pool.get().await?;
        for sql in sqls {
            log_info!("ddl begin: {}", sql);
            Self::execute(connection.client_mut(), sql).await?;
            log_info!("ddl succeed");
        }
        Ok(())
    }

    async fn execute(client: &mut MssqlClient, sql: &str) -> anyhow::Result<()> {
        client.simple_query(sql).await?.into_results().await?;
        Ok(())
    }
}
