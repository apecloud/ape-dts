use std::cmp;

use anyhow::bail;
use sqlx::{query, MySql, Pool, Postgres};
use tokio::time::Instant;

use crate::sinker::base_sinker::BaseSinker;
use dt_common::{
    config::config_enums::ConflictPolicyEnum, error::Error, log_error, log_info,
    meta::struct_meta::struct_data::StructData, rdb_filter::RdbFilter,
    utils::limit_queue::LimitedQueue,
};

pub struct BaseStructSinker {}

pub enum DBConnPool {
    MySQL(Pool<MySql>),
    PostgreSQL(Pool<Postgres>),
}

impl BaseStructSinker {
    pub async fn sink_structs(
        conn_pool: &DBConnPool,
        conflict_policy: &ConflictPolicyEnum,
        data: Vec<StructData>,
        filter: &RdbFilter,
        base_sinker: &BaseSinker,
    ) -> anyhow::Result<()> {
        let monitor_interval_secs = base_sinker.monitor_interval_secs();
        let mut rts = LimitedQueue::new(cmp::min(100, data.len()));
        let mut last_monitor_time = Instant::now();

        let mut data_len = 0;
        for mut struct_data in data {
            data_len += 1;
            for (_, sql) in struct_data.statement.to_sqls(filter)?.iter() {
                log_info!("ddl begin: {}", sql);
                let start_time = Instant::now();
                match Self::execute(conn_pool, sql).await {
                    Ok(()) => {
                        log_info!("ddl succeed");
                    }

                    Err(error) => {
                        log_error!("ddl failed, error: {}", error);
                        match conflict_policy {
                            ConflictPolicyEnum::Interrupt => bail! {error},
                            ConflictPolicyEnum::Ignore => {}
                        }
                    }
                }
                rts.push((start_time.elapsed().as_millis() as u64, 1));
                if last_monitor_time.elapsed().as_secs() >= monitor_interval_secs {
                    base_sinker
                        .update_serial_monitor(data_len as u64, 0)
                        .await?;
                    base_sinker.update_monitor_rt(&rts).await?;
                    rts.clear();
                    data_len = 0;
                    last_monitor_time = Instant::now();
                }
            }
        }

        if data_len > 0 {
            base_sinker
                .update_serial_monitor(data_len as u64, 0)
                .await?;
            base_sinker.update_monitor_rt(&rts).await?;
        }
        Ok(())
    }

    async fn execute(pool: &DBConnPool, sql: &str) -> anyhow::Result<()> {
        match pool {
            DBConnPool::MySQL(pool) => match query(sql).execute(pool).await {
                Ok(_) => Ok(()),
                Err(error) => bail! {Error::SqlxError(error)},
            },
            DBConnPool::PostgreSQL(pool) => match query(sql).execute(pool).await {
                Ok(_) => Ok(()),
                Err(error) => bail! {Error::SqlxError(error)},
            },
        }
    }
}
