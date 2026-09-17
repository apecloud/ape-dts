use std::cmp;

use anyhow::bail;
use dt_common::{
    config::config_enums::ConflictPolicyEnum,
    error::{classify_sqlx_error, ErrorCode},
    log_error, log_info, log_warn,
    meta::struct_meta::struct_data::StructData,
    rdb_filter::RdbFilter,
    utils::limit_queue::LimitedQueue,
};
use sqlx::{query, MySql, Pool, Postgres};
use tokio::time::Instant;

use crate::sinker::base_sinker::BaseSinker;

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
                        // Struct sync is intentionally re-runnable: a table is created with
                        // CREATE TABLE IF NOT EXISTS, so a re-run must also tolerate indexes and
                        // constraints that already exist (e.g. MySQL 1061 / 1826) instead of
                        // failing the whole task. Structural equivalence is verified separately
                        // by the struct checker, so tolerating "already exists" here does not
                        // mask a real schema mismatch.
                        if is_already_exists_error(&error) {
                            log_warn!("ddl skipped: object already exists, error: {}", error);
                        } else {
                            log_error!("ddl failed, error: {}", error);
                            match conflict_policy {
                                ConflictPolicyEnum::Interrupt => bail! {error},
                                ConflictPolicyEnum::Ignore => {}
                            }
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
                Err(error) => {
                    bail! {error}
                }
            },
            DBConnPool::PostgreSQL(pool) => match query(sql).execute(pool).await {
                Ok(_) => Ok(()),
                Err(error) => {
                    bail! {error}
                }
            },
        }
    }
}

/// Returns true when the provider rejected a DDL because the object it creates already
/// exists. Structure sync is re-runnable, so this condition is benign and must not fail
/// the task.
fn is_already_exists_error(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<sqlx::Error>()
        .map(|error| {
            classify_sqlx_error(error).error_code() == Some(ErrorCode::ObjectAlreadyExists)
        })
        .unwrap_or(false)
}
