use std::sync::Arc;

use anyhow::{bail, Result};
use sqlx::{MySql, Pool, Postgres};
use strum::{Display, EnumString, IntoStaticStr};
use tokio::time::Instant;

use crate::extractor::resumer::{
    recorder::{to_database::DatabaseRecorder, Recorder},
    recovery::{from_database::DatabaseRecovery, from_log::LogRecovery, Recovery},
};
use dt_common::{
    config::{config_enums::TaskType, resumer_config::ResumerConfig},
    log_info, log_warn,
    meta::position::Position,
};
pub mod recorder;
pub mod recovery;
pub mod utils;

const CURRENT_POSITION_LOG_FLAG: &str = "| current_position |";
const TAIL_POSITION_COUNT: usize = 200;
const DEFAULT_RESUMER_SCHEMA: &str = "apecloud_metadata";
const DEFAULT_RESUMER_TABLE: &str = "apedts_task_position";
const DEFAULT_POSITION_KEY: &str = "default_key";

#[derive(Clone, Debug)]
pub enum ResumerDbPool {
    MySql(Pool<MySql>),
    Postgres(Pool<Postgres>),
    // TODO: add more database types here in the future
}

#[derive(Clone, Display, EnumString, IntoStaticStr, Debug, PartialEq, Eq)]
pub enum ResumerType {
    SnapshotDoing,
    SnapshotFinished,
    CdcDoing,
    NotSupported,
}

impl ResumerType {
    pub fn from_position(position: &Position) -> Self {
        match position {
            Position::RdbSnapshot { .. } | Position::FoxlakeS3 { .. } => Self::SnapshotDoing,
            Position::RdbSnapshotFinished { .. } => Self::SnapshotFinished,
            Position::MysqlCdc { .. }
            | Position::PgCdc { .. }
            | Position::MongoCdc { .. }
            | Position::Redis { .. }
            | Position::Kafka { .. } => Self::CdcDoing,
            _ => Self::NotSupported,
        }
    }
}

pub async fn build_recorder(
    task_id: &str,
    resumer_config: &ResumerConfig,
    pool: Option<ResumerDbPool>,
) -> Result<Option<Arc<dyn Recorder + Send + Sync>>> {
    match resumer_config {
        ResumerConfig::FromDB { .. } => {
            let pool_inner = match pool {
                Some(p) => p,
                None => bail!("pool is required for FromDB resumer config"),
            };
            let recorder = DatabaseRecorder::new(task_id, resumer_config, pool_inner).await?;
            Ok(Some(Arc::new(recorder)))
        }
        _ => {
            log_warn!("recorder unsupported resumer config: {:?}", resumer_config);
            Ok(None)
        }
    }
}

pub async fn build_recovery(
    task_id: &str,
    task_type: TaskType,
    resumer_config: &ResumerConfig,
    pool: Option<ResumerDbPool>,
) -> Result<Option<Arc<dyn Recovery + Send + Sync>>> {
    let begin = Instant::now();
    let result: Option<Arc<dyn Recovery + Send + Sync>> = match resumer_config {
        ResumerConfig::FromDB { .. } => {
            let pool_inner = match pool {
                Some(p) => p,
                None => bail!("pool is required for FromDB resumer config"),
            };
            let recovery = DatabaseRecovery::new(task_id, resumer_config, pool_inner).await?;
            Some(Arc::new(recovery))
        }
        ResumerConfig::FromLog { .. } => {
            let recovery = LogRecovery::new(task_type, resumer_config).await?;
            Some(Arc::new(recovery))
        }
        _ => {
            log_warn!("recovery unsupported resumer config: {:?}", resumer_config);
            None
        }
    };
    log_info!(
        "recovery initialization for task_id: {} finished in {:?} ms",
        task_id,
        begin.elapsed().as_millis()
    );
    Ok(result)
}
