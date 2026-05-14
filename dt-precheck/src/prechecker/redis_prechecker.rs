use std::{sync::atomic::AtomicBool, sync::Arc};

use async_trait::async_trait;
use tokio::sync::Mutex;

use super::traits::Prechecker;
use crate::{
    config::precheck_config::PrecheckConfig,
    fetcher::{redis::redis_fetcher::RedisFetcher, traits::Fetcher},
    meta::{check_item::CheckItem, check_result::CheckResult},
};
use dt_common::{
    config::{
        config_enums::{DbType, ExtractType},
        extractor_config::ExtractorConfig,
        task_config::TaskConfig,
    },
    meta::{dt_ctl_queue::DtCtlQueue, dt_queue::DtQueue, syncer::Syncer},
    monitor::{task_monitor::MonitorType, task_monitor_handle::TaskMonitorHandle},
    rdb_filter::RdbFilter,
    time_filter::TimeFilter,
};
use dt_connector::{
    extractor::{
        base_extractor::{BaseExtractor, ExtractState},
        extractor_monitor::ExtractorMonitor,
        redis::{redis_client::RedisClient, redis_psync_extractor::RedisPsyncExtractor},
    },
    rdb_router::RdbRouter,
};

pub struct RedisPrechecker {
    pub fetcher: RedisFetcher,
    pub task_config: TaskConfig,
    pub precheck_config: PrecheckConfig,
    pub is_source: bool,
}

const MIN_SUPPORTED_VERSION: f32 = 2.8;

#[async_trait]
impl Prechecker for RedisPrechecker {
    async fn build_connection(&mut self) -> anyhow::Result<CheckResult> {
        self.fetcher.build_connection().await?;
        Ok(CheckResult::build_with_err(
            CheckItem::CheckDatabaseConnection,
            self.is_source,
            DbType::Redis,
            None,
            None,
        ))
    }

    async fn check_database_version(&mut self) -> anyhow::Result<CheckResult> {
        let version = self.fetcher.fetch_version().await?;
        let version: f32 = version.parse().unwrap();
        let check_error = if version < MIN_SUPPORTED_VERSION {
            Some(anyhow::Error::msg(format!(
                "redis version:[{}] is NOT supported, the minimum supported version is {}.",
                version, MIN_SUPPORTED_VERSION
            )))
        } else {
            None
        };

        Ok(CheckResult::build_with_err(
            CheckItem::CheckDatabaseVersionSupported,
            self.is_source,
            DbType::Redis,
            check_error,
            None,
        ))
    }

    async fn check_cdc_supported(&mut self) -> anyhow::Result<CheckResult> {
        let repl_port = match self.task_config.extractor {
            ExtractorConfig::RedisCdc { repl_port, .. }
            | ExtractorConfig::RedisSnapshot { repl_port, .. } => repl_port,
            // should never happen since we've already checked the extractor type before into this function
            _ => 0,
        };
        let buffer = Arc::new(DtQueue::new(1, 0, None, None));
        let ctl_buffer = Arc::new(DtCtlQueue::new());

        let filter = RdbFilter::from_config(&self.task_config.filter, &DbType::Redis)?;
        let monitor = TaskMonitorHandle::noop(MonitorType::Extractor);

        let base_extractor = BaseExtractor {
            buffer,
            ctl_buffer,
            router: RdbRouter::from_config(&self.task_config.router, &DbType::Redis)?,
            shut_down: Arc::new(AtomicBool::new(false)),
        };
        let extract_state = ExtractState {
            monitor: ExtractorMonitor::new(monitor, String::new()).await,
            data_marker: None,
            time_filter: TimeFilter::default(),
        };

        let mut psyncer = RedisPsyncExtractor {
            conn: RedisClient::new(&self.fetcher.url, &self.fetcher.connection_auth).await?,
            repl_id: String::new(),
            repl_offset: 0,
            now_db_id: 0,
            repl_port,
            filter,
            base_extractor,
            extract_state,
            extract_type: ExtractType::Snapshot,
            syncer: Arc::new(Mutex::new(Syncer::default())),
            keepalive_interval_secs: 0,
            heartbeat_interval_secs: 0,
            heartbeat_key: String::new(),
            recovery: None,
        };

        if let Err(error) = psyncer.start_psync().await {
            return Ok(CheckResult::build_with_err(
                CheckItem::CheckAccountPermission,
                self.is_source,
                DbType::Redis,
                Some(error),
                None,
            ));
        } else {
            Ok(CheckResult::build(
                CheckItem::CheckAccountPermission,
                self.is_source,
            ))
        }
    }

    async fn check_permission(&mut self) -> anyhow::Result<CheckResult> {
        Ok(CheckResult::build(
            CheckItem::CheckAccountPermission,
            self.is_source,
        ))
    }

    async fn check_struct_existed_or_not(&mut self) -> anyhow::Result<CheckResult> {
        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfStructExisted,
            self.is_source,
            DbType::Redis,
            None,
            None,
        ))
    }

    async fn check_table_structs(&mut self) -> anyhow::Result<CheckResult> {
        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfTableStructSupported,
            self.is_source,
            DbType::Redis,
            None,
            None,
        ))
    }
}
