use std::{sync::atomic::AtomicBool, sync::Arc};

use async_trait::async_trait;
use tokio::sync::Mutex;
use url::Url;

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
    meta::{dt_queue::DtQueue, redis::cluster_node::ClusterNode, syncer::Syncer},
    monitor::{task_monitor::MonitorType, task_monitor_handle::TaskMonitorHandle},
    rdb_filter::RdbFilter,
    time_filter::TimeFilter,
    utils::redis_util::RedisUtil,
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

#[derive(Debug, PartialEq, Eq)]
enum RedisCdcPrecheckMode {
    ClusterNodePsync,
    SingleNodePsync,
}

fn redis_cdc_precheck_mode(is_cluster: bool) -> RedisCdcPrecheckMode {
    if is_cluster {
        RedisCdcPrecheckMode::ClusterNodePsync
    } else {
        RedisCdcPrecheckMode::SingleNodePsync
    }
}

fn redis_cluster_psync_url(base_url: &str, nodes: &[ClusterNode]) -> anyhow::Result<String> {
    let node = nodes
        .first()
        .ok_or_else(|| anyhow::anyhow!("source redis cluster has no master nodes"))?;

    let mut url = Url::parse(base_url)?;
    url.set_host(Some(&node.host))
        .map_err(|_| anyhow::anyhow!("invalid redis cluster node host: {}", node.host))?;
    url.set_port(Some(node.port.parse()?))
        .map_err(|_| anyhow::anyhow!("invalid redis cluster node port: {}", node.port))?;
    Ok(url.to_string())
}

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
        let precheck_mode = {
            let conn = self
                .fetcher
                .conn
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("redis connection is not initialized"))?;
            redis_cdc_precheck_mode(RedisUtil::is_redis_cluster(conn))
        };

        let psync_url = if let RedisCdcPrecheckMode::ClusterNodePsync = precheck_mode {
            let conn = self
                .fetcher
                .conn
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("redis connection is not initialized"))?;
            match RedisUtil::get_cluster_master_nodes(conn)
                .and_then(|nodes| redis_cluster_psync_url(&self.fetcher.url, &nodes))
            {
                Ok(url) => url,
                Err(error) => {
                    return Ok(CheckResult::build_with_err(
                        CheckItem::CheckAccountPermission,
                        self.is_source,
                        DbType::Redis,
                        Some(error),
                        None,
                    ));
                }
            }
        } else {
            self.fetcher.url.clone()
        };

        let buffer = Arc::new(DtQueue::new(1, 0, None, None));

        let filter = RdbFilter::from_config(&self.task_config.filter, &DbType::Redis)?;
        let monitor = TaskMonitorHandle::noop(MonitorType::Extractor);

        let base_extractor = BaseExtractor {
            buffer,
            router: RdbRouter::from_config(&self.task_config.router, &DbType::Redis)?,
            shut_down: Arc::new(AtomicBool::new(false)),
        };
        let extract_state = ExtractState {
            monitor: ExtractorMonitor::new(monitor, String::new()).await,
            data_marker: None,
            time_filter: TimeFilter::default(),
        };

        let mut psyncer = RedisPsyncExtractor {
            conn: RedisClient::new(&psync_url, &self.fetcher.connection_auth).await?,
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
            cluster_node: None,
            wait_task_finish: true,
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

#[cfg(test)]
mod tests {
    use super::*;
    use dt_common::meta::redis::cluster_node::ClusterNode;

    fn cluster_node(address: &str) -> ClusterNode {
        let (host, port) = address.split_once(':').unwrap();
        ClusterNode {
            is_master: true,
            id: "node-1".to_string(),
            master_id: "-".to_string(),
            host: host.to_string(),
            port: port.to_string(),
            address: address.to_string(),
            slots: vec![],
            slot_hash_tag_map: Default::default(),
        }
    }

    #[test]
    fn cluster_cdc_precheck_uses_cluster_master_node_as_psync_target() {
        assert_eq!(
            redis_cdc_precheck_mode(true),
            RedisCdcPrecheckMode::ClusterNodePsync
        );

        let nodes = vec![cluster_node("10.0.0.2:6380")];
        let psync_url = redis_cluster_psync_url("redis://user:pass@10.0.0.1:6379/0", &nodes)
            .expect("cluster node should become psync url");

        assert_eq!(psync_url, "redis://user:pass@10.0.0.2:6380/0");
    }
}
