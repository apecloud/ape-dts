use std::sync::Arc;

use anyhow::{bail, Context};
use async_trait::async_trait;
use tokio::{sync::Mutex, task::JoinSet};
use url::Url;

use crate::{
    extractor::{
        base_extractor::{BaseExtractor, ExtractState},
        extractor_monitor::ExtractorMonitor,
        redis::{
            redis_client::RedisClient,
            redis_psync_extractor::{
                RedisClusterPositionTracker, RedisPsyncExtractor, RedisPsyncNode,
            },
        },
        resumer::recovery::Recovery,
    },
    Extractor,
};
use dt_common::{
    config::{config_enums::ExtractType, connection_auth_config::ConnectionAuthConfig},
    error::Error,
    log_info, log_warn,
    meta::{
        position::{Position, RedisNodePosition},
        redis::cluster_node::ClusterNode,
        syncer::Syncer,
    },
    rdb_filter::RdbFilter,
    utils::redis_util::RedisUtil,
};

pub struct RedisClusterPsyncExtractor {
    pub base_extractor: BaseExtractor,
    pub extract_state: ExtractState,
    pub url: String,
    pub connection_auth: ConnectionAuthConfig,
    pub repl_port: u64,
    pub keepalive_interval_secs: u64,
    pub heartbeat_interval_secs: u64,
    pub heartbeat_key: String,
    pub syncer: Arc<Mutex<Syncer>>,
    pub filter: RdbFilter,
    pub extract_type: ExtractType,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

#[async_trait]
impl Extractor for RedisClusterPsyncExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        log_info!("RedisClusterPsyncExtractor starts");

        let nodes = self.get_cluster_master_nodes().await?;
        let recovery_position = self.get_recovery_position().await;
        let recovered_positions =
            Self::matched_recovery_positions(&nodes, recovery_position.as_ref());
        let tracker = RedisClusterPositionTracker::from_positions(recovered_positions.clone());

        let mut join_set = JoinSet::new();
        for node in nodes {
            let node_position = recovered_positions
                .iter()
                .find(|position| position.node_id == node.id)
                .cloned();
            let mut extractor = self
                .build_node_extractor(node, node_position, tracker.clone())
                .await?;
            join_set.spawn(async move { extractor.extract().await });
        }

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    self.base_extractor
                        .shut_down
                        .store(true, std::sync::atomic::Ordering::Release);
                    bail!(err);
                }
                Err(err) => {
                    self.base_extractor
                        .shut_down
                        .store(true, std::sync::atomic::Ordering::Release);
                    bail!(Error::ExtractorError(format!(
                        "redis cluster psync task failed: {err}"
                    )));
                }
            }
        }

        self.base_extractor
            .wait_task_finish(&mut self.extract_state)
            .await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl RedisClusterPsyncExtractor {
    async fn get_cluster_master_nodes(&self) -> anyhow::Result<Vec<ClusterNode>> {
        let mut conn = RedisUtil::create_redis_conn(&self.url, &self.connection_auth).await?;
        let nodes = RedisUtil::get_cluster_master_nodes(&mut conn)?;
        if nodes.is_empty() {
            bail!(Error::MetadataError(
                "source redis cluster has no master nodes".into()
            ));
        }
        Ok(nodes)
    }

    async fn get_recovery_position(&self) -> Option<Position> {
        let Some(recovery) = &self.recovery else {
            return None;
        };
        let position = recovery.get_cdc_resume_position().await;
        match &position {
            Some(Position::RedisCluster { .. }) => position,
            Some(position) => {
                log_warn!(
                    "position:{} is not a valid redis cluster psync position",
                    position
                );
                None
            }
            None => None,
        }
    }

    fn match_node_position(
        node: &ClusterNode,
        recovery_position: Option<&Position>,
    ) -> Option<RedisNodePosition> {
        let Some(Position::RedisCluster { nodes, .. }) = recovery_position else {
            return None;
        };

        nodes
            .iter()
            .find(|position| position.node_id == node.id)
            .or_else(|| {
                nodes
                    .iter()
                    .find(|position| position.address == node.address)
            })
            .cloned()
    }

    fn matched_recovery_positions(
        nodes: &[ClusterNode],
        recovery_position: Option<&Position>,
    ) -> Vec<RedisNodePosition> {
        let mut positions = Vec::new();
        for node in nodes {
            if let Some(mut position) = Self::match_node_position(node, recovery_position) {
                position.node_id = node.id.clone();
                position.address = node.address.clone();
                positions.push(position);
            }
        }
        positions
    }

    async fn build_node_extractor(
        &self,
        node: ClusterNode,
        position: Option<RedisNodePosition>,
        tracker: RedisClusterPositionTracker,
    ) -> anyhow::Result<RedisPsyncExtractor> {
        let node_url = Self::node_url(&self.url, &node)?;
        let node_state = self.derive_node_state().await;

        let (repl_id, repl_offset, now_db_id) = if let Some(position) = position {
            log_info!(
                "redis cluster psync recovery node_id:[{}], address:[{}], repl_id:[{}], repl_offset:[{}], repl_port:[{}], now_db_id:[{}]",
                position.node_id,
                position.address,
                position.repl_id,
                position.repl_offset,
                position.repl_port,
                position.now_db_id
            );
            (position.repl_id, position.repl_offset, position.now_db_id)
        } else {
            (String::new(), 0, 0)
        };

        let heartbeat_hash_tag = node.slot_hash_tag_map.values().next().cloned();

        Ok(RedisPsyncExtractor {
            conn: RedisClient::new(&node_url, &self.connection_auth).await?,
            syncer: self.syncer.clone(),
            repl_port: self.repl_port,
            filter: self.filter.clone(),
            base_extractor: self.base_extractor.clone(),
            extract_state: node_state,
            extract_type: self.extract_type.clone(),
            repl_id,
            repl_offset,
            now_db_id,
            keepalive_interval_secs: self.keepalive_interval_secs,
            heartbeat_interval_secs: self.heartbeat_interval_secs,
            heartbeat_key: self.heartbeat_key.clone(),
            recovery: None,
            cluster_node: Some(RedisPsyncNode {
                id: node.id,
                address: node.address,
                heartbeat_hash_tag,
            }),
            cluster_position_tracker: Some(tracker),
            wait_task_finish: false,
        })
    }

    async fn derive_node_state(&self) -> ExtractState {
        let monitor = ExtractorMonitor::new(
            self.extract_state.monitor.monitor.clone(),
            self.extract_state.monitor.default_task_id.clone(),
        )
        .await;
        self.extract_state
            .derive_for_table(monitor, self.extract_state.data_marker.clone())
    }

    fn node_url(base_url: &str, node: &ClusterNode) -> anyhow::Result<String> {
        let mut url = Url::parse(base_url)?;
        url.set_host(Some(&node.host)).map_err(|_| {
            Error::ConfigError(format!("invalid redis cluster node host: {}", node.host))
        })?;
        url.set_port(Some(node.port.parse().with_context(|| {
            format!("invalid redis cluster node port: {}", node.port)
        })?))
        .map_err(|_| {
            Error::ConfigError(format!("invalid redis cluster node port: {}", node.port))
        })?;
        Ok(url.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dt_common::meta::position::{Position, RedisNodePosition};

    use super::RedisClusterPsyncExtractor;
    use crate::extractor::redis::redis_psync_extractor::RedisClusterPositionTracker;
    use dt_common::meta::redis::cluster_node::ClusterNode;

    fn node(id: &str, address: &str) -> ClusterNode {
        let (host, port) = address.split_once(':').unwrap();
        ClusterNode {
            is_master: true,
            id: id.to_string(),
            master_id: "-".to_string(),
            host: host.to_string(),
            port: port.to_string(),
            address: address.to_string(),
            slots: vec![],
            slot_hash_tag_map: HashMap::new(),
        }
    }

    fn position(node_id: &str, address: &str, repl_offset: u64) -> RedisNodePosition {
        RedisNodePosition {
            node_id: node_id.to_string(),
            address: address.to_string(),
            repl_id: format!("repl-{node_id}"),
            repl_port: 10008,
            repl_offset,
            now_db_id: 0,
            timestamp: String::new(),
        }
    }

    #[test]
    fn match_node_position_prefers_node_id_then_address() {
        let recovery_position = Position::RedisCluster {
            nodes: vec![
                position("old-id", "127.0.0.1:6371", 10),
                position("node-2", "127.0.0.1:6372", 20),
            ],
            timestamp: String::new(),
        };

        let id_match = RedisClusterPsyncExtractor::match_node_position(
            &node("node-2", "other:6379"),
            Some(&recovery_position),
        )
        .unwrap();
        assert_eq!(id_match.repl_offset, 20);

        let address_match = RedisClusterPsyncExtractor::match_node_position(
            &node("new-id", "127.0.0.1:6371"),
            Some(&recovery_position),
        )
        .unwrap();
        assert_eq!(address_match.repl_offset, 10);
    }

    #[test]
    fn matched_recovery_positions_use_current_cluster_nodes() {
        let recovery_position = Position::RedisCluster {
            nodes: vec![
                position("old-id", "127.0.0.1:6371", 10),
                position("node-2", "127.0.0.1:6372", 20),
                position("removed-node", "127.0.0.1:6399", 99),
            ],
            timestamp: String::new(),
        };
        let current_nodes = vec![
            node("node-1", "127.0.0.1:6371"),
            node("node-2", "127.0.0.1:6372"),
        ];

        let positions = RedisClusterPsyncExtractor::matched_recovery_positions(
            &current_nodes,
            Some(&recovery_position),
        );

        assert_eq!(positions.len(), 2);
        assert!(positions.iter().any(|position| position.node_id == "node-1"
            && position.address == "127.0.0.1:6371"
            && position.repl_offset == 10));
        assert!(positions.iter().any(|position| position.node_id == "node-2"
            && position.address == "127.0.0.1:6372"
            && position.repl_offset == 20));
    }

    #[tokio::test]
    async fn seeded_tracker_keeps_quiet_shard_positions_after_update() {
        let tracker = RedisClusterPositionTracker::from_positions(vec![
            position("node-1", "127.0.0.1:6371", 10),
            position("node-2", "127.0.0.1:6372", 20),
            position("node-3", "127.0.0.1:6373", 30),
        ]);

        let updated = tracker
            .update(position("node-2", "127.0.0.1:6372", 25))
            .await;

        let Position::RedisCluster { nodes, .. } = updated else {
            panic!("expected redis cluster position");
        };
        assert_eq!(nodes.len(), 3);
        assert_eq!(
            nodes
                .iter()
                .find(|position| position.node_id == "node-1")
                .unwrap()
                .repl_offset,
            10
        );
        assert_eq!(
            nodes
                .iter()
                .find(|position| position.node_id == "node-2")
                .unwrap()
                .repl_offset,
            25
        );
        assert_eq!(
            nodes
                .iter()
                .find(|position| position.node_id == "node-3")
                .unwrap()
                .repl_offset,
            30
        );
    }
}
