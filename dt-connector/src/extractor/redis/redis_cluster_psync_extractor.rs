use std::sync::{atomic::Ordering, Arc};

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
            redis_psync_extractor::{RedisPsyncExtractor, RedisPsyncNode},
        },
        resumer::recovery::Recovery,
    },
    Extractor,
};
use dt_common::{
    config::{config_enums::ExtractType, connection_auth_config::ConnectionAuthConfig},
    error::Error,
    log_info, log_warn,
    meta::{position::Position, redis::cluster_node::ClusterNode, syncer::Syncer},
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
        let recovery_positions = self.get_recovery_positions().await;
        let recovered_positions = Self::matched_recovery_positions(&nodes, &recovery_positions);

        let mut join_set = JoinSet::new();
        for node in nodes {
            let node_position = Self::match_node_position(&node, &recovered_positions);
            let mut extractor = self.build_node_extractor(node, node_position).await?;
            join_set.spawn(async move { extractor.extract().await });
        }

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    self.base_extractor.shut_down.store(true, Ordering::Release);
                    bail!(err);
                }
                Err(err) => {
                    self.base_extractor.shut_down.store(true, Ordering::Release);
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

    async fn get_recovery_positions(&self) -> Vec<Position> {
        let Some(recovery) = &self.recovery else {
            return Vec::new();
        };

        let positions = recovery.get_cdc_resume_positions().await;
        let mut nodes = Vec::new();
        for position in positions {
            match position {
                Position::Redis {
                    node_id: Some(_),
                    address: Some(_),
                    ..
                } => nodes.push(position),
                position => {
                    log_warn!(
                        "position:{} is not a valid redis cluster psync position",
                        position
                    );
                }
            }
        }
        nodes
    }

    fn match_node_position(
        node: &ClusterNode,
        recovery_positions: &[Position],
    ) -> Option<Position> {
        recovery_positions
            .iter()
            .find(|position| match position {
                Position::Redis { node_id, .. } => node_id.as_deref() == Some(node.id.as_str()),
                _ => false,
            })
            .or_else(|| {
                recovery_positions.iter().find(|position| match position {
                    Position::Redis { address, .. } => {
                        address.as_deref() == Some(node.address.as_str())
                    }
                    _ => false,
                })
            })
            .cloned()
    }

    fn matched_recovery_positions(
        nodes: &[ClusterNode],
        recovery_positions: &[Position],
    ) -> Vec<Position> {
        let mut positions = Vec::new();
        for node in nodes {
            if let Some(Position::Redis {
                repl_id,
                repl_port,
                repl_offset,
                now_db_id,
                timestamp,
                ..
            }) = Self::match_node_position(node, recovery_positions)
            {
                positions.push(Position::Redis {
                    node_id: Some(node.id.clone()),
                    address: Some(node.address.clone()),
                    repl_id,
                    repl_port,
                    repl_offset,
                    now_db_id,
                    timestamp,
                });
            }
        }
        positions
    }

    async fn build_node_extractor(
        &self,
        node: ClusterNode,
        position: Option<Position>,
    ) -> anyhow::Result<RedisPsyncExtractor> {
        let node_url = Self::node_url(&self.url, &node)?;
        let node_state = self.derive_node_state().await;

        let (repl_id, repl_offset, now_db_id) = if let Some(Position::Redis {
            node_id,
            address,
            repl_id,
            repl_offset,
            repl_port,
            now_db_id,
            ..
        }) = position
        {
            log_info!(
                "redis cluster psync recovery node_id:[{}], address:[{}], repl_id:[{}], repl_offset:[{}], repl_port:[{}], now_db_id:[{}]",
                node_id.unwrap_or_default(),
                address.unwrap_or_default(),
                repl_id,
                repl_offset,
                repl_port,
                now_db_id
            );
            (repl_id, repl_offset, now_db_id)
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

    use dt_common::meta::position::Position;

    use super::RedisClusterPsyncExtractor;
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

    fn position(node_id: &str, address: &str, repl_offset: u64) -> Position {
        Position::Redis {
            node_id: Some(node_id.to_string()),
            address: Some(address.to_string()),
            repl_id: format!("repl-{node_id}"),
            repl_port: 10008,
            repl_offset,
            now_db_id: 0,
            timestamp: String::new(),
        }
    }

    fn redis_fields(position: &Position) -> (&str, &str, u64) {
        let Position::Redis {
            node_id,
            address,
            repl_offset,
            ..
        } = position
        else {
            panic!("expected redis position");
        };
        (
            node_id.as_deref().unwrap_or_default(),
            address.as_deref().unwrap_or_default(),
            *repl_offset,
        )
    }

    #[test]
    fn match_node_position_prefers_node_id_then_address() {
        let recovery_positions = vec![
            position("old-id", "127.0.0.1:6371", 10),
            position("node-2", "127.0.0.1:6372", 20),
        ];

        let id_match = RedisClusterPsyncExtractor::match_node_position(
            &node("node-2", "other:6379"),
            &recovery_positions,
        )
        .unwrap();
        assert_eq!(redis_fields(&id_match).2, 20);

        let address_match = RedisClusterPsyncExtractor::match_node_position(
            &node("new-id", "127.0.0.1:6371"),
            &recovery_positions,
        )
        .unwrap();
        assert_eq!(redis_fields(&address_match).2, 10);
    }

    #[test]
    fn matched_recovery_positions_use_current_cluster_nodes() {
        let recovery_positions = vec![
            position("old-id", "127.0.0.1:6371", 10),
            position("node-2", "127.0.0.1:6372", 20),
            position("removed-node", "127.0.0.1:6399", 99),
        ];
        let current_nodes = vec![
            node("node-1", "127.0.0.1:6371"),
            node("node-2", "127.0.0.1:6372"),
        ];

        let positions = RedisClusterPsyncExtractor::matched_recovery_positions(
            &current_nodes,
            &recovery_positions,
        );

        assert_eq!(positions.len(), 2);
        assert!(positions.iter().any(|position| {
            let (node_id, address, repl_offset) = redis_fields(position);
            node_id == "node-1" && address == "127.0.0.1:6371" && repl_offset == 10
        }));
        assert!(positions.iter().any(|position| {
            let (node_id, address, repl_offset) = redis_fields(position);
            node_id == "node-2" && address == "127.0.0.1:6372" && repl_offset == 20
        }));
    }
}
