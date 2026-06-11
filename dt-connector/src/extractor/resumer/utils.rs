use std::{str::FromStr, time::Duration};

use anyhow::{bail, Context, Result};
use mongodb::options::ClientOptions;
use redis::Connection;
use serde::{Deserialize, Serialize};
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
    postgres::{PgConnectOptions, PgPoolOptions},
};
use url::Url;

use crate::extractor::resumer::{
    RedisResumerConn, ResumerDbPool, ResumerType, DEFAULT_POSITION_KEY, DEFAULT_RESUMER_SCHEMA,
    DEFAULT_RESUMER_TABLE,
};
use dt_common::{
    config::{config_enums::DbType, connection_auth_config::ConnectionAuthConfig},
    log_info,
    meta::position::Position,
    meta::redis::cluster_node::ClusterNode,
    utils::redis_util::RedisUtil,
};

pub struct ResumerUtil {}

const REDIS_RESUMER_KEY_PREFIX: &str = "apedts:resumer";
const REDIS_SCAN_COUNT: usize = 1000;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RedisResumerRecord {
    pub resumer_type: String,
    pub position_key: String,
    pub position_data: String,
}

impl ResumerUtil {
    pub fn get_full_table_name(full_table_name: &str) -> Result<(String, String)> {
        if full_table_name.is_empty() {
            return Ok((
                DEFAULT_RESUMER_SCHEMA.to_string(),
                DEFAULT_RESUMER_TABLE.to_string(),
            ));
        }

        let parts = full_table_name.split('.').collect::<Vec<&str>>();
        if parts.len() != 2 {
            bail!("invalid full table name: {}", full_table_name)
        }
        let schema = parts[0];
        let table = parts[1];
        if schema.is_empty() || table.is_empty() {
            bail!("invalid full table name: {}", full_table_name)
        }
        Ok((schema.to_string(), table.to_string()))
    }

    pub async fn create_pool(
        url: &str,
        connection_auth: &ConnectionAuthConfig,
        db_type: &DbType,
        max_connections: u32,
    ) -> anyhow::Result<ResumerDbPool> {
        let final_url = ConnectionAuthConfig::merge_url_with_auth(url, connection_auth)
            .context("failed to merge URL with connection auth")?;

        match db_type {
            DbType::Mysql => {
                let mut conn_options = MySqlConnectOptions::from_str(&final_url)
                    .context("failed to parse MySQL connection URL")?;

                if let Some(ssl) = connection_auth.ssl_config() {
                    conn_options = ssl.apply_mysql(conn_options);
                }

                let pool = MySqlPoolOptions::new()
                    .max_connections(max_connections)
                    .acquire_timeout(Duration::from_secs(15))
                    .idle_timeout(Some(Duration::from_secs(5 * 60)))
                    .connect_with(conn_options)
                    .await
                    .context("failed to create MySQL connection pool")?;

                Ok(ResumerDbPool::MySql(pool))
            }
            DbType::Pg => {
                let mut conn_options = PgConnectOptions::from_str(&final_url)
                    .context("failed to parse PostgreSQL connection URL")?;

                if let Some(ssl) = connection_auth.ssl_config() {
                    conn_options = ssl.apply_pg(conn_options);
                }

                let pool = PgPoolOptions::new()
                    .max_connections(max_connections)
                    .connect_with(conn_options)
                    .await
                    .context("failed to create PostgreSQL connection pool")?;

                Ok(ResumerDbPool::Postgres(pool))
            }
            DbType::Mongo => {
                let mut client_options = ClientOptions::parse_async(&final_url)
                    .await
                    .context("failed to parse MongoDB connection URL")?;
                client_options.app_name = Some("ape-dts-resumer".to_string());
                client_options.direct_connection = Some(true);
                client_options.max_pool_size = Some(max_connections);

                let client = mongodb::Client::with_options(client_options)
                    .context("failed to create MongoDB client")?;
                Ok(ResumerDbPool::Mongo(client))
            }
            DbType::Redis => {
                let mut conn = RedisUtil::create_redis_conn(url, connection_auth)
                    .await
                    .context("failed to create Redis resumer connection")?;
                let is_cluster = Self::is_redis_cluster(&mut conn);

                if is_cluster {
                    let nodes = RedisUtil::get_cluster_master_nodes(&mut conn)
                        .context("failed to get Redis cluster master nodes for resumer")?;
                    let node = nodes
                        .iter()
                        .find(|node| !node.slot_hash_tag_map.is_empty())
                        .context("failed to find Redis cluster master with owned slots")?;
                    let hash_tag = node
                        .slot_hash_tag_map
                        .values()
                        .next()
                        .cloned()
                        .context("failed to pick Redis cluster hash tag for resumer")?;
                    let node_url = Self::redis_node_url(url, node)?;

                    log_info!(
                        "Redis resumer uses cluster node: {}, hash_tag: {}",
                        node.address,
                        hash_tag
                    );

                    Ok(ResumerDbPool::Redis(RedisResumerConn {
                        url: node_url,
                        connection_auth: connection_auth.clone(),
                        is_cluster: true,
                        hash_tag: Some(hash_tag),
                    }))
                } else {
                    log_info!("Redis resumer uses standalone Redis connection");
                    Ok(ResumerDbPool::Redis(RedisResumerConn {
                        url: url.to_string(),
                        connection_auth: connection_auth.clone(),
                        is_cluster: false,
                        hash_tag: None,
                    }))
                }
            }
            _ => {
                bail!(
                    "unsupported database type for DatabaseRecorder: {:?}",
                    db_type
                )
            }
        }
    }

    fn is_redis_cluster(conn: &mut Connection) -> bool {
        RedisUtil::send_cmd(conn, &["INFO", "cluster"])
            .ok()
            .and_then(|value| RedisUtil::parse_result_as_string(value).ok())
            .is_some_and(|values| {
                values
                    .iter()
                    .any(|value| value.contains("cluster_enabled:1"))
            })
    }

    fn redis_node_url(base_url: &str, node: &ClusterNode) -> Result<String> {
        let mut url = Url::parse(base_url)
            .with_context(|| format!("failed to parse Redis URL: {}", base_url))?;
        url.set_host(Some(&node.host))
            .map_err(|_| anyhow::anyhow!("invalid Redis cluster node host: {}", node.host))?;
        url.set_port(Some(node.port.parse().with_context(|| {
            format!("invalid Redis cluster node port: {}", node.port)
        })?))
        .map_err(|_| anyhow::anyhow!("invalid Redis cluster node port: {}", node.port))?;
        Ok(url.to_string())
    }

    pub fn get_redis_resumer_key(
        task_id: &str,
        resumer_type: &str,
        position_key: &str,
        hash_tag: Option<&str>,
    ) -> String {
        if let Some(hash_tag) = hash_tag.filter(|hash_tag| !hash_tag.is_empty()) {
            format!(
                "{}:{{{}}}:{}:{}:{}",
                REDIS_RESUMER_KEY_PREFIX, hash_tag, task_id, resumer_type, position_key
            )
        } else {
            format!(
                "{}:{}:{}:{}",
                REDIS_RESUMER_KEY_PREFIX, task_id, resumer_type, position_key
            )
        }
    }

    pub fn get_redis_resumer_scan_pattern(task_id: &str, hash_tag: Option<&str>) -> String {
        if let Some(hash_tag) = hash_tag.filter(|hash_tag| !hash_tag.is_empty()) {
            format!(
                "{}:{{{}}}:{}:*",
                REDIS_RESUMER_KEY_PREFIX, hash_tag, task_id
            )
        } else {
            format!("{}:{}:*", REDIS_RESUMER_KEY_PREFIX, task_id)
        }
    }

    pub fn scan_redis_keys(conn: &mut Connection, pattern: &str) -> Result<Vec<String>> {
        let mut cursor = 0_u64;
        let mut keys = Vec::new();
        loop {
            let (next_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(REDIS_SCAN_COUNT)
                .query(conn)
                .with_context(|| format!("failed to scan Redis keys with pattern: {}", pattern))?;
            keys.extend(batch);
            cursor = next_cursor;
            if cursor == 0 {
                break;
            }
        }
        Ok(keys)
    }

    pub fn get_key_from_position(position: &Position) -> String {
        match position {
            Position::RdbSnapshot { schema, tb, .. }
            | Position::RdbSnapshotFinished { schema, tb, .. }
            | Position::FoxlakeS3 { schema, tb, .. } => {
                format!("{}-{}", schema, tb)
            }
            Position::Kafka {
                topic, partition, ..
            } => {
                format!("{}-{}", topic, partition)
            }
            Position::Redis {
                node_id, address, ..
            } => {
                if let Some(node_id) = node_id.as_ref().filter(|node_id| !node_id.is_empty()) {
                    format!("redis-node-{}", node_id)
                } else if let Some(address) = address.as_ref().filter(|address| !address.is_empty())
                {
                    format!("redis-node-{}", address)
                } else {
                    DEFAULT_POSITION_KEY.to_string()
                }
            }
            _ => DEFAULT_POSITION_KEY.to_string(),
        }
    }

    pub fn get_key_from_base((schema, tb): (String, String), resumer_type: ResumerType) -> String {
        match resumer_type {
            ResumerType::SnapshotDoing | ResumerType::SnapshotFinished => {
                format!("{}-{}", schema, tb)
            }
            _ => DEFAULT_POSITION_KEY.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::extractor::resumer::{
        utils::ResumerUtil, DEFAULT_RESUMER_SCHEMA, DEFAULT_RESUMER_TABLE,
    };
    use dt_common::meta::position::Position;

    #[test]
    fn test_get_full_table_name() {
        // Test default values
        let (schema, table) = ResumerUtil::get_full_table_name("").unwrap();
        assert_eq!(schema, DEFAULT_RESUMER_SCHEMA);
        assert_eq!(table, DEFAULT_RESUMER_TABLE);

        // Test valid full table name
        let (schema, table) = ResumerUtil::get_full_table_name("test_schema.test_table").unwrap();
        assert_eq!(schema, "test_schema");
        assert_eq!(table, "test_table");

        // Test invalid full table name - no dot
        let result = ResumerUtil::get_full_table_name("invalid_name");
        assert!(result.is_err());

        // Test invalid full table name - too many dots
        let result = ResumerUtil::get_full_table_name("too.many.dots");
        assert!(result.is_err());

        // Test invalid full table name - empty schema
        let result = ResumerUtil::get_full_table_name(".table_name");
        assert!(result.is_err());

        // Test invalid full table name - empty table
        let result = ResumerUtil::get_full_table_name("schema_name.");
        assert!(result.is_err());
    }

    #[test]
    fn redis_cluster_position_uses_node_id_as_resumer_key() {
        let position = Position::Redis {
            node_id: Some("node-1".to_string()),
            address: Some("127.0.0.1:6371".to_string()),
            repl_id: "repl-1".to_string(),
            repl_port: 10008,
            repl_offset: 10,
            now_db_id: 0,
            timestamp: String::new(),
        };

        assert_eq!(
            ResumerUtil::get_key_from_position(&position),
            "redis-node-node-1"
        );
    }

    #[test]
    fn redis_resumer_key_uses_hash_tag_for_cluster() {
        assert_eq!(
            ResumerUtil::get_redis_resumer_key(
                "task-1",
                "CdcDoing",
                "redis-node-node-1",
                Some("42")
            ),
            "apedts:resumer:{42}:task-1:CdcDoing:redis-node-node-1"
        );
        assert_eq!(
            ResumerUtil::get_redis_resumer_scan_pattern("task-1", Some("42")),
            "apedts:resumer:{42}:task-1:*"
        );
    }
}
