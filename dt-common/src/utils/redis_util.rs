use regex::Regex;
use std::{collections::HashMap, str::FromStr};

use anyhow::{bail, Context};
use redis::{Connection, ConnectionLike, Value};

use crate::config::connection_auth_config::ConnectionAuthConfig;
use crate::error::Error;
use crate::log_info;
use crate::meta::redis::{
    cluster_node::ClusterNode,
    command::{cmd_encoder::CmdEncoder, key_parser::KeyParser},
    redis_object::RedisCmd,
};

pub struct RedisUtil {}

const SLOTS_COUNT: usize = 16384;

impl RedisUtil {
    pub async fn create_redis_conn(
        url: &str,
        connection_auth: &ConnectionAuthConfig,
    ) -> anyhow::Result<redis::Connection> {
        let final_url = ConnectionAuthConfig::merge_url_with_auth(url, connection_auth)?;
        let conn = redis::Client::open(final_url)
            .with_context(|| format!("invalid redis url: [{}]", url))?
            .get_connection()
            .with_context(|| format!("can not connect redis: [{}]", url))?;
        Ok(conn)
    }

    pub fn send_cmd(conn: &mut Connection, cmd: &[&str]) -> anyhow::Result<Value> {
        let cmd = RedisCmd::from_str_args(cmd);
        let packed_cmd = CmdEncoder::encode(&cmd);
        Ok(conn.req_packed_command(&packed_cmd)?)
    }

    pub fn get_cluster_master_nodes(
        conn: &mut redis::Connection,
    ) -> anyhow::Result<Vec<ClusterNode>> {
        let cmd = RedisCmd::from_str_args(&["cluster", "nodes"]);
        let value = conn.req_packed_command(&CmdEncoder::encode(&cmd))?;
        if let redis::Value::BulkString(data) = value {
            let nodes_str = String::from_utf8(data)?;
            let nodes = Self::parse_cluster_nodes(&nodes_str)?;
            let master_nodes = nodes.into_iter().filter(|i| i.is_master).collect();
            Ok(master_nodes)
        } else {
            bail! {Error::RedisResultError(
                "can not get redis cluster nodes".into(),
            )}
        }
    }

    pub fn get_slot_address_map(nodes: &[ClusterNode]) -> HashMap<u16, &'static str> {
        let mut slot_address_map = HashMap::new();
        for node in nodes.iter() {
            if !node.is_master {
                continue;
            }

            let address: &'static str = Box::leak(node.address.clone().into_boxed_str());
            for slot in node.slots.iter() {
                slot_address_map.insert(*slot, address);
            }
        }
        slot_address_map
    }

    pub fn get_redis_version(conn: &mut redis::Connection) -> anyhow::Result<f32> {
        let cmd = RedisCmd::from_str_args(&["INFO"]);
        let value = conn.req_packed_command(&CmdEncoder::encode(&cmd))?;
        if let redis::Value::BulkString(data) = value {
            let info = String::from_utf8(data)?;
            log_info!("redis INFO result: {}", info);
            let re = Regex::new(r"redis_version:(\S+)").unwrap();
            let cap = re.captures(&info).unwrap();

            let version_str = cap[1].to_string();
            let tokens: Vec<&str> = version_str.split('.').collect();
            if tokens.is_empty() {
                bail! {Error::RedisResultError(
                    "can not get redis version by INFO".into(),
                )}
            }

            let mut version = tokens[0].to_string();
            if tokens.len() > 1 {
                version = format!("{}.{}", tokens[0], tokens[1]);
            }
            return Ok(f32::from_str(&version)?);
        }
        bail! {Error::RedisResultError(
            "can not get redis version by INFO".into(),
        )}
    }

    pub fn parse_result_as_string(value: Value) -> anyhow::Result<Vec<String>> {
        let mut results = Vec::new();
        match value {
            Value::BulkString(data) => {
                results.push(String::from_utf8_lossy(&data).to_string());
            }

            Value::SimpleString(data) => {
                results.push(data);
            }

            Value::Array(data) | Value::Set(data) => {
                for i in data {
                    let sub_results = Self::parse_result_as_string(i)?;
                    results.extend_from_slice(&sub_results);
                }
            }

            Value::Int(data) => results.push(data.to_string()),

            Value::Double(data) => {
                results.push(format!("{}", data));
            }

            Value::Boolean(data) => {
                results.push(format!("{}", data));
            }

            Value::BigNumber(data) => {
                results.push(data.to_string());
            }

            _ => {
                bail! {Error::RedisResultError(
                    format!("redis result type can not be parsed as string, value: {:?}", value),
                )}
            }
        }
        Ok(results)
    }

    fn get_slot_hash_tag_map() -> HashMap<u16, String> {
        let mut res: HashMap<u16, String> = HashMap::new();
        for i in 0.. {
            let key = i.to_string();
            let slot = KeyParser::calc_slot(key.as_bytes());
            //  0 to 16383
            if (slot as usize) < SLOTS_COUNT && !res.contains_key(&slot) {
                res.insert(slot, key);
            }
            if res.len() >= SLOTS_COUNT {
                break;
            }
        }
        res
    }

    fn parse_cluster_nodes(nodes_str: &str) -> anyhow::Result<Vec<ClusterNode>> {
        // refer: https://github.com/tair-opensource/RedisShake/blob/v4/internal/utils/cluster_nodes.go
        let mut all_slots_count = 0;
        let mut parsed_nodes = Vec::new();
        let all_slot_hash_tag_map = Self::get_slot_hash_tag_map();

        log_info!("cluster nodes: {}", nodes_str);
        // 5bafc7277da3038a8fbf01873179260351ed0a0a 172.28.0.13:6379@16379 master - 0 1712124938134 3 connected 12589-15758 15760-16383
        // 0e9d360631a20c27f629267bf3e01de8e8c4cbec 172.28.0.11:6379@16379 myself,master - 0 1712124940000 1 connected 1672-2267 2269-5460
        // 587ec020a7cd63397afe33d6e92ee975b4ab79a2 172.28.0.14:6379@16379 slave 5bafc7277da3038a8fbf01873179260351ed0a0a 0 1712124940213 3 connected
        for line in nodes_str.lines() {
            let line = line.trim();
            let words: Vec<&str> = line.split_whitespace().collect();

            if words.len() < 8 {
                bail! {Error::MetadataError(format!(
                    "invalid cluster nodes line: {}",
                    line
                ))}
            }

            let id = words[0].to_string();
            let master_id = words[3].to_string();
            let is_master = words[2].contains("master");

            let mut address = words[1].split('@').next().unwrap().to_string();
            let tokens: Vec<&str> = address.split(':').collect();
            let (host, port, address) = if tokens.len() > 2 {
                let port = tokens.last().unwrap().to_string();
                let ipv6_addr = tokens[..tokens.len() - 1].join(":");
                address = format!("[{}]:{}", ipv6_addr, port);
                (ipv6_addr, port, address)
            } else {
                (tokens[0].to_string(), tokens[1].to_string(), address)
            };

            let mut node = ClusterNode {
                is_master,
                id,
                master_id,
                port,
                host,
                address,
                slots: Vec::new(),
                slot_hash_tag_map: HashMap::new(),
            };

            if !is_master {
                parsed_nodes.push(node);
                continue;
            }

            if words.len() < 9 {
                log::warn!(
                    "the current master node does not hold any slots. address=[{}]",
                    node.address
                );
                parsed_nodes.push(node);
                continue;
            }

            let mut slots = Vec::new();
            for word in words.iter().skip(8) {
                if word.starts_with('[') {
                    break;
                }

                let range: Vec<&str> = word.split('-').collect();
                let (start, end) = if range.len() > 1 {
                    (
                        range[0].parse::<u16>().expect("failed to parse slot start"),
                        range[1].parse::<u16>().expect("failed to parse slot end"),
                    )
                } else {
                    let slot_num = word.parse::<u16>().expect("failed to parse slot number");
                    (slot_num, slot_num)
                };

                for j in start..=end {
                    slots.push(j);
                }
            }

            all_slots_count += slots.len();
            if !slots.is_empty() {
                let mut node_slot_hash_tag_map = HashMap::with_capacity(slots.len());
                for i in slots.iter() {
                    node_slot_hash_tag_map
                        .insert(*i, all_slot_hash_tag_map.get(i).unwrap().to_owned());
                }
                node.slot_hash_tag_map = node_slot_hash_tag_map;
            }
            node.slots = slots;
            parsed_nodes.push(node);
        }

        if all_slots_count != SLOTS_COUNT {
            bail! {Error::MetadataError(format!(
                "invalid cluster nodes slots. slots_count={}, cluster_nodes={}",
                all_slots_count, nodes_str
            ))}
        } else {
            Ok(parsed_nodes)
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_parse_cluster_nodes() {
        let cluster_nodes = r#"09596be5c2150ad93c51fdca1ff9116d1077e042 172.28.0.17:6379@16379 master - 0 1711678515085 7 connected 0-1671 2268 5461-7127 8620 10923-12588 15759
        0e9d360631a20c27f629267bf3e01de8e8c4cbec 172.28.0.11:6379@16379 myself,master - 0 1711678514000 1 connected 1672-2267 2269-5460
        5bafc7277da3038a8fbf01873179260351ed0a0a 172.28.0.13:6379@16379 master - 0 1711678515180 3 connected 12589-15758 15760-16383
        c02d3f6210367e1b7bbfd131b5c2269520ef4f73 172.28.0.12:6379@16379 master - 0 1711678514044 2 connected 7128-8619 8621-10922
        66e84ed6d7f28971cdf59d530c490561c64dda61 172.28.0.16:6379@16379 slave c02d3f6210367e1b7bbfd131b5c2269520ef4f73 0 1711678514561 2 connected
        7dd62287c3543b076551b7412cd7425f8251809d 172.28.0.18:6379@16379 slave 09596be5c2150ad93c51fdca1ff9116d1077e042 0 1711678514000 7 connected
        76d90b851f7692358d9a01d783cf64c1ac673ef5 172.28.0.15:6379@16379 slave 0e9d360631a20c27f629267bf3e01de8e8c4cbec 0 1711678514562 1 connected
        587ec020a7cd63397afe33d6e92ee975b4ab79a2 172.28.0.14:6379@16379 slave 5bafc7277da3038a8fbf01873179260351ed0a0a 0 1711678514562 3 connected"#;
        let nodes = RedisUtil::parse_cluster_nodes(cluster_nodes).unwrap();

        assert_eq!(nodes.len(), 8);

        assert_eq!(nodes[0].slots.len(), 5008);
        assert_eq!(nodes[1].slots.len(), 3788);
        assert_eq!(nodes[2].slots.len(), 3794);
        assert_eq!(nodes[3].slots.len(), 3794);

        assert!(nodes[0].slots.contains(&0));
        assert!(nodes[0].slots.contains(&1671));
        assert!(nodes[0].slots.contains(&15759));

        assert!(nodes[1].slots.contains(&1672));
        assert!(nodes[1].slots.contains(&2267));
        assert!(nodes[1].slots.contains(&2269));
        assert!(nodes[1].slots.contains(&5460));

        assert!(nodes[0].is_master);
        assert!(nodes[1].is_master);
        assert!(!nodes[4].is_master);
    }
}
