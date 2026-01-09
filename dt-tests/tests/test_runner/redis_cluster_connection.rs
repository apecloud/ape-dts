use dt_common::utils::redis_util::RedisUtil;
use dt_common::{
    config::connection_auth_config::ConnectionAuthConfig,
    meta::redis::command::key_parser::KeyParser,
};
use redis::Connection;
use std::collections::{HashMap, HashSet};
use url::Url;

pub struct RedisClusterConnection {
    slot_node_map: HashMap<u16, &'static str>,
    node_conn_map: HashMap<String, Connection>,
    default_conn: Connection,
    key_parser: KeyParser,
}

impl RedisClusterConnection {
    pub async fn new(
        url: &str,
        connection_auth: &ConnectionAuthConfig,
        is_cluster: bool,
    ) -> anyhow::Result<Self> {
        let mut slot_node_map = HashMap::new();
        let mut node_conn_map = HashMap::new();
        let mut conn = RedisUtil::create_redis_conn(url, connection_auth).await?;

        if is_cluster {
            let nodes = RedisUtil::get_cluster_master_nodes(&mut conn)?;
            slot_node_map = RedisUtil::get_slot_address_map(&nodes);

            let url_info = Url::parse(url)?;
            let username = url_info.username();
            let password = url_info.password().unwrap_or("").to_string();

            let nodes = RedisUtil::get_cluster_master_nodes(&mut conn)?;
            for node in nodes {
                println!("redis cluster node: {}", node.address);
                let new_url = format!("redis://{}:{}@{}", username, password, node.address);
                let conn = RedisUtil::create_redis_conn(&new_url, connection_auth).await?;
                node_conn_map.insert(node.address.clone(), conn);
            }
        }

        Ok(Self {
            slot_node_map,
            node_conn_map,
            default_conn: conn,
            key_parser: KeyParser::new(),
        })
    }

    pub fn is_cluster(&self) -> bool {
        !self.node_conn_map.is_empty()
    }

    pub fn get_default_conn(&mut self) -> &mut Connection {
        &mut self.default_conn
    }

    pub fn get_node_conn_by_key(&mut self, key: &str) -> &mut Connection {
        if self.slot_node_map.is_empty() {
            return self.get_default_conn();
        }

        let slot = KeyParser::calc_slot(key.as_bytes());
        let node = *self.slot_node_map.get(&slot).unwrap();
        println!("get redis node: {} by key: {:?}", node, key);

        self.node_conn_map.get_mut(node).unwrap()
    }

    pub fn get_node_conns_by_cmd<'a>(&'a mut self, args: &[String]) -> Vec<&'a mut Connection> {
        if self.slot_node_map.is_empty() {
            return vec![self.get_default_conn()];
        }

        let (_cmd_name, _group, keys, _keys_indexes) =
            self.key_parser.parse_key_from_argv(args).unwrap();
        if keys.is_empty() {
            // some cmd has no key, and should be executed in all nodes, example: SWAPDB 0 1
            let conns: Vec<&mut Connection> = self.node_conn_map.iter_mut().map(|i| i.1).collect();
            conns
        } else {
            let mut nodes = HashSet::new();
            for key in keys.iter() {
                let slot = KeyParser::calc_slot(key.as_bytes());
                nodes.insert(self.slot_node_map.get(&slot).unwrap().to_string());
            }
            println!("get redis nodes: {:?} by keys: {:?}", nodes, keys);

            let mut conns = Vec::new();
            for (node, conn) in self.node_conn_map.iter_mut() {
                if nodes.contains(node) {
                    conns.push(conn);
                }
            }
            conns
        }
    }
}
