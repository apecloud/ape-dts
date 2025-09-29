use std::collections::HashMap;

use dt_common::config::config_token_parser::TokenEscapePair;
use dt_common::meta::redis::command::cmd_encoder::CmdEncoder;
use dt_common::meta::redis::redis_object::RedisCmd;
use dt_common::{config::config_token_parser::ConfigTokenParser, utils::sql_util::SqlUtil};
use redis::{Connection, ConnectionLike, Value};

use super::redis_cluster_connection::RedisClusterConnection;

const DELIMITERS: [char; 1] = [' '];
const DEFAULT_ESCAPE_PAIRS: [(char, char); 1] = [('"', '"')];
const SYSTEM_KEYS: [&str; 5] = [
    "backup1",
    "backup2",
    "backup3",
    "backup4",
    "ape_dts_heartbeat_key",
];

pub struct RedisTestUtil {
    escape_pairs: Vec<(char, char)>,
}

impl RedisTestUtil {
    pub fn new_default() -> Self {
        Self::new(DEFAULT_ESCAPE_PAIRS.to_vec())
    }

    pub fn new(escape_pairs: Vec<(char, char)>) -> Self {
        Self { escape_pairs }
    }

    pub fn get_hash_entry(&self, conn: &mut Connection, key: &str) -> HashMap<String, Value> {
        let cmd = format!("HGETALL {}", self.escape_key(key));
        let result = self.execute_cmd(conn, &cmd);

        let mut kvs = HashMap::new();
        if let redis::Value::Array(mut values) = result {
            for _i in (0..values.len()).step_by(2) {
                let k = values.remove(0);
                let v = values.remove(0);
                if let redis::Value::BulkString(k) = k {
                    kvs.insert(String::from_utf8(k).unwrap(), v);
                } else {
                    panic!();
                }
            }
        } else {
            panic!();
        }
        kvs
    }

    pub fn list_dbs(&self, conn: &mut Connection) -> Vec<String> {
        let mut dbs = Vec::new();
        let cmd = "INFO keyspace";
        match self.execute_cmd(conn, cmd) {
            redis::Value::BulkString(data) => {
                let spaces = String::from_utf8(data).unwrap();
                for space in spaces.split("\r\n").collect::<Vec<&str>>() {
                    if space.contains("db") {
                        let tokens: Vec<&str> = space.split(":").collect::<Vec<&str>>();
                        dbs.push(tokens[0].trim_start_matches("db").to_string());
                    }
                }
            }
            _ => panic!(),
        }
        dbs
    }

    pub fn list_keys(&self, conn: &mut Connection, match_pattern: &str) -> Vec<String> {
        let mut keys = Vec::new();
        let cmd = format!("KEYS {}", match_pattern);
        match self.execute_cmd(conn, &cmd) {
            redis::Value::Array(values) => {
                for v in values {
                    match v {
                        redis::Value::BulkString(data) => {
                            let key = String::from_utf8(data).unwrap();
                            if SYSTEM_KEYS.contains(&key.as_str()) {
                                continue;
                            }
                            keys.push(key)
                        }
                        _ => panic!(),
                    }
                }
            }
            _ => panic!(),
        }
        keys.sort();
        keys
    }

    pub fn get_key_type(&self, conn: &mut Connection, key: &str) -> String {
        let cmd = format!("type {}", self.escape_key(key));
        let value = self.execute_cmd(conn, &cmd);
        match value {
            redis::Value::SimpleString(key_type) => key_type,
            _ => panic!(),
        }
    }

    pub fn escape_key(&self, key: &str) -> String {
        format!(
            "{}{}{}",
            self.escape_pairs[0].0, key, self.escape_pairs[0].1
        )
    }

    pub fn execute_cmds(&self, conn: &mut Connection, cmds: &[String]) {
        for cmd in cmds.iter() {
            self.execute_cmd(conn, cmd);
        }
    }

    pub fn execute_cmd(&self, conn: &mut Connection, cmd: &str) -> Value {
        println!("execute cmd: {:?}", cmd);
        let packed_cmd = self.pack_cmd(cmd);
        let res = conn.req_packed_command(&packed_cmd).unwrap();
        println!("cmd result: {:?}", res);
        res
    }

    pub fn execute_cmds_in_cluster(&self, conn: &mut RedisClusterConnection, cmds: &[String]) {
        for cmd in cmds.iter() {
            self.execute_cmd_in_cluster(conn, cmd)
        }
    }

    pub fn execute_cmd_in_cluster(&self, conn: &mut RedisClusterConnection, cmd: &str) {
        let args = self.get_cmd_args(cmd);
        for node_conn in conn.get_node_conns_by_cmd(&args) {
            self.execute_cmd(node_conn, cmd);
        }
    }

    pub fn execute_cmd_in_one_cluster_node(
        &self,
        conn: &mut RedisClusterConnection,
        cmd: &str,
    ) -> Value {
        let args = self.get_cmd_args(cmd);
        let mut node_conns = conn.get_node_conns_by_cmd(&args);
        if node_conns.len() > 1 {
            panic!(
                "cmd has multi keys which hashed to different nodes, cmd: {}",
                cmd
            )
        }
        self.execute_cmd(node_conns[0], cmd)
    }

    fn pack_cmd(&self, cmd: &str) -> Vec<u8> {
        // parse cmd args
        let mut redis_cmd = RedisCmd::new();
        for arg in ConfigTokenParser::parse(
            cmd,
            &DELIMITERS,
            &TokenEscapePair::from_char_pairs(self.escape_pairs.clone()),
        ) {
            let mut arg = arg.clone();
            for (left, right) in &self.escape_pairs {
                arg = arg
                    .trim_start_matches(*left)
                    .trim_end_matches(*right)
                    .to_string();
            }
            redis_cmd.add_str_arg(&arg);
        }
        CmdEncoder::encode(&redis_cmd)
    }

    fn get_cmd_args(&self, cmd: &str) -> Vec<String> {
        let tokens = ConfigTokenParser::parse(
            cmd,
            &DELIMITERS,
            &TokenEscapePair::from_char_pairs(self.escape_pairs.clone()),
        );
        let mut args = Vec::new();
        for token in tokens.iter() {
            let arg = SqlUtil::unescape(token, &self.escape_pairs[0]);
            args.push(arg);
        }
        args
    }
}
