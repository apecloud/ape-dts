use anyhow::Context;
use async_trait::async_trait;
use redis::{Connection, ConnectionLike};
use std::{
    cmp,
    collections::{HashMap, HashSet},
    future::Future,
    time::{Duration, Instant},
};
use url::Url;

use crate::{
    extractor::base_extractor::{BaseExtractor, ExtractState},
    Extractor,
};
use dt_common::{
    config::connection_auth_config::ConnectionAuthConfig,
    log_debug, log_info,
    meta::redis::{
        cluster_node::ClusterNode, command::cmd_encoder::CmdEncoder, redis_object::RedisCmd,
    },
    utils::redis_util::RedisUtil,
};

const SLOTS_COUNT: usize = 16384;
const RESHARD_VERIFY_TIMEOUT: Duration = Duration::from_secs(30);
const RESHARD_VERIFY_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Copy)]
enum ExpectedResponse {
    Ok,
    OkOrNoKey,
}

enum MigrateAuth {
    Password(String),
    UsernamePassword { username: String, password: String },
}

#[derive(Debug, Eq, PartialEq)]
struct ClusterSlotMap {
    owners: Vec<String>,
    slot_counts: HashMap<String, usize>,
}

pub struct RedisReshardExtractor {
    pub base_extractor: BaseExtractor,
    pub extract_state: ExtractState,
    pub url: String,
    pub connection_auth: ConnectionAuthConfig,
}

#[async_trait]
impl Extractor for RedisReshardExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        log_info!("RedisReshardExtractor starts");
        self.reshard().await?;
        self.base_extractor
            .wait_task_finish(&mut self.extract_state)
            .await
    }
}

impl RedisReshardExtractor {
    pub async fn reshard(&self) -> anyhow::Result<()> {
        let mut conn = RedisUtil::create_redis_conn(&self.url, &self.connection_auth).await?;
        let nodes = RedisUtil::get_cluster_master_nodes(&mut conn)?;
        let slot_address_map = RedisUtil::get_slot_address_map(&nodes);
        let avg_slot_count = SLOTS_COUNT / nodes.len();

        // find nodes with slots to be moved out
        let mut move_out_slots = Vec::new();
        for node in nodes.iter() {
            log_info!("node: [{}] has [{}] slots", node.id, node.slots.len());
            for i in avg_slot_count..node.slots.len() {
                move_out_slots.push(node.slots[i]);
            }
        }

        // find nodes with slots to be moved in
        let mut node_move_in_slots = HashMap::new();
        let mut i = 0;
        for node in nodes.iter() {
            if avg_slot_count <= node.slots.len() || move_out_slots.len() <= i {
                continue;
            }

            let count = cmp::min(move_out_slots.len() - i, avg_slot_count - node.slots.len());
            let slots = move_out_slots[i..i + count].to_vec();
            i += count;

            log_info!("will move slots to: [{}], slots: {:?}", node.id, slots);
            node_move_in_slots.insert(node.id.clone(), slots);
        }

        self.move_slots(&nodes, &node_move_in_slots, &slot_address_map)
            .await?;
        self.wait_for_no_open_slots(&nodes).await
    }

    async fn move_slots(
        &self,
        nodes: &[ClusterNode],
        node_move_in_slots: &HashMap<String, Vec<u16>>,
        slot_address_map: &HashMap<u16, &str>,
    ) -> anyhow::Result<()> {
        for (dst_node_id, move_in_slots) in node_move_in_slots.iter() {
            // get dst_node by id
            let dst_node = nodes.iter().find(|i| i.id == *dst_node_id).unwrap();
            let mut dst_conn = self.get_node_conn(dst_node).await?;
            let migrate_auth =
                Self::get_migrate_auth(&self.get_node_url(dst_node)?, &self.connection_auth)?;

            let mut cur_src_node: Option<ClusterNode> = None;
            let mut cur_src_conn: Option<Connection> = None;
            for slot in move_in_slots.iter() {
                // get src_node by address
                let src_address = slot_address_map.get(slot).unwrap().to_string();
                let src_node = nodes.iter().find(|i| i.address == *src_address).unwrap();

                // get src conn
                let src_node_changed =
                    cur_src_node.is_none() || src_node.id != cur_src_node.as_ref().unwrap().id;
                if src_node_changed {
                    cur_src_node = Some(src_node.clone());
                    cur_src_conn = Some(self.get_node_conn(src_node).await?);
                }

                // move slot
                Self::setslot_and_migrate(
                    src_node,
                    dst_node,
                    cur_src_conn.as_mut().unwrap(),
                    &mut dst_conn,
                    *slot,
                    migrate_auth.as_ref(),
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn setslot_and_migrate<S: ConnectionLike, D: ConnectionLike>(
        src_node: &ClusterNode,
        dst_node: &ClusterNode,
        src_conn: &mut S,
        dst_conn: &mut D,
        slot: u16,
        migrate_auth: Option<&MigrateAuth>,
    ) -> anyhow::Result<()> {
        log_info!(
            "moving slot {} from {} to {}",
            slot,
            src_node.id,
            dst_node.id
        );

        let keys = Self::get_keys_in_slot(src_conn, slot)?;
        log_info!("slot {} has {} keys", slot, keys.len());

        // cluster setslot importing
        let dst_cmd = RedisCmd::from_str_args(&[
            "cluster",
            "setslot",
            &slot.to_string(),
            "importing",
            &src_node.id,
        ]);
        // cluster setslot migrating
        let src_cmd = RedisCmd::from_str_args(&[
            "cluster",
            "setslot",
            &slot.to_string(),
            "migrating",
            &dst_node.id,
        ]);
        Self::req_packed_command_checked(dst_conn, &dst_cmd, ExpectedResponse::Ok)?;
        Self::req_packed_command_checked(src_conn, &src_cmd, ExpectedResponse::Ok)?;

        // migrate
        for key in keys.iter() {
            log_debug!(
                "migrating key: [{}] in slot {} from {} to {}",
                key,
                slot,
                src_node.id,
                dst_node.id
            );
            Self::migrate_key(src_conn, dst_node, key, migrate_auth)?;
        }

        // cluster setslot node
        let cmd = RedisCmd::from_str_args(&[
            "cluster",
            "setslot",
            &slot.to_string(),
            "node",
            &dst_node.id,
        ]);
        Self::req_packed_command_checked(dst_conn, &cmd, ExpectedResponse::Ok)?;
        Self::req_packed_command_checked(src_conn, &cmd, ExpectedResponse::Ok)?;
        log_info!(
            "moved slot {} from {} to {}",
            slot,
            src_node.id,
            dst_node.id
        );

        Ok(())
    }

    fn migrate_command(
        dst_node: &ClusterNode,
        key: &str,
        migrate_auth: Option<&MigrateAuth>,
    ) -> RedisCmd {
        let mut cmd =
            RedisCmd::from_str_args(&["migrate", &dst_node.host, &dst_node.port, "", "0", "5000"]);
        match migrate_auth {
            Some(MigrateAuth::Password(password)) => {
                cmd.add_str_arg("auth");
                cmd.add_str_arg(password);
            }
            Some(MigrateAuth::UsernamePassword { username, password }) => {
                cmd.add_str_arg("auth2");
                cmd.add_str_arg(username);
                cmd.add_str_arg(password);
            }
            None => {}
        }
        cmd.add_str_arg("keys");
        cmd.add_str_arg(key);
        cmd
    }

    fn migrate_key<C: ConnectionLike>(
        src_conn: &mut C,
        dst_node: &ClusterNode,
        key: &str,
        migrate_auth: Option<&MigrateAuth>,
    ) -> anyhow::Result<()> {
        let cmd = Self::migrate_command(dst_node, key, migrate_auth);
        let result = src_conn.req_packed_command(&CmdEncoder::encode(&cmd))?;

        if let Some(MigrateAuth::UsernamePassword { password, .. }) = migrate_auth {
            if Self::auth2_fallback_required(&result) {
                let fallback_auth = MigrateAuth::Password(password.clone());
                let fallback_cmd = Self::migrate_command(dst_node, key, Some(&fallback_auth));
                return Self::req_packed_command_checked(
                    src_conn,
                    &fallback_cmd,
                    ExpectedResponse::OkOrNoKey,
                );
            }
        }

        Self::validate_command_response(result, ExpectedResponse::OkOrNoKey)
    }

    fn auth2_fallback_required(result: &redis::Value) -> bool {
        let redis::Value::ServerError(error) = result else {
            return false;
        };
        if error.code() != "ERR" {
            return false;
        }
        let Some(detail) = error.details() else {
            return false;
        };

        detail == "syntax error"
            || detail
                == "Target instance replied with error: ERR wrong number of arguments for 'auth' command"
    }

    fn get_migrate_auth(
        node_url: &str,
        connection_auth: &ConnectionAuthConfig,
    ) -> anyhow::Result<Option<MigrateAuth>> {
        let url = ConnectionAuthConfig::merge_url_with_auth(node_url, connection_auth)?;
        let client = redis::Client::open(url)?;
        let redis = &client.get_connection_info().redis;
        let Some(password) = redis.password.as_ref() else {
            return Ok(None);
        };

        match redis
            .username
            .as_ref()
            .filter(|username| !username.is_empty())
        {
            Some(username) => Ok(Some(MigrateAuth::UsernamePassword {
                username: username.clone(),
                password: password.clone(),
            })),
            None => Ok(Some(MigrateAuth::Password(password.clone()))),
        }
    }

    fn req_packed_command_checked<C: ConnectionLike>(
        conn: &mut C,
        cmd: &RedisCmd,
        expected: ExpectedResponse,
    ) -> anyhow::Result<()> {
        let result = conn.req_packed_command(&CmdEncoder::encode(cmd))?;
        Self::validate_command_response(result, expected)
    }

    fn validate_command_response(
        result: redis::Value,
        expected: ExpectedResponse,
    ) -> anyhow::Result<()> {
        match (expected, result.extract_error()?) {
            (_, redis::Value::Okay) => Ok(()),
            (ExpectedResponse::OkOrNoKey, redis::Value::SimpleString(result))
                if result == "NOKEY" =>
            {
                Ok(())
            }
            (_, result) => anyhow::bail!("unexpected Redis reshard command response: {result:?}"),
        }
    }

    async fn wait_for_no_open_slots(&self, nodes: &[ClusterNode]) -> anyhow::Result<()> {
        let deadline = Instant::now() + RESHARD_VERIFY_TIMEOUT;
        loop {
            match self.check_no_open_slots(nodes, deadline).await {
                Ok(()) => return Ok(()),
                Err(err) if Instant::now() < deadline => {
                    log_debug!("Redis reshard terminal verification not ready: {err:#}");
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    tokio::time::sleep(cmp::min(RESHARD_VERIFY_INTERVAL, remaining)).await;
                }
                Err(err) => {
                    return Err(err).context("Redis reshard terminal verification timed out");
                }
            }
        }
    }

    async fn check_no_open_slots(
        &self,
        nodes: &[ClusterNode],
        deadline: Instant,
    ) -> anyhow::Result<()> {
        let expected_master_ids: HashSet<&str> =
            nodes.iter().map(|node| node.id.as_str()).collect();
        let mut expected_slot_map: Option<ClusterSlotMap> = None;

        for node in nodes {
            let cluster_nodes = self
                .get_cluster_nodes_with_deadline(node, deadline)
                .await
                .with_context(|| format!("can not read verification node [{}]", node.id))?;
            let slot_map = Self::parse_cluster_slot_map(&cluster_nodes)
                .with_context(|| format!("node [{}] reports invalid slot map", node.id))?;
            let actual_master_ids: HashSet<&str> =
                slot_map.slot_counts.keys().map(String::as_str).collect();
            if actual_master_ids != expected_master_ids {
                anyhow::bail!(
                    "node [{}] reports a different master set: expected={:?}, actual={:?}",
                    node.id,
                    expected_master_ids,
                    actual_master_ids
                );
            }
            if let Some(expected) = expected_slot_map.as_ref() {
                if slot_map != *expected {
                    anyhow::bail!("node [{}] reports a different slot ownership map", node.id);
                }
            } else {
                expected_slot_map = Some(slot_map);
            }
        }
        Ok(())
    }

    async fn get_cluster_nodes_with_deadline(
        &self,
        node: &ClusterNode,
        deadline: Instant,
    ) -> anyhow::Result<String> {
        let url = ConnectionAuthConfig::merge_url_with_auth(
            &self.get_node_url(node)?,
            &self.connection_auth,
        )?;
        let client = redis::Client::open(url)?;
        Self::within_deadline(deadline, async move {
            let mut conn = client.get_multiplexed_async_connection().await?;
            let result: redis::Value = redis::cmd("cluster")
                .arg("nodes")
                .query_async(&mut conn)
                .await?;
            match result.extract_error()? {
                redis::Value::BulkString(result) => Ok(String::from_utf8(result)?),
                result => anyhow::bail!("unexpected CLUSTER NODES response: {result:?}"),
            }
        })
        .await
    }

    async fn within_deadline<T, F>(deadline: Instant, future: F) -> anyhow::Result<T>
    where
        F: Future<Output = anyhow::Result<T>>,
    {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .context("Redis reshard terminal verification deadline elapsed")?;
        if remaining.is_zero() {
            anyhow::bail!("Redis reshard terminal verification deadline elapsed")
        }
        tokio::time::timeout(remaining, future)
            .await
            .context("Redis reshard terminal verification I/O timed out")?
    }

    fn parse_cluster_slot_map(cluster_nodes: &str) -> anyhow::Result<ClusterSlotMap> {
        let mut owners: Vec<Option<String>> = vec![None; SLOTS_COUNT];
        let mut slot_counts = HashMap::new();

        for line in cluster_nodes.lines().filter(|line| !line.trim().is_empty()) {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 8 {
                anyhow::bail!("malformed CLUSTER NODES line: {line}")
            }
            if fields.iter().skip(8).any(|field| field.starts_with('[')) {
                anyhow::bail!("open Redis cluster slot remains in line: {line}")
            }

            let flags: HashSet<&str> = fields[2].split(',').collect();
            if !flags.contains("master") {
                continue;
            }
            if fields[7] != "connected"
                || flags
                    .iter()
                    .any(|flag| matches!(*flag, "fail" | "fail?" | "handshake" | "noaddr"))
            {
                anyhow::bail!("master [{}] is not connected and healthy", fields[0])
            }
            if slot_counts.insert(fields[0].to_string(), 0).is_some() {
                anyhow::bail!("duplicate master [{}] in CLUSTER NODES", fields[0])
            }

            for field in fields.iter().skip(8) {
                let (start, end) = if let Some((start, end)) = field.split_once('-') {
                    if end.contains('-') {
                        anyhow::bail!("malformed Redis slot range: {field}")
                    }
                    (start.parse::<usize>()?, end.parse::<usize>()?)
                } else {
                    let slot = field.parse::<usize>()?;
                    (slot, slot)
                };
                if start > end || end >= SLOTS_COUNT {
                    anyhow::bail!("invalid Redis slot range: {field}")
                }

                for slot in start..=end {
                    if let Some(owner) = owners[slot].as_ref() {
                        anyhow::bail!(
                            "Redis slot [{slot}] has duplicate owners [{owner}] and [{}]",
                            fields[0]
                        )
                    }
                    owners[slot] = Some(fields[0].to_string());
                    *slot_counts.get_mut(fields[0]).unwrap() += 1;
                }
            }
        }

        if slot_counts.is_empty() {
            anyhow::bail!("CLUSTER NODES response contains no master")
        }
        let missing_count = owners.iter().filter(|owner| owner.is_none()).count();
        if missing_count != 0 {
            anyhow::bail!("Redis slot map is missing [{missing_count}] slots")
        }

        let min_slots = slot_counts.values().min().copied().unwrap();
        let max_slots = slot_counts.values().max().copied().unwrap();
        if max_slots - min_slots > 1 {
            anyhow::bail!(
                "Redis slot map is not balanced: min_slots={min_slots}, max_slots={max_slots}"
            )
        }

        Ok(ClusterSlotMap {
            owners: owners.into_iter().map(Option::unwrap).collect(),
            slot_counts,
        })
    }

    fn get_keys_in_slot<C: ConnectionLike>(conn: &mut C, slot: u16) -> anyhow::Result<Vec<String>> {
        // get all keys in slot
        let cmd =
            RedisCmd::from_str_args(&["cluster", "getkeysinslot", &slot.to_string(), "100000000"]);
        let packed_cmd = &CmdEncoder::encode(&cmd);
        let result = conn.req_packed_command(packed_cmd)?;
        RedisUtil::parse_result_as_string(result)
    }

    async fn get_node_conn(&self, node: &ClusterNode) -> anyhow::Result<Connection> {
        RedisUtil::create_redis_conn(&self.get_node_url(node)?, &self.connection_auth).await
    }

    fn get_node_url(&self, node: &ClusterNode) -> anyhow::Result<String> {
        Self::build_node_url(&self.url, node)
    }

    fn build_node_url(base_url: &str, node: &ClusterNode) -> anyhow::Result<String> {
        let url_info = Url::parse(base_url)?;
        let username = url_info.username();
        let password = url_info.password().unwrap_or("").to_string();
        Ok(format!(
            "redis://{}:{}@{}",
            username, password, node.address
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, VecDeque},
        time::{Duration, Instant},
    };

    use redis::{parse_redis_value, ConnectionLike, ErrorKind, RedisError, RedisResult, Value};

    use super::{ExpectedResponse, MigrateAuth, RedisReshardExtractor};
    use dt_common::{
        config::{
            connection_auth_config::ConnectionAuthConfig,
            ssl_config::{SslConfig, SslMode},
        },
        meta::redis::{
            cluster_node::ClusterNode, command::cmd_encoder::CmdEncoder, redis_object::RedisCmd,
        },
    };

    struct FakeConnection {
        responses: VecDeque<RedisResult<Value>>,
        commands: Vec<Vec<u8>>,
    }

    impl FakeConnection {
        fn new(responses: Vec<RedisResult<Value>>) -> Self {
            Self {
                responses: responses.into(),
                commands: Vec::new(),
            }
        }

        fn single(response: RedisResult<Value>) -> Self {
            Self::new(vec![response])
        }
    }

    impl ConnectionLike for FakeConnection {
        fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value> {
            self.commands.push(cmd.to_vec());
            self.responses
                .pop_front()
                .expect("test must provide one response per command")
        }

        fn req_packed_commands(
            &mut self,
            _cmd: &[u8],
            _offset: usize,
            _count: usize,
        ) -> RedisResult<Vec<Value>> {
            unreachable!("reshard mutation commands are not pipelined")
        }

        fn get_db(&self) -> i64 {
            0
        }

        fn check_connection(&mut self) -> bool {
            true
        }

        fn is_open(&self) -> bool {
            true
        }
    }

    fn setslot_command() -> RedisCmd {
        RedisCmd::from_str_args(&["cluster", "setslot", "42", "migrating", "node-id"])
    }

    fn server_error_with_detail(detail: &str) -> RedisResult<Value> {
        let response = format!("*1\r\n-ERR {detail}\r\n");
        Ok(parse_redis_value(response.as_bytes())
            .unwrap()
            .into_sequence()
            .unwrap()
            .pop()
            .unwrap())
    }

    fn server_error() -> RedisResult<Value> {
        server_error_with_detail("slot is already busy")
    }

    fn keys_response() -> RedisResult<Value> {
        Ok(Value::Array(vec![Value::BulkString(b"key-42".to_vec())]))
    }

    fn ok_response() -> RedisResult<Value> {
        Ok(Value::Okay)
    }

    fn cluster_node(id: &str, address: &str) -> ClusterNode {
        let (host, port) = address.split_once(':').unwrap();
        ClusterNode {
            is_master: true,
            id: id.to_string(),
            master_id: "-".to_string(),
            host: host.to_string(),
            port: port.to_string(),
            address: address.to_string(),
            slots: vec![42],
            slot_hash_tag_map: HashMap::new(),
        }
    }

    fn packed_command_args_from_bytes(packed: &[u8]) -> Vec<String> {
        let Value::Array(args) = parse_redis_value(packed).unwrap() else {
            panic!("packed command must be a RESP array");
        };
        args.into_iter()
            .map(|arg| match arg {
                Value::BulkString(arg) => String::from_utf8(arg).unwrap(),
                _ => panic!("packed command arguments must be bulk strings"),
            })
            .collect()
    }

    fn packed_command_args(cmd: &RedisCmd) -> Vec<String> {
        packed_command_args_from_bytes(&CmdEncoder::encode(cmd))
    }

    fn migrate_command_args(node_url: &str, connection_auth: &ConnectionAuthConfig) -> Vec<String> {
        let auth = RedisReshardExtractor::get_migrate_auth(node_url, connection_auth).unwrap();
        let node = cluster_node("dst", "127.0.0.2:6379");
        packed_command_args(&RedisReshardExtractor::migrate_command(
            &node,
            "key-42",
            auth.as_ref(),
        ))
    }

    #[test]
    fn migrate_command_omits_auth_when_target_connection_has_no_password() {
        let args = migrate_command_args("redis://127.0.0.2:6379", &ConnectionAuthConfig::NoAuth);

        assert_eq!(
            args,
            vec![
                "migrate",
                "127.0.0.2",
                "6379",
                "",
                "0",
                "5000",
                "keys",
                "key-42",
            ]
        );
    }

    #[test]
    fn migrate_command_uses_auth_for_password_only_target_connection() {
        let args = migrate_command_args(
            "redis://:target-secret@127.0.0.2:6379",
            &ConnectionAuthConfig::NoAuth,
        );

        assert_eq!(
            args,
            vec![
                "migrate",
                "127.0.0.2",
                "6379",
                "",
                "0",
                "5000",
                "auth",
                "target-secret",
                "keys",
                "key-42",
            ]
        );
    }

    #[test]
    fn migrate_command_uses_auth2_and_decodes_url_credentials() {
        let args = migrate_command_args(
            "redis://user%40name:p%40ss%3Aword@127.0.0.2:6379",
            &ConnectionAuthConfig::NoAuth,
        );

        assert_eq!(
            args,
            vec![
                "migrate",
                "127.0.0.2",
                "6379",
                "",
                "0",
                "5000",
                "auth2",
                "user@name",
                "p@ss:word",
                "keys",
                "key-42",
            ]
        );
    }

    #[test]
    fn migrate_command_uses_connection_auth_override() {
        let auth = ConnectionAuthConfig::Basic {
            username: "override-user".to_string(),
            password: Some("override-secret".to_string()),
        };
        let args = migrate_command_args("redis://old:old-secret@127.0.0.2:6379", &auth);

        assert_eq!(
            args,
            vec![
                "migrate",
                "127.0.0.2",
                "6379",
                "",
                "0",
                "5000",
                "auth2",
                "override-user",
                "override-secret",
                "keys",
                "key-42",
            ]
        );
    }

    #[test]
    fn migrate_command_uses_connection_auth_special_characters() {
        let auth = ConnectionAuthConfig::Basic {
            username: "user@name".to_string(),
            password: Some("p@ss:word".to_string()),
        };
        let args = migrate_command_args("redis://127.0.0.2:6379", &auth);

        assert_eq!(
            &args[6..],
            ["auth2", "user@name", "p@ss:word", "keys", "key-42"]
        );
    }

    #[test]
    fn migrate_command_node_url_without_password_matches_target_connection_auth() {
        let node = cluster_node("dst", "127.0.0.2:6379");
        let node_url =
            RedisReshardExtractor::build_node_url("redis://default@seed:6379", &node).unwrap();
        assert_eq!(node_url, "redis://default:@127.0.0.2:6379");

        let args = migrate_command_args(&node_url, &ConnectionAuthConfig::NoAuth);

        assert_eq!(&args[6..], ["keys", "key-42"]);
    }

    #[test]
    fn migrate_command_empty_basic_password_does_not_override_absent_url_password() {
        let auth = ConnectionAuthConfig::Basic {
            username: "default".to_string(),
            password: Some(String::new()),
        };
        let args = migrate_command_args("redis://127.0.0.2:6379", &auth);

        assert_eq!(&args[6..], ["keys", "key-42"]);
    }

    #[test]
    fn migrate_command_uses_basic_ssl_auth_override() {
        let auth = ConnectionAuthConfig::BasicSsl {
            username: Some("tls-user".to_string()),
            password: Some("tls-secret".to_string()),
            ssl_config: SslConfig {
                ssl_mode: SslMode::Require,
                ssl_ca_path: String::new(),
            },
        };
        let args = migrate_command_args("redis://old:old-secret@127.0.0.2:6379", &auth);

        assert_eq!(
            &args[6..],
            ["auth2", "tls-user", "tls-secret", "keys", "key-42"]
        );
    }

    #[test]
    fn migrate_command_basic_ssl_without_username_preserves_url_auth() {
        let auth = ConnectionAuthConfig::BasicSsl {
            username: None,
            password: Some("ignored-secret".to_string()),
            ssl_config: SslConfig {
                ssl_mode: SslMode::Require,
                ssl_ca_path: String::new(),
            },
        };
        let args = migrate_command_args("redis://url-user:url-secret@127.0.0.2:6379", &auth);

        assert_eq!(
            &args[6..],
            ["auth2", "url-user", "url-secret", "keys", "key-42"]
        );
    }

    #[test]
    fn checked_command_accepts_only_expected_success_responses() {
        let mut ok_conn = FakeConnection::single(Ok(Value::Okay));
        RedisReshardExtractor::req_packed_command_checked(
            &mut ok_conn,
            &setslot_command(),
            ExpectedResponse::Ok,
        )
        .unwrap();

        let migrate_cmd =
            RedisCmd::from_str_args(&["migrate", "127.0.0.1", "6379", "", "0", "5000"]);
        let mut nokey_conn = FakeConnection::single(Ok(Value::SimpleString("NOKEY".to_string())));
        RedisReshardExtractor::req_packed_command_checked(
            &mut nokey_conn,
            &migrate_cmd,
            ExpectedResponse::OkOrNoKey,
        )
        .unwrap();

        let mut setslot_nokey_conn =
            FakeConnection::single(Ok(Value::SimpleString("NOKEY".to_string())));
        let err = RedisReshardExtractor::req_packed_command_checked(
            &mut setslot_nokey_conn,
            &setslot_command(),
            ExpectedResponse::Ok,
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("unexpected Redis reshard command response"));
    }

    #[test]
    fn checked_command_rejects_server_and_transport_errors() {
        let mut server_error_conn = FakeConnection::single(server_error());
        let err = RedisReshardExtractor::req_packed_command_checked(
            &mut server_error_conn,
            &setslot_command(),
            ExpectedResponse::Ok,
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("slot is already busy"));

        let mut transport_error_conn = FakeConnection::single(Err(RedisError::from((
            ErrorKind::IoError,
            "connection lost",
        ))));
        let err = RedisReshardExtractor::req_packed_command_checked(
            &mut transport_error_conn,
            &setslot_command(),
            ExpectedResponse::Ok,
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("connection lost"));
    }

    #[test]
    fn checked_command_rejects_unexpected_non_error_response() {
        let mut conn = FakeConnection::single(Ok(Value::Nil));

        let err = RedisReshardExtractor::req_packed_command_checked(
            &mut conn,
            &setslot_command(),
            ExpectedResponse::Ok,
        )
        .unwrap_err();

        assert!(format!("{err:#}").contains("unexpected Redis reshard command response"));
    }

    #[tokio::test]
    async fn slot_migration_stops_at_each_rejected_mutation() {
        let scenarios = vec![
            (vec![keys_response()], vec![server_error()], 1, 1),
            (
                vec![keys_response(), server_error()],
                vec![ok_response()],
                2,
                1,
            ),
            (
                vec![keys_response(), ok_response(), server_error()],
                vec![ok_response()],
                3,
                1,
            ),
            (
                vec![keys_response(), ok_response(), ok_response()],
                vec![ok_response(), server_error()],
                3,
                2,
            ),
            (
                vec![
                    keys_response(),
                    ok_response(),
                    ok_response(),
                    server_error(),
                ],
                vec![ok_response(), ok_response()],
                4,
                2,
            ),
        ];

        for (src_responses, dst_responses, expected_src_commands, expected_dst_commands) in
            scenarios
        {
            let mut src_conn = FakeConnection::new(src_responses);
            let mut dst_conn = FakeConnection::new(dst_responses);

            let err = RedisReshardExtractor::setslot_and_migrate(
                &cluster_node("src", "127.0.0.1:6379"),
                &cluster_node("dst", "127.0.0.2:6379"),
                &mut src_conn,
                &mut dst_conn,
                42,
                None,
            )
            .await
            .unwrap_err();

            assert!(format!("{err:#}").contains("slot is already busy"));
            assert_eq!(src_conn.commands.len(), expected_src_commands);
            assert_eq!(dst_conn.commands.len(), expected_dst_commands);
        }
    }

    #[tokio::test]
    async fn slot_migration_sends_target_auth2_with_migrate() {
        let auth = RedisReshardExtractor::get_migrate_auth(
            "redis://default:target-secret@127.0.0.2:6379",
            &ConnectionAuthConfig::NoAuth,
        )
        .unwrap();
        let mut src_conn = FakeConnection::new(vec![
            keys_response(),
            ok_response(),
            ok_response(),
            ok_response(),
        ]);
        let mut dst_conn = FakeConnection::new(vec![ok_response(), ok_response()]);

        RedisReshardExtractor::setslot_and_migrate(
            &cluster_node("src", "127.0.0.1:6379"),
            &cluster_node("dst", "127.0.0.2:6379"),
            &mut src_conn,
            &mut dst_conn,
            42,
            auth.as_ref(),
        )
        .await
        .unwrap();

        assert_eq!(
            packed_command_args_from_bytes(&src_conn.commands[2]),
            vec![
                "migrate",
                "127.0.0.2",
                "6379",
                "",
                "0",
                "5000",
                "auth2",
                "default",
                "target-secret",
                "keys",
                "key-42",
            ]
        );
    }

    #[test]
    fn migrate_retries_password_auth_only_for_auth2_compatibility_errors() {
        let auth = MigrateAuth::UsernamePassword {
            username: "default".to_string(),
            password: "target-secret".to_string(),
        };
        let compatibility_errors = [
            "syntax error",
            "Target instance replied with error: ERR wrong number of arguments for 'auth' command",
        ];

        for compatibility_error in compatibility_errors {
            let mut src_conn = FakeConnection::new(vec![
                server_error_with_detail(compatibility_error),
                ok_response(),
            ]);

            RedisReshardExtractor::migrate_key(
                &mut src_conn,
                &cluster_node("dst", "127.0.0.2:6379"),
                "key-42",
                Some(&auth),
            )
            .unwrap();

            assert_eq!(src_conn.commands.len(), 2);
            assert_eq!(
                &packed_command_args_from_bytes(&src_conn.commands[0])[6..],
                ["auth2", "default", "target-secret", "keys", "key-42"]
            );
            assert_eq!(
                &packed_command_args_from_bytes(&src_conn.commands[1])[6..],
                ["auth", "target-secret", "keys", "key-42"]
            );
        }
    }

    #[test]
    fn migrate_does_not_fallback_for_authentication_failure() {
        let auth = MigrateAuth::UsernamePassword {
            username: "named-user".to_string(),
            password: "wrong-secret".to_string(),
        };
        let mut src_conn = FakeConnection::single(server_error_with_detail(
            "Target instance replied with error: WRONGPASS invalid username-password pair",
        ));

        let err = RedisReshardExtractor::migrate_key(
            &mut src_conn,
            &cluster_node("dst", "127.0.0.2:6379"),
            "key-42",
            Some(&auth),
        )
        .unwrap_err();

        assert_eq!(src_conn.commands.len(), 1);
        assert!(format!("{err:#}").contains("WRONGPASS"));
    }

    #[test]
    fn migrate_does_not_fallback_for_auth2_compatibility_near_match() {
        let auth = MigrateAuth::UsernamePassword {
            username: "named-user".to_string(),
            password: "target-secret".to_string(),
        };
        let mut src_conn = FakeConnection::single(server_error_with_detail(
            "proxy: wrong number of arguments for 'auth' command",
        ));

        let err = RedisReshardExtractor::migrate_key(
            &mut src_conn,
            &cluster_node("dst", "127.0.0.2:6379"),
            "key-42",
            Some(&auth),
        )
        .unwrap_err();

        assert_eq!(src_conn.commands.len(), 1);
        assert!(format!("{err:#}").contains("wrong number of arguments"));
    }

    #[test]
    fn terminal_verification_requires_complete_balanced_slot_ownership() {
        let clean = "\
src 127.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-8191\n\
dst 127.0.0.2:6379@16379 master - 0 0 2 connected 8192-16383";
        let clean_map = RedisReshardExtractor::parse_cluster_slot_map(clean).unwrap();
        assert_eq!(clean_map.slot_counts["src"], 8192);
        assert_eq!(clean_map.slot_counts["dst"], 8192);

        let migrating = format!("{clean} [42->-dst]");
        assert!(RedisReshardExtractor::parse_cluster_slot_map(&migrating).is_err());

        let importing = format!("{clean} [42-<-src]");
        assert!(RedisReshardExtractor::parse_cluster_slot_map(&importing).is_err());

        let missing = "\
src 127.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-8190\n\
dst 127.0.0.2:6379@16379 master - 0 0 2 connected 8192-16383";
        assert!(RedisReshardExtractor::parse_cluster_slot_map(missing).is_err());

        let duplicate = "\
src 127.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-8191\n\
dst 127.0.0.2:6379@16379 master - 0 0 2 connected 8191-16383";
        assert!(RedisReshardExtractor::parse_cluster_slot_map(duplicate).is_err());

        let imbalanced = "\
src 127.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-8999\n\
dst 127.0.0.2:6379@16379 master - 0 0 2 connected 9000-16383";
        assert!(RedisReshardExtractor::parse_cluster_slot_map(imbalanced).is_err());

        let malformed_slot = "\
src 127.0.0.1:6379@16379 myself,master - 0 0 1 connected garbage\n\
dst 127.0.0.2:6379@16379 master - 0 0 2 connected 0-16383";
        assert!(RedisReshardExtractor::parse_cluster_slot_map(malformed_slot).is_err());

        assert!(RedisReshardExtractor::parse_cluster_slot_map("").is_err());
        assert!(RedisReshardExtractor::parse_cluster_slot_map("malformed").is_err());
    }

    #[test]
    fn terminal_verification_detects_disagreement_between_master_views() {
        let first = "\
src 127.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-8191\n\
dst 127.0.0.2:6379@16379 master - 0 0 2 connected 8192-16383";
        let second = "\
src 127.0.0.1:6379@16379 master - 0 0 1 connected 0-8190 8192\n\
dst 127.0.0.2:6379@16379 myself,master - 0 0 2 connected 8191 8193-16383";

        let first_map = RedisReshardExtractor::parse_cluster_slot_map(first).unwrap();
        let second_map = RedisReshardExtractor::parse_cluster_slot_map(second).unwrap();
        assert_ne!(first_map, second_map);
    }

    #[tokio::test]
    async fn terminal_verification_deadline_cancels_stalled_io() {
        let started = Instant::now();
        let err =
            RedisReshardExtractor::within_deadline(started + Duration::from_millis(20), async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(())
            })
            .await
            .unwrap_err();

        assert!(format!("{err:#}").contains("I/O timed out"));
        assert!(started.elapsed() < Duration::from_millis(500));
    }
}
