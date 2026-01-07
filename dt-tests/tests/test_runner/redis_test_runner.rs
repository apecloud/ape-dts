use crate::test_runner::redis_test_util::RedisTestUtil;

use super::{base_test_runner::BaseTestRunner, redis_cluster_connection::RedisClusterConnection};
use anyhow::bail;
use dt_common::{
    config::{
        config_enums::DbType,
        config_token_parser::{ConfigTokenParser, TokenEscapePair},
        extractor_config::ExtractorConfig,
        sinker_config::SinkerConfig,
        task_config::TaskConfig,
    },
    error::Error,
    rdb_filter::RdbFilter,
    utils::{redis_util::RedisUtil, sql_util::SqlUtil, time_util::TimeUtil},
};

use redis::{Connection, Value};

pub struct RedisTestRunner {
    pub base: BaseTestRunner,
    src_conn: Connection,
    dst_conn: RedisClusterConnection,
    redis_util: RedisTestUtil,
    filter: RdbFilter,
}

impl RedisTestRunner {
    pub async fn new_default(relative_test_dir: &str) -> anyhow::Result<Self> {
        Self::new(relative_test_dir, vec![('"', '"')]).await
    }

    pub async fn new(
        relative_test_dir: &str,
        escape_pairs: Vec<(char, char)>,
    ) -> anyhow::Result<Self> {
        let base = BaseTestRunner::new(relative_test_dir).await.unwrap();

        let config = TaskConfig::new(&base.task_config_file).unwrap();
        let src_conn = match config.extractor {
            ExtractorConfig::RedisSnapshot {
                url,
                connection_auth,
                ..
            }
            | ExtractorConfig::RedisCdc {
                url,
                connection_auth,
                ..
            }
            | ExtractorConfig::RedisSnapshotAndCdc {
                url,
                connection_auth,
                ..
            } => RedisUtil::create_redis_conn(&url, &connection_auth)
                .await
                .unwrap(),
            _ => {
                bail! {Error::ConfigError("unsupported extractor config".into())};
            }
        };

        let dst_conn = match config.sinker {
            SinkerConfig::Redis {
                url, is_cluster, ..
            } => RedisClusterConnection::new(&url, is_cluster).await.unwrap(),
            _ => {
                bail! {Error::ConfigError("unsupported sinker config".into())};
            }
        };

        let redis_util = RedisTestUtil::new(escape_pairs);
        let filter = RdbFilter::from_config(&config.filter, &DbType::Redis)?;
        Ok(Self {
            base,
            src_conn,
            dst_conn,
            redis_util,
            filter,
        })
    }

    pub async fn run_snapshot_test(&mut self) -> anyhow::Result<()> {
        self.execute_prepare_sqls()?;

        self.print_version_info();

        self.execute_test_sqls()?;
        self.base.start_task().await?;
        self.compare_all_data()
    }

    pub async fn run_cdc_test(
        &mut self,
        start_millis: u64,
        parse_millis: u64,
    ) -> anyhow::Result<()> {
        self.execute_prepare_sqls()?;

        let task = self.base.spawn_task().await?;
        TimeUtil::sleep_millis(start_millis).await;

        self.print_version_info();

        self.execute_test_sqls()?;
        TimeUtil::sleep_millis(parse_millis).await;
        self.compare_all_data()?;

        self.base.abort_task(&task).await
    }

    pub async fn run_heartbeat_test(
        &mut self,
        start_millis: u64,
        _parse_millis: u64,
    ) -> anyhow::Result<()> {
        let config = TaskConfig::new(&self.base.task_config_file).unwrap();
        let heartbeat_key = match config.extractor {
            ExtractorConfig::RedisCdc { heartbeat_key, .. } => heartbeat_key.clone(),
            _ => String::new(),
        };

        let heartbeat_db_key = ConfigTokenParser::parse(
            &heartbeat_key,
            &['.'],
            &TokenEscapePair::from_char_pairs(SqlUtil::get_escape_pairs(&DbType::Redis)),
        );
        let db_id: i64 = heartbeat_db_key[0].parse().unwrap();
        let key = &heartbeat_db_key[1];

        let cmd = format!("SELECT {}", db_id);
        self.redis_util.execute_cmd(&mut self.src_conn, &cmd);

        self.execute_prepare_sqls()?;

        let cmd = format!("GET {}", self.redis_util.escape_key(key));
        let result = self.redis_util.execute_cmd(&mut self.src_conn, &cmd);
        assert_eq!(result, Value::Nil);

        let task = self.base.spawn_task().await?;
        TimeUtil::sleep_millis(start_millis).await;
        self.base.abort_task(&task).await.unwrap();

        let result = self.redis_util.execute_cmd(&mut self.src_conn, &cmd);
        assert_ne!(result, Value::Nil);
        Ok(())
    }

    pub fn execute_prepare_sqls(&mut self) -> anyhow::Result<()> {
        self.redis_util
            .execute_cmds(&mut self.src_conn, &self.base.src_prepare_sqls.clone());
        self.redis_util
            .execute_cmds_in_cluster(&mut self.dst_conn, &self.base.dst_prepare_sqls.clone());
        Ok(())
    }

    pub fn execute_test_sqls(&mut self) -> anyhow::Result<()> {
        self.redis_util
            .execute_cmds(&mut self.src_conn, &self.base.src_test_sqls.clone());
        Ok(())
    }

    pub fn compare_all_data(&mut self) -> anyhow::Result<()> {
        let dbs = if self.dst_conn.is_cluster() {
            // a redis cluster strictly supports only database 0
            vec!["0".to_string()]
        } else {
            self.redis_util.list_dbs(&mut self.src_conn)
        };
        for db in dbs.iter() {
            println!("compare data for db: {}", db);
            self.compare_data(db)?;
        }
        Ok(())
    }

    fn compare_data(&mut self, db: &str) -> anyhow::Result<()> {
        self.redis_util
            .execute_cmd(&mut self.src_conn, &format!("SELECT {}", db));
        self.redis_util
            .execute_cmd_in_cluster(&mut self.dst_conn, &format!("SELECT {}", db));

        let data_marker_key = if let Some(data_marker) = self.base.get_data_marker() {
            data_marker.marker
        } else {
            String::new()
        };

        let mut string_keys = Vec::new();
        let mut hash_keys = Vec::new();
        let mut list_keys = Vec::new();
        let mut stream_keys = Vec::new();
        let mut set_keys = Vec::new();
        let mut zset_keys = Vec::new();

        // json
        let mut json_keys = Vec::new();
        // bloom filter
        let mut bf_bloom_keys = Vec::new();
        // cuckoo filter
        let mut cf_bloom_keys = Vec::new();
        // count min sketch
        let mut cmsk_keys = Vec::new();
        // tdigest
        let mut tdigest_keys = Vec::new();
        // topk
        let mut topk_keys = Vec::new();
        // time series
        let mut tsdb_keys = Vec::new();
        // graph
        let mut graph_keys = Vec::new();

        let keys = self.redis_util.list_keys(&mut self.src_conn, "*");
        for i in keys.iter() {
            let key = i.clone();

            if key == data_marker_key {
                continue;
            }

            let key_type = self.redis_util.get_key_type(&mut self.src_conn, &key);
            match key_type.to_lowercase().as_str() {
                "string" => string_keys.push(key),
                "hash" => hash_keys.push(key),
                "zset" => zset_keys.push(key),
                "stream" => stream_keys.push(key),
                "set" => set_keys.push(key),
                "list" => list_keys.push(key),
                "rejson-rl" => json_keys.push(key),
                "mbbloom--" => bf_bloom_keys.push(key),
                "mbbloomcf" => cf_bloom_keys.push(key),
                "cmsk-type" => cmsk_keys.push(key),
                "tdis-type" => tdigest_keys.push(key),
                "topk-type" => topk_keys.push(key),
                "tsdb-type" => tsdb_keys.push(key),
                "graphdata" => graph_keys.push(key),
                _ => {
                    println!("unknown type: {} for key: {}", key_type, key);
                    string_keys.push(key)
                }
            }
        }

        self.compare_string_entries(db, &string_keys);
        self.compare_hash_entries(db, &hash_keys);
        self.compare_list_entries(db, &list_keys);
        self.compare_set_entries(db, &set_keys);
        self.compare_zset_entries(db, &zset_keys);
        self.compare_stream_entries(db, &stream_keys);
        self.compare_rejson_entries(db, &json_keys);
        self.compare_bf_bloom_entries(db, &bf_bloom_keys);
        self.compare_cf_bloom_entries(db, &cf_bloom_keys);
        self.compare_cmsk_entries(db, &cmsk_keys);
        self.compare_tdigest_entries(db, &tdigest_keys);
        self.compare_topk_entries(db, &topk_keys);
        self.compare_tsdb_entries(db, &tsdb_keys);
        self.compare_graph_entries(db, &graph_keys);
        self.check_expire(&keys);
        Ok(())
    }

    fn check_expire(&mut self, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("PTTL {}", self.redis_util.escape_key(key));
            let src_result = self.redis_util.execute_cmd(&mut self.src_conn, &cmd);
            let dst_result = self
                .redis_util
                .execute_cmd_in_one_cluster_node(&mut self.dst_conn, &cmd);

            let get_expire = |result: Value| -> i64 {
                match result {
                    Value::Int(expire) => expire,
                    _ => {
                        // should never happen
                        -1000
                    }
                }
            };

            let src_expire = get_expire(src_result);
            let dst_expire = get_expire(dst_result);
            assert_ne!(src_expire, -1000);
            assert_ne!(dst_expire, -1000);
            if src_expire > 0 {
                println!("src key: {} expire in {}", key, src_expire);
                println!("dst key: {} expire in {}", key, dst_expire);
                assert!(dst_expire > 0)
            } else {
                assert!(dst_expire < 0)
            }
        }
    }

    fn compare_string_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("GET {}", self.redis_util.escape_key(key));
            self.compare_cmd_results(&cmd, db, key, None);
        }
    }

    fn compare_hash_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let src_kvs = self.redis_util.get_hash_entry(&mut self.src_conn, key);
            let dst_node_conn = self.dst_conn.get_node_conn_by_key(key);
            let dst_kvs = self.redis_util.get_hash_entry(dst_node_conn, key);
            println!(
                "compare results for hash entries, \r\n src_kvs: {:?} \r\n dst_kvs: {:?}",
                src_kvs, dst_kvs
            );

            if self.filter.filter_schema(db) {
                println!("filtered, db: {}, key: {}", db, key);
                assert_eq!(dst_kvs.len(), 0);
            } else {
                assert_eq!(src_kvs, dst_kvs);
            }
        }
    }

    fn compare_list_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("LRANGE {} 0 -1", self.redis_util.escape_key(key));
            self.compare_cmd_results(&cmd, db, key, None);
        }
    }

    fn compare_set_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("SORT {} ALPHA", self.redis_util.escape_key(key));
            self.compare_cmd_results(&cmd, db, key, None);
        }
    }

    fn compare_zset_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("ZRANGE {} 0 -1 WITHSCORES", self.redis_util.escape_key(key));
            self.compare_cmd_results(&cmd, db, key, None);
        }
    }

    fn compare_stream_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("XRANGE {} - +", self.redis_util.escape_key(key));
            self.compare_cmd_results(&cmd, db, key, None);
        }
    }

    fn compare_rejson_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("JSON.GET {}", self.redis_util.escape_key(key));
            self.compare_cmd_results(&cmd, db, key, None);
        }
    }

    fn compare_bf_bloom_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("BF.DEBUG {}", self.redis_util.escape_key(key));
            self.compare_cmd_results(&cmd, db, key, None);
        }
    }

    fn compare_cf_bloom_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("CF.DEBUG {}", self.redis_util.escape_key(key));
            self.compare_cmd_results(&cmd, db, key, None);
        }
    }

    fn compare_cmsk_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("CMS.INFO {}", self.redis_util.escape_key(key));
            self.compare_cmd_results(&cmd, db, key, None);
        }
    }

    fn compare_tdigest_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!(
                "TDIGEST.QUANTILE {} 0.1 0.25 0.5 0.75 0.9 0.95 0.99",
                self.redis_util.escape_key(key)
            );
            self.compare_cmd_results(&cmd, db, key, None);
        }
    }

    fn compare_topk_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("TOPK.LIST {} WITHCOUNT", self.redis_util.escape_key(key));
            self.compare_cmd_results(&cmd, db, key, None);
        }
    }

    fn compare_tsdb_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("TS.RANGE {} - +", self.redis_util.escape_key(key));
            self.compare_cmd_results(&cmd, db, key, None);
        }
    }

    fn compare_graph_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!(
                "GRAPH.QUERY {} \"MATCH (n) RETURN n\"",
                self.redis_util.escape_key(key)
            );
            self.compare_cmd_results(&cmd, db, key, Some(1));
        }
    }

    fn compare_cmd_results(&mut self, cmd: &str, db: &str, key: &str, result_index: Option<usize>) {
        let src_result = self.redis_util.execute_cmd(&mut self.src_conn, cmd);
        let dst_result = self
            .redis_util
            .execute_cmd_in_one_cluster_node(&mut self.dst_conn, cmd);

        let (src_to_compare, dst_to_compare) = if let Some(idx) = result_index {
            match (&src_result, &dst_result) {
                (Value::Array(src_arr), Value::Array(dst_arr)) => {
                    let src_elem = src_arr.get(idx as usize).unwrap_or(&Value::Nil);
                    let dst_elem = dst_arr.get(idx as usize).unwrap_or(&Value::Nil);
                    (src_elem, dst_elem)
                }
                (Value::Set(src_set), Value::Set(dst_set)) => {
                    let src_elem = src_set.get(idx as usize).unwrap_or(&Value::Nil);
                    let dst_elem = dst_set.get(idx as usize).unwrap_or(&Value::Nil);
                    (src_elem, dst_elem)
                }
                _ => (&src_result, &dst_result),
            }
        } else {
            (&src_result, &dst_result)
        };

        println!(
            "compare results for cmd: {}, \r\n src_kvs: {:?} \r\n dst_kvs: {:?}",
            cmd, src_to_compare, dst_to_compare
        );

        if self.filter.filter_schema(db) {
            println!("filtered, db: {}, key: {}", db, key);
            match dst_to_compare {
                Value::Array(v) | Value::Set(v) => assert_eq!(v, &[]),
                _ => assert_eq!(dst_to_compare, &Value::Nil),
            }
        } else {
            assert_eq!(src_to_compare, dst_to_compare);
        }
    }

    fn print_version_info(&mut self) {
        println!(
            "src: {}",
            RedisUtil::get_redis_version(&mut self.src_conn).unwrap()
        );
        let dst_node_conn = self.dst_conn.get_default_conn();
        println!(
            "dst: {}",
            RedisUtil::get_redis_version(dst_node_conn).unwrap()
        );
    }
}
