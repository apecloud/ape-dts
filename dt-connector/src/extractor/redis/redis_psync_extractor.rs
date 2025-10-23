use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::bail;
use async_trait::async_trait;
use tokio::{sync::Mutex, time::Instant};

use super::redis_client::RedisClient;
use crate::extractor::{
    base_extractor::BaseExtractor,
    redis::{
        rdb::{rdb_parser::RdbParser, reader::rdb_reader::RdbReader},
        redis_resp_types::Value,
        StreamReader,
    },
    resumer::recovery::Recovery,
};
use crate::Extractor;
use dt_common::{
    config::{
        config_enums::{DbType, ExtractType},
        config_token_parser::{ConfigTokenParser, TokenEscapePair},
    },
    error::Error,
    log_debug, log_error, log_info, log_position,
    meta::{
        dt_data::DtData,
        position::Position,
        redis::{redis_entry::RedisEntry, redis_object::RedisCmd},
        syncer::Syncer,
    },
    rdb_filter::RdbFilter,
    utils::{sql_util::SqlUtil, time_util::TimeUtil},
};

pub struct RedisPsyncExtractor {
    pub base_extractor: BaseExtractor,
    pub conn: RedisClient,
    pub repl_id: String,
    pub repl_offset: u64,
    pub repl_port: u64,
    pub now_db_id: i64,
    pub keepalive_interval_secs: u64,
    pub heartbeat_interval_secs: u64,
    pub heartbeat_key: String,
    pub syncer: Arc<Mutex<Syncer>>,
    pub filter: RdbFilter,
    pub extract_type: ExtractType,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

#[async_trait]
impl Extractor for RedisPsyncExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        log_info!(
            "RedisPsyncExtractor starts, repl_id: {}, repl_offset: {}, now_db_id: {}, 
             keepalive_interval_secs: {}, heartbeat_interval_secs: {}, heartbeat_key: {}",
            self.repl_id,
            self.repl_offset,
            self.now_db_id,
            self.keepalive_interval_secs,
            self.heartbeat_interval_secs,
            self.heartbeat_key
        );

        let full_sync = self.start_psync().await?;
        if full_sync {
            // server won't send rdb if it's NOT full sync
            self.receive_rdb().await?;
        }

        if matches!(
            self.extract_type,
            ExtractType::Cdc | ExtractType::SnapshotAndCdc
        ) {
            self.receive_aof().await?;
        }

        self.base_extractor.wait_task_finish().await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.conn.close().await
    }
}

impl RedisPsyncExtractor {
    pub async fn start_psync(&mut self) -> anyhow::Result<bool> {
        // replconf listening-port [port]
        let repl_port = self.repl_port.to_string();
        let repl_cmd = RedisCmd::from_str_args(&["replconf", "listening-port", &repl_port]);
        log_info!("repl command: {}", repl_cmd.to_string());

        self.conn.send(&repl_cmd).await?;
        if let Value::Okay = self.conn.read().await? {
        } else {
            bail! {Error::ExtractorError(
                "replconf listening-port response is not Ok".into(),
            )}
        }

        let full_sync = self.repl_id.is_empty() && self.repl_offset == 0;
        let (repl_id, repl_offset) = if full_sync {
            ("?".to_string(), "-1".to_string())
        } else {
            (self.repl_id.clone(), self.repl_offset.to_string())
        };

        // PSYNC [repl_id] [offset]
        let psync_cmd = RedisCmd::from_str_args(&["PSYNC", &repl_id, &repl_offset]);
        log_info!("PSYNC command: {}", psync_cmd.to_string());
        self.conn.send(&psync_cmd).await?;
        let value = self.conn.read().await?;

        if let Value::Status(s) = value {
            log_info!("PSYNC command response status: {:?}", s);
            if full_sync {
                let tokens: Vec<&str> = s.split_whitespace().collect();
                self.repl_id = tokens[1].to_string();
                self.repl_offset = tokens[2].parse::<u64>()?;
            } else if s != "CONTINUE" {
                bail! {Error::ExtractorError(
                    "PSYNC command response is NOT CONTINUE".into(),
                )}
            }
        } else {
            bail! {Error::ExtractorError(
                "PSYNC command response is NOT status".into(),
            )}
        };
        Ok(full_sync)
    }

    async fn receive_rdb(&mut self) -> anyhow::Result<()> {
        let mut stream_reader: Box<&mut (dyn StreamReader + Send)> = Box::new(&mut self.conn);
        // format: \n\n\n$<length>\r\n<rdb>
        loop {
            let buf = stream_reader.read_bytes(1).await?;
            if buf[0] == b'\n' {
                continue;
            }
            if buf[0] != b'$' {
                panic!("invalid rdb format");
            }
            break;
        }

        // length of rdb data
        let mut rdb_length_str = String::new();
        loop {
            let buf = stream_reader.read_bytes(1).await?;
            if buf[0] == b'\n' {
                break;
            }
            if buf[0] != b'\r' {
                rdb_length_str.push(buf[0] as char);
            }
        }
        let rdb_length = rdb_length_str.parse::<usize>()?;

        let reader = RdbReader {
            conn: &mut stream_reader,
            rdb_length,
            position: 0,
            copy_raw: false,
            raw_bytes: Vec::new(),
        };

        let mut parser = RdbParser {
            reader,
            repl_stream_db_id: 0,
            now_db_id: self.now_db_id,
            expire_ms: 0,
            idle: 0,
            freq: 0,
            is_end: false,
        };

        let version = parser.load_meta().await?;
        log_info!("source redis version: {:?}", version);

        loop {
            if let Some(entry) = parser.load_entry().await? {
                self.now_db_id = entry.db_id;
                if matches!(
                    self.extract_type,
                    ExtractType::Snapshot | ExtractType::SnapshotAndCdc
                ) {
                    if let Some(data_marker) = &self.base_extractor.data_marker {
                        if data_marker.is_redis_marker_info(&entry) {
                            continue;
                        }
                    }

                    Self::push_to_buf(
                        &mut self.base_extractor,
                        &mut self.filter,
                        entry,
                        Position::None,
                    )
                    .await?;
                }
            }

            if parser.is_end {
                log_info!(
                    "end extracting data from rdb, all count: {}",
                    self.base_extractor.monitor.counters.pushed_record_count
                );
                break;
            }
        }

        // this log to mark the snapshot rdb was all received
        let position = Position::Redis {
            repl_id: self.repl_id.clone(),
            repl_port: self.repl_port,
            repl_offset: self.repl_offset + 1,
            now_db_id: parser.now_db_id,
            timestamp: String::new(),
        };
        log_position!("start_aof_position | {}", position.to_string());
        Ok(())
    }

    async fn receive_aof(&mut self) -> anyhow::Result<()> {
        let heartbeat_db_key = ConfigTokenParser::parse(
            &self.heartbeat_key,
            &['.'],
            &TokenEscapePair::from_char_pairs(SqlUtil::get_escape_pairs(&DbType::Redis)),
        );
        let heartbeat_db_id = if heartbeat_db_key.len() == 2 {
            heartbeat_db_key[0].parse()?
        } else {
            i64::MIN
        };

        // start heartbeat
        if heartbeat_db_key.len() == 2 {
            self.start_heartbeat(
                heartbeat_db_id,
                &heartbeat_db_key[1],
                self.base_extractor.shut_down.clone(),
            )
            .await?;
        } else {
            log_info!("heartbeat disabled, heartbeat_tb should be like db.key");
        }

        let mut heartbeat_timestamp = String::new();
        let mut start_time = Instant::now();
        loop {
            // heartbeat
            if start_time.elapsed().as_secs() >= self.keepalive_interval_secs {
                self.keep_alive_ack().await?;
                start_time = Instant::now();
            }

            let (value, n) = self.conn.read_with_len().await?;
            if Value::Nil == value {
                TimeUtil::sleep_millis(1).await;
                continue;
            }

            self.repl_offset += n as u64;
            let cmd = self.handle_redis_value(value).await?;
            log_debug!("received cmd: [{}]", cmd);

            if !cmd.args.is_empty() {
                let cmd_name = cmd.get_name().to_ascii_lowercase();

                // switch db
                if cmd_name == "select" {
                    self.now_db_id = String::from_utf8(cmd.args[1].clone())?.parse::<i64>()?;
                    continue;
                }

                // get timestamp generated by heartbeat
                if self.now_db_id == heartbeat_db_id
                    && cmd
                        .get_str_arg(1)
                        .eq_ignore_ascii_case(&heartbeat_db_key[1])
                {
                    heartbeat_timestamp = cmd.get_str_arg(2);
                    continue;
                }

                let position = Position::Redis {
                    repl_id: self.repl_id.clone(),
                    repl_port: self.repl_port,
                    repl_offset: self.repl_offset,
                    now_db_id: self.now_db_id,
                    timestamp: heartbeat_timestamp.clone(),
                };

                // transaction begin
                // if there is only 1 command in a transaction, MULTI/EXEC won't be saved in aof.
                // but in our two-way sync scenario, it is OK since we will always add an additional
                // SET command as data marker following MULTI
                if cmd_name == "multi" {
                    // since not all commands are wrapped by MULTI and EXEC,
                    // in two-way sync scenario, we must push both DtData::Begin and DtData::Commit
                    // to buf to make sure:
                    // 1, only the first command following MULTI be considered as data marker info.
                    // 2, data_marker will be reset following EXEC.
                    self.base_extractor
                        .refresh_and_check_data_marker(&DtData::Begin {});
                    // ignore MULTI & EXEC
                    continue;
                }

                // transaction end
                if cmd_name == "exec" {
                    self.base_extractor
                        .refresh_and_check_data_marker(&DtData::Commit { xid: String::new() });
                    continue;
                }

                // a single ping(should NOT be in a transaction)
                if cmd_name == "ping" {
                    self.base_extractor
                        .push_dt_data(DtData::Heartbeat {}, position)
                        .await?;
                    continue;
                }

                // filter dangerous cmds, eg: flushdb, flushall
                if self.filter.filter_cmd(&cmd_name) {
                    continue;
                }

                // build entry and push it to buffer
                let mut entry = RedisEntry::new();
                entry.cmd = cmd;
                entry.db_id = self.now_db_id;

                Self::push_to_buf(&mut self.base_extractor, &mut self.filter, entry, position)
                    .await?;
            }
        }
    }

    async fn handle_redis_value(&mut self, value: Value) -> anyhow::Result<RedisCmd> {
        let mut cmd = RedisCmd::new();
        match value {
            Value::Bulk(values) => {
                for v in values {
                    match v {
                        Value::Data(data) => cmd.add_arg(data),
                        _ => {
                            log_error!("received unexpected value in aof bulk: {:?}", v);
                            break;
                        }
                    }
                }
            }
            v => {
                bail! {Error::RedisRdbError(format!(
                    "received unexpected aof value: {:?}",
                    v
                ))}
            }
        }
        Ok(cmd)
    }

    async fn keep_alive_ack(&mut self) -> anyhow::Result<()> {
        // send replconf ack to keep the connection alive
        let mut position_repl_offset = self.repl_offset;
        if let Position::Redis { repl_offset, .. } = self.syncer.lock().await.committed_position {
            if repl_offset >= self.repl_offset {
                position_repl_offset = repl_offset
            }
        }

        let repl_offset = &position_repl_offset.to_string();
        let args = vec!["replconf", "ack", repl_offset];
        let cmd = RedisCmd::from_str_args(&args);
        log_info!("replconf ack cmd: {}", cmd.to_string());
        if let Err(err) = self.conn.send(&cmd).await {
            log_error!("replconf ack failed, error: {:?}", err);
        }
        Ok(())
    }

    async fn start_heartbeat(
        &self,
        db_id: i64,
        key: &str,
        shut_down: Arc<AtomicBool>,
    ) -> anyhow::Result<()> {
        log_info!(
            "try starting heartbeat, heartbeat_interval_secs: {}, db_id: {}, key: {}",
            self.heartbeat_interval_secs,
            db_id,
            key
        );

        if self.heartbeat_interval_secs == 0 || db_id == i64::MIN || key.is_empty() {
            log_info!("heartbeat disabled, heartbeat_tb should be like db_id.key");
            return Ok(());
        }

        let mut conn = RedisClient::new(&self.conn.url).await?;
        let heartbeat_interval_secs = self.heartbeat_interval_secs;
        let key = key.to_string();

        tokio::spawn(async move {
            // set db
            let cmd = RedisCmd::from_str_args(&["SELECT", &db_id.to_string()]);
            if let Err(err) = conn.send(&cmd).await {
                log_error!(
                    "heartbeat failed, cmd: {}, error: {:?}",
                    cmd.to_string(),
                    err
                );
            }

            let mut start_time = Instant::now();
            while !shut_down.load(Ordering::Acquire) {
                if start_time.elapsed().as_secs() >= heartbeat_interval_secs {
                    Self::heartbeat(&key, &mut conn).await.unwrap();
                    start_time = Instant::now();
                }
                TimeUtil::sleep_millis(1000 * heartbeat_interval_secs).await;
            }
        });
        log_info!("heartbeat started");
        Ok(())
    }

    async fn heartbeat(key: &str, conn: &mut RedisClient) -> anyhow::Result<()> {
        // send `SET heartbeat_key current_timestamp` by another connection to generate timestamp
        let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let timestamp =
            since_epoch.as_secs() * 1000 + since_epoch.subsec_nanos() as u64 / 1_000_000;
        let heartbeat_value = Position::format_timestamp_millis(timestamp as i64);

        let cmd = RedisCmd::from_str_args(&["SET", key, &heartbeat_value]);
        log_info!("heartbeat cmd: {}", cmd.to_string());
        if let Err(err) = conn.send(&cmd).await {
            log_error!("heartbeat failed, error: {:?}", err);
        }
        Ok(())
    }

    pub async fn push_to_buf(
        base_extractor: &mut BaseExtractor,
        filter: &mut RdbFilter,
        mut entry: RedisEntry,
        position: Position,
    ) -> anyhow::Result<()> {
        // currently only support db filter
        entry.data_size = entry.get_data_malloc_size();
        if filter.filter_schema(&entry.db_id.to_string()) {
            base_extractor
                .push_dt_data(DtData::Heartbeat {}, position)
                .await
        } else {
            base_extractor
                .push_dt_data(DtData::Redis { entry }, position)
                .await
        }
    }
}
