use std::sync::Arc;

use anyhow::bail;
use async_trait::async_trait;
use dt_common::utils::limit_queue::LimitedQueue;
use redis::Connection;
use redis::ConnectionLike;
use redis::Value;
use tokio::{sync::RwLock, time::Instant};

use super::entry_rewriter::EntryRewriter;
use crate::call_batch_fn;
use crate::data_marker::DataMarker;
use crate::sinker::base_sinker::BaseSinker;
use crate::Sinker;
use dt_common::error::Error;
use dt_common::log_debug;
use dt_common::meta::dt_data::DtData;
use dt_common::meta::dt_data::DtItem;
use dt_common::meta::rdb_meta_manager::RdbMetaManager;
use dt_common::meta::redis::cluster_node::ClusterNode;
use dt_common::meta::redis::command::cmd_encoder::CmdEncoder;
use dt_common::meta::redis::command::key_parser::KeyParser;
use dt_common::meta::redis::redis_object::RedisCmd;
use dt_common::meta::redis::redis_object::RedisObject;
use dt_common::meta::redis::redis_write_method::RedisWriteMethod;
use dt_common::meta::row_data::RowData;
use dt_common::meta::row_type::RowType;
use dt_common::monitor::monitor::Monitor;

pub struct RedisSinker {
    pub cluster_node: Option<ClusterNode>,
    pub batch_size: usize,
    pub conn: Connection,
    pub now_db_id: i64,
    pub version: f32,
    pub method: RedisWriteMethod,
    pub meta_manager: Option<RdbMetaManager>,
    pub monitor: Arc<Monitor>,
    pub data_marker: Option<Arc<RwLock<DataMarker>>>,
    pub key_parser: KeyParser,
}

#[async_trait]
impl Sinker for RedisSinker {
    async fn sink_raw(&mut self, mut data: Vec<DtItem>, _batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let is_cluster_two_way = self.cluster_node.is_some() && self.data_marker.is_some();
        if self.batch_size <= 1 || is_cluster_two_way {
            self.serial_sink_raw(&mut data).await?;
        } else {
            call_batch_fn!(self, data, Self::batch_sink_raw);
        }
        Ok(())
    }

    async fn sink_dml(&mut self, mut data: Vec<Arc<RowData>>, _batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        if self.batch_size > 1 {
            call_batch_fn!(self, data, Self::batch_sink_dml);
        } else {
            self.serial_sink_dml(&mut data).await?;
        }
        Ok(())
    }

    fn get_id(&self) -> String {
        if let Some(node) = &self.cluster_node {
            node.address.clone()
        } else {
            String::new()
        }
    }
}

/// sink raw
impl RedisSinker {
    async fn batch_sink_raw(
        &mut self,
        data: &mut [DtItem],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let mut data_size = 0;

        let mut cmds = Vec::new();
        for dt_item in data.iter_mut().skip(start_index).take(batch_size) {
            data_size += dt_item.dt_data.get_data_size();
            cmds.extend_from_slice(&self.rewrite_entry(&mut dt_item.dt_data)?);
        }

        self.batch_sink(&cmds).await?;

        BaseSinker::update_batch_monitor(&self.monitor, cmds.len() as u64, data_size).await
    }

    async fn serial_sink_raw(&mut self, data: &mut [DtItem]) -> anyhow::Result<()> {
        let mut data_size = 0;

        for dt_item in data.iter_mut() {
            data_size += dt_item.dt_data.get_data_size();
            let cmds = self.rewrite_entry(&mut dt_item.dt_data)?;
            for cmd in cmds {
                self.batch_sink(&[cmd]).await?;
            }
        }

        BaseSinker::update_serial_monitor(&self.monitor, data.len() as u64, data_size).await
    }

    fn rewrite_entry(&mut self, dt_data: &mut DtData) -> anyhow::Result<Vec<RedisCmd>> {
        let mut cmds = Vec::new();
        if let DtData::Redis { entry } = dt_data {
            if entry.db_id != self.now_db_id {
                let db_id = &entry.db_id.to_string();
                let args = vec!["SELECT", db_id];
                let cmd = RedisCmd::from_str_args(&args);
                cmds.push(cmd);
                self.now_db_id = entry.db_id;
            }

            match self.method {
                RedisWriteMethod::Restore => {
                    if entry.is_raw() {
                        let cmd = EntryRewriter::rewrite_as_restore(entry, self.version)?;
                        cmds.push(cmd);
                    } else {
                        cmds.push(entry.cmd.clone());
                    }
                }

                RedisWriteMethod::Rewrite => {
                    let mut rewrite_cmds = match entry.value {
                        RedisObject::String(ref mut obj) => EntryRewriter::rewrite_string(obj),
                        RedisObject::List(ref mut obj) => EntryRewriter::rewrite_list(obj),
                        RedisObject::Set(ref mut obj) => EntryRewriter::rewrite_set(obj),
                        RedisObject::Hash(ref mut obj) => EntryRewriter::rewrite_hash(obj),
                        RedisObject::Zset(ref mut obj) => EntryRewriter::rewrite_zset(obj),
                        RedisObject::Stream(ref mut obj) => Ok(obj.cmds.drain(..).collect()),
                        RedisObject::Module(_) => {
                            // TODO: support rewrite some 8.0+ major module2 types, such as: JSON, Bloom, CountMinSketch, TDigest, TopK, TimeSeries
                            let cmd = EntryRewriter::rewrite_as_restore(entry, self.version)?;
                            Ok(vec![cmd])
                        }
                        _ => bail! {Error::SinkerError("rewrite not implemented".into())},
                    }?;
                    if let Some(expire_cmd) = EntryRewriter::rewrite_expire(entry)? {
                        rewrite_cmds.push(expire_cmd)
                    }
                    cmds.extend(rewrite_cmds);
                }
            }
        }
        Ok(cmds)
    }
}

/// sink dml
impl RedisSinker {
    async fn batch_sink_dml(
        &mut self,
        data: &mut [Arc<RowData>],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let mut data_size = 0;

        let mut cmds = Vec::new();
        for row_data in data.iter().skip(start_index).take(batch_size) {
            data_size += row_data.get_data_size();
            if let Some(cmd) = self.dml_to_redis_cmd(row_data).await? {
                cmds.push(cmd);
            }
        }
        self.batch_sink(&cmds).await?;

        BaseSinker::update_batch_monitor(&self.monitor, cmds.len() as u64, data_size).await
    }

    async fn serial_sink_dml(&mut self, data: &mut [Arc<RowData>]) -> anyhow::Result<()> {
        let mut data_size = 0;

        for row_data in data.iter() {
            data_size += row_data.get_data_size();
            if let Some(cmd) = self.dml_to_redis_cmd(row_data).await? {
                self.batch_sink(&[cmd]).await?
            }
        }

        BaseSinker::update_serial_monitor(&self.monitor, data.len() as u64, data_size).await
    }

    async fn dml_to_redis_cmd(
        &mut self,
        row_data: &Arc<RowData>,
    ) -> anyhow::Result<Option<RedisCmd>> {
        if self.meta_manager.is_none() {
            return Ok(None);
        }

        let tb_meta = self
            .meta_manager
            .as_mut()
            .unwrap()
            .get_tb_meta(&row_data.schema, &row_data.tb)
            .await?;

        // no single primary / unique key exists, do not sink to redis
        if tb_meta.order_cols.is_empty() {
            return Ok(None);
        }

        let key = if let Some(col) = tb_meta.order_cols.first() {
            match row_data.row_type {
                RowType::Insert | RowType::Update => row_data
                    .require_after()?
                    .get(col)
                    .and_then(|v| v.to_option_string()),
                RowType::Delete => row_data
                    .require_before()?
                    .get(col)
                    .and_then(|v| v.to_option_string()),
            }
        } else {
            None
        };

        let key = if let Some(v) = key {
            format!("{}.{}.{}", row_data.schema, row_data.tb, v)
        } else {
            return Ok(None);
        };

        let mut cmd = RedisCmd::new();
        match row_data.row_type {
            RowType::Insert | RowType::Update => {
                cmd.add_str_arg("hset");
                cmd.add_str_arg(&key);
                for (col, col_value) in row_data.require_after()? {
                    cmd.add_str_arg(col);
                    if let Some(v) = col_value.to_option_string() {
                        cmd.add_str_arg(&v);
                    } else {
                        cmd.add_str_arg("");
                    }
                }
            }
            RowType::Delete => {
                cmd.add_str_arg("del");
                cmd.add_str_arg(&key);
            }
        }
        Ok(Some(cmd))
    }
}

impl RedisSinker {
    async fn batch_sink(&mut self, cmds: &[RedisCmd]) -> anyhow::Result<()> {
        if cmds.is_empty() {
            return Ok(());
        }

        let mut packed_cmds = Vec::new();
        let mut is_tx = false;
        let mut tx_wrapper_cmds = Vec::new();
        if let Some(data_marker_cmd) = self.get_data_marker_cmd(cmds[0].clone()).await? {
            let multi_cmd = RedisCmd::from_str_args(&["MULTI"]);
            packed_cmds.extend_from_slice(&CmdEncoder::encode(&multi_cmd));
            packed_cmds.extend_from_slice(&CmdEncoder::encode(&data_marker_cmd));
            tx_wrapper_cmds.push(multi_cmd);
            tx_wrapper_cmds.push(data_marker_cmd);
            is_tx = true;
        }

        for cmd in cmds.iter() {
            packed_cmds.extend_from_slice(&CmdEncoder::encode(cmd));
        }

        if is_tx {
            let exec_cmd = RedisCmd::from_str_args(&["EXEC"]);
            packed_cmds.extend_from_slice(&CmdEncoder::encode(&exec_cmd));
            tx_wrapper_cmds.push(exec_cmd);
        }

        let count = if is_tx { cmds.len() + 3 } else { cmds.len() };
        let mut rts = LimitedQueue::new(1);
        let start_time = Instant::now();
        let result = self.conn.req_packed_commands(&packed_cmds, 0, count);
        rts.push((start_time.elapsed().as_millis() as u64, 1));
        BaseSinker::update_monitor_rt(&self.monitor, &rts).await?;

        match result {
            Err(error) => {
                bail! {Error::SinkerError(format!(
                    "batch sink failed, error: {:?}",
                    error
                ))}
            }

            Ok(values) => {
                for (i, v) in values.iter().enumerate() {
                    if *v == Value::Okay {
                        continue;
                    }

                    let cmd = if is_tx {
                        match i {
                            0 => &tx_wrapper_cmds[0],
                            1 => &tx_wrapper_cmds[1],
                            n if n == values.len() - 1 => &tx_wrapper_cmds[2],
                            n => &cmds[n - 2],
                        }
                    } else {
                        &cmds[i]
                    };

                    match v {
                        Value::ServerError(e) => {
                            bail! {Error::SinkerError(format!(
                                "sink failed, server error: [{:?}], result: [{:?}], cmd: [{}]",
                                e, v, cmd
                            ))}
                        }
                        _ => {
                            log_debug!("sink result: [{:?}], cmd: [{}]", v, cmd)
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn get_data_marker_cmd(&self, mut cmd: RedisCmd) -> anyhow::Result<Option<RedisCmd>> {
        if let Some(data_marker) = &self.data_marker {
            let data_marker = data_marker.read().await;
            let key = if let Some(node) = &self.cluster_node {
                // the target Redis is a cluster node.
                cmd.parse_keys(&self.key_parser)?;
                let hash_tag = if !cmd.keys.is_empty() {
                    // find the hash_tag for the slot which cmd.keys[0] belongs to,
                    // otherwise we may get: (error) CROSSSLOT Keys in request don't hash to the same slot
                    let slot = KeyParser::calc_slot(cmd.keys[0].as_bytes());
                    node.slot_hash_tag_map.get(&slot).unwrap()
                } else {
                    // if the redis cmd has no key, find a hash_tag for any slot in current node
                    let (_, hash_tag) = node.slot_hash_tag_map.iter().next().unwrap();
                    hash_tag
                };
                &format!("{}{{{}}}", data_marker.marker, hash_tag)
            } else {
                &data_marker.marker
            };

            let data_marker_cmd =
                RedisCmd::from_str_args(&["SET", key, &data_marker.data_origin_node]);
            log_debug!("data_marker_cmd: [{}] by cmd: [{}]", data_marker_cmd, cmd);
            return Ok(Some(data_marker_cmd));
        }
        Ok(None)
    }
}
