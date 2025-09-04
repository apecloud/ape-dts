use std::{collections::HashMap, sync::Arc};

use anyhow::bail;
use async_trait::async_trait;

use super::base_parallelizer::BaseParallelizer;
use crate::{DataSize, Parallelizer};
use dt_common::meta::{
    dt_data::{DtData, DtItem},
    dt_queue::DtQueue,
    redis::command::key_parser::KeyParser,
};
use dt_common::{error::Error, log_warn};
use dt_connector::Sinker;

pub struct RedisParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub parallel_size: usize,
    // redis cluster
    pub slot_node_map: HashMap<u16, &'static str>,
    pub key_parser: KeyParser,
    pub node_sinker_index_map: HashMap<String, usize>,
}

#[async_trait]
impl Parallelizer for RedisParallelizer {
    fn get_name(&self) -> String {
        "RedisParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &DtQueue) -> anyhow::Result<Vec<DtItem>> {
        self.base_parallelizer.drain(buffer).await
    }

    async fn sink_raw(
        &mut self,
        data: Vec<DtItem>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        let data_size = DataSize {
            count: data.len() as u64,
            bytes: data.iter().map(|v| v.get_data_size()).sum(),
        };

        if self.slot_node_map.is_empty() {
            self.base_parallelizer
                .sink_raw(vec![data], sinkers, 1, false)
                .await?;
            return Ok(data_size);
        }

        if self.node_sinker_index_map.is_empty() {
            self.node_sinker_index_map = HashMap::new();
            for (i, sinker) in sinkers.iter().enumerate() {
                self.node_sinker_index_map
                    .insert(sinker.lock().await.get_id(), i);
            }
        }

        let mut node_data_items = Vec::new();
        for _ in 0..sinkers.len() {
            node_data_items.push(Vec::new());
        }

        // for redis cluster
        for mut dt_item in data {
            let slots = if let DtData::Redis { entry } = &mut dt_item.dt_data {
                let slots = entry.cal_slots(&self.key_parser)?;
                for i in 1..slots.len() {
                    if slots[i] != slots[0] {
                        bail! {Error::RedisCmdError(format!(
                            "multi keys don't hash to the same slot, cmd: {}",
                            entry.cmd
                        ))};
                    }
                }

                if slots.is_empty() {
                    log_warn!("entry has no key, cmd: {}", entry.cmd.to_string());
                }
                slots
            } else {
                // never happen
                vec![]
            };

            // example: SWAPDB 0 1
            // sink to all nodes
            if slots.is_empty() {
                for node_data in node_data_items.iter_mut() {
                    node_data.push(dt_item.clone());
                }
                continue;
            }

            // find the dst node for entry by slot
            let node = *self.slot_node_map.get(&slots[0]).unwrap();
            let sinker_index = *self.node_sinker_index_map.get(node).unwrap();
            node_data_items[sinker_index].push(dt_item);
        }

        let mut futures = Vec::new();
        for sinker in sinkers.iter().take(node_data_items.len()) {
            let node_data = node_data_items.remove(0);
            let sinker = sinker.clone();
            let future = tokio::spawn(async move {
                sinker
                    .lock()
                    .await
                    .sink_raw(node_data, false)
                    .await
                    .unwrap()
            });
            futures.push(future);
        }

        for future in futures {
            future.await.unwrap();
        }

        Ok(data_size)
    }
}
