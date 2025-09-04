use std::sync::Arc;

use async_trait::async_trait;

use super::base_parallelizer::BaseParallelizer;
use crate::{DataSize, Parallelizer};
use dt_common::meta::{dt_data::DtItem, dt_queue::DtQueue, row_data::RowData};
use dt_connector::Sinker;

pub struct SnapshotParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub parallel_size: usize,
}

#[async_trait]
impl Parallelizer for SnapshotParallelizer {
    fn get_name(&self) -> String {
        "SnapshotParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &DtQueue) -> anyhow::Result<Vec<DtItem>> {
        self.base_parallelizer.drain(buffer).await
    }

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        let data_size = DataSize {
            count: data.len() as u64,
            bytes: data.iter().map(|v| v.data_size as u64).sum(),
        };

        let sub_datas = Self::partition(data, self.parallel_size)?;
        self.base_parallelizer
            .sink_dml(sub_datas, sinkers, self.parallel_size, true)
            .await?;

        Ok(data_size)
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

        let sub_data_items = Self::partition(data, self.parallel_size)?;
        self.base_parallelizer
            .sink_raw(sub_data_items, sinkers, self.parallel_size, true)
            .await?;

        Ok(data_size)
    }
}

impl SnapshotParallelizer {
    pub fn partition<T>(data: Vec<T>, parallele_size: usize) -> anyhow::Result<Vec<Vec<T>>> {
        let mut sub_data_items = Vec::new();
        if parallele_size <= 1 {
            sub_data_items.push(data);
            return Ok(sub_data_items);
        }

        let avg_size = data.len() / parallele_size + 1;
        for _ in 0..parallele_size {
            sub_data_items.push(Vec::with_capacity(avg_size));
        }

        let mut i = 0;
        for item in data {
            sub_data_items[i].push(item);
            if sub_data_items[i].len() >= avg_size {
                i += 1;
            }
        }
        Ok(sub_data_items)
    }
}
