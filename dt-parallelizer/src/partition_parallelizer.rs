use std::sync::Arc;

use async_trait::async_trait;

use super::{base_parallelizer::BaseParallelizer, rdb_partitioner::RdbPartitioner};
use crate::{DataSize, Parallelizer};
use dt_common::meta::{
    dt_data::{DtData, DtItem},
    dt_queue::DtQueue,
    row_data::RowData,
};
use dt_common::monitor::counter::Counter;
use dt_connector::Sinker;

pub struct PartitionParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub partitioner: RdbPartitioner,
    pub parallel_size: usize,
}

#[async_trait]
impl Parallelizer for PartitionParallelizer {
    fn get_name(&self) -> String {
        "PartitionParallelizer".to_string()
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.partitioner.close().await
    }

    async fn drain(&mut self, buffer: &DtQueue) -> anyhow::Result<Vec<DtItem>> {
        let mut data = Vec::new();
        let mut record_size_counter = Counter::new(0, 0);
        while let Ok(item) = self
            .base_parallelizer
            .pop(buffer, &mut record_size_counter)
            .await
        {
            match &item.dt_data {
                DtData::Dml { row_data } => {
                    if self.parallel_size > 1
                        && !self.partitioner.can_be_partitioned(row_data).await?
                    {
                        data.push(item);
                        break;
                    } else {
                        data.push(item);
                    }
                }

                DtData::Commit { .. } => {
                    data.push(item);
                }

                _ => {}
            }
        }

        self.base_parallelizer
            .update_monitor(&record_size_counter)
            .await;
        Ok(data)
    }

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        let data_size = DataSize {
            count: data.len() as u64,
            bytes: data.iter().map(|v| v.get_data_size()).sum(),
        };

        let sub_data = self.partitioner.partition(data, self.parallel_size).await?;
        self.base_parallelizer
            .sink_dml(sub_data, sinkers, self.parallel_size, false)
            .await?;

        Ok(data_size)
    }
}
