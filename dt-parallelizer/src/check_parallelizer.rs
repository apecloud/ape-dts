use std::sync::Arc;

use async_trait::async_trait;

use super::{base_parallelizer::BaseParallelizer, snapshot_parallelizer::SnapshotParallelizer};
use crate::{DataSize, Merger, Parallelizer};
use dt_common::meta::{
    dt_data::DtItem, dt_queue::DtQueue, row_data::RowData, struct_meta::struct_data::StructData,
};
use dt_connector::Sinker;

pub struct CheckParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub merger: Box<dyn Merger + Send + Sync>,
    pub parallel_size: usize,
}

#[async_trait]
impl Parallelizer for CheckParallelizer {
    fn get_name(&self) -> String {
        "CheckParallelizer".to_string()
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.merger.close().await
    }

    async fn drain(&mut self, buffer: &DtQueue) -> anyhow::Result<Vec<DtItem>> {
        self.base_parallelizer.drain(buffer).await
    }

    async fn sink_dml(
        &mut self,
        data: Vec<Arc<RowData>>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        let mut data_size = DataSize::default();

        let mut merged_data_items = self.merger.merge(data).await?;
        for tb_merged_data in merged_data_items.drain(..) {
            let batch_data = tb_merged_data.insert_rows;
            data_size
                .add_count(batch_data.len() as u64)
                .add_bytes(batch_data.iter().map(|v| v.get_data_size()).sum());
            let batch_sub_data_items =
                SnapshotParallelizer::partition(batch_data, self.parallel_size)?;
            self.base_parallelizer
                .sink_dml(batch_sub_data_items, sinkers, self.parallel_size, true)
                .await?;

            let serial_data = tb_merged_data.unmerged_rows;
            data_size
                .add_count(serial_data.len() as u64)
                .add_bytes(serial_data.iter().map(|v| v.get_data_size()).sum());
            let serial_sub_data_items =
                SnapshotParallelizer::partition(serial_data, self.parallel_size)?;
            self.base_parallelizer
                .sink_dml(serial_sub_data_items, sinkers, self.parallel_size, false)
                .await?;
        }

        Ok(data_size)
    }

    async fn sink_struct(
        &mut self,
        data: Vec<StructData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        let data_size = DataSize {
            count: data.len() as u64,
            bytes: 0,
        };
        sinkers[0].lock().await.sink_struct(data).await?;
        Ok(data_size)
    }
}
