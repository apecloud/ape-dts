use std::sync::Arc;

use crate::{table_partitioner::TablePartitioner, DataSize, Parallelizer};
use async_trait::async_trait;
use dt_common::meta::{
    ddl_meta::ddl_data::DdlData, dt_data::DtItem, dt_queue::DtQueue, row_data::RowData,
};
use dt_connector::Sinker;

use super::base_parallelizer::BaseParallelizer;

pub struct TableParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub parallel_size: usize,
}

#[async_trait]
impl Parallelizer for TableParallelizer {
    fn get_name(&self) -> String {
        "TableParallelizer".to_string()
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
            bytes: data.iter().map(|v| v.get_data_size()).sum(),
        };

        let sub_data = TablePartitioner::partition_dml(data)?;
        self.base_parallelizer
            .sink_dml(sub_data, sinkers, self.parallel_size, false)
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

        let sub_data = TablePartitioner::partition_raw(data)?;
        self.base_parallelizer
            .sink_raw(sub_data, sinkers, self.parallel_size, false)
            .await?;

        Ok(data_size)
    }

    async fn sink_ddl(
        &mut self,
        data: Vec<DdlData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        let data_size = DataSize {
            count: data.len() as u64,
            bytes: data.iter().map(|v| v.get_data_size()).sum(),
        };

        self.base_parallelizer
            .sink_ddl(vec![data], sinkers, 1, false)
            .await?;

        Ok(data_size)
    }

    fn drain_ctl_data(&mut self) -> Vec<DtItem> {
        self.base_parallelizer.drain_ctl_data()
    }
}
