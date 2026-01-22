pub mod base_parallelizer;
pub mod check_parallelizer;
pub mod foxlake_parallelizer;
pub mod merge_parallelizer;
pub mod mongo_merger;
pub mod partition_parallelizer;
pub mod rdb_merger;
pub mod rdb_partitioner;
pub mod redis_parallelizer;
pub mod serial_parallelizer;
pub mod snapshot_parallelizer;
pub mod table_parallelizer;

use std::sync::Arc;

use async_trait::async_trait;
use dt_common::meta::{
    dcl_meta::dcl_data::DclData, ddl_meta::ddl_data::DdlData, dt_data::DtItem, dt_queue::DtQueue,
    row_data::RowData, struct_meta::struct_data::StructData,
};
use dt_connector::Sinker;
use merge_parallelizer::TbMergedData;

#[async_trait]
pub trait Parallelizer {
    fn get_name(&self) -> String;

    async fn drain(&mut self, _buffer: &DtQueue) -> anyhow::Result<Vec<DtItem>> {
        Ok(Vec::new())
    }

    async fn sink_ddl(
        &mut self,
        _data: Vec<DdlData>,
        _sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        Ok(DataSize::default())
    }

    async fn sink_dml(
        &mut self,
        _data: Vec<Arc<RowData>>,
        _sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        Ok(DataSize::default())
    }

    async fn sink_dcl(
        &mut self,
        _data: Vec<DclData>,
        _sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        Ok(DataSize::default())
    }

    async fn sink_raw(
        &mut self,
        _data: Vec<DtItem>,
        _sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        Ok(DataSize::default())
    }

    async fn sink_struct(
        &mut self,
        _data: Vec<StructData>,
        _sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        Ok(DataSize::default())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait Merger {
    async fn merge(&mut self, data: Vec<Arc<RowData>>) -> anyhow::Result<Vec<TbMergedData>>;

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct DataSize {
    pub count: u64,
    pub bytes: u64,
}

impl DataSize {
    pub fn add(&mut self, other: DataSize) {
        self.count += other.count;
        self.bytes += other.bytes;
    }

    pub fn add_count(&mut self, count: u64) -> &mut Self {
        self.count += count;
        self
    }

    pub fn add_bytes(&mut self, bytes: u64) -> &mut Self {
        self.bytes += bytes;
        self
    }
}
