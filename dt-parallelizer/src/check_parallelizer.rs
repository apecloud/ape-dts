use std::sync::Arc;

use async_trait::async_trait;

use super::{base_parallelizer::BaseParallelizer, snapshot_parallelizer::SnapshotParallelizer};
use crate::{DataSize, Merger, Parallelizer};
use dt_common::meta::{
    dt_data::DtItem, dt_queue::DtQueue, row_data::RowData, struct_meta::struct_data::StructData,
};
use dt_connector::{checker::CheckerHandle, Sinker};

pub struct CheckParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub merger: Box<dyn Merger + Send + Sync>,
    pub parallel_size: usize,
    pub checker: Option<CheckerHandle>,
}

#[async_trait]
impl Parallelizer for CheckParallelizer {
    fn get_name(&self) -> String {
        "CheckParallelizer".to_string()
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        if let Some(checker) = &mut self.checker {
            checker.close().await?;
        }
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
        let mut check_data: Vec<Arc<RowData>> = Vec::new();

        let mut merged_data_items = self.merger.merge(data).await?;
        for tb_merged_data in merged_data_items.drain(..) {
            // delete first, then insert (same order as MergeParallelizer)
            let delete_data = tb_merged_data.delete_rows;
            if self.checker.is_some() {
                check_data.extend(delete_data.iter().cloned());
            }
            data_size
                .add_count(delete_data.len() as u64)
                .add_bytes(delete_data.iter().map(|v| v.get_data_size()).sum());
            let delete_sub_data_items =
                SnapshotParallelizer::partition(delete_data, self.parallel_size)?;
            self.base_parallelizer
                .sink_dml(delete_sub_data_items, sinkers, self.parallel_size, false)
                .await?;

            let batch_data = tb_merged_data.insert_rows;
            if self.checker.is_some() {
                check_data.extend(batch_data.iter().cloned());
            }
            data_size
                .add_count(batch_data.len() as u64)
                .add_bytes(batch_data.iter().map(|v| v.get_data_size()).sum());
            let batch_sub_data_items =
                SnapshotParallelizer::partition(batch_data, self.parallel_size)?;
            self.base_parallelizer
                .sink_dml(batch_sub_data_items, sinkers, self.parallel_size, true)
                .await?;

            let serial_data = tb_merged_data.unmerged_rows;
            if self.checker.is_some() {
                check_data.extend(serial_data.iter().cloned());
            }
            data_size
                .add_count(serial_data.len() as u64)
                .add_bytes(serial_data.iter().map(|v| v.get_data_size()).sum());
            let serial_sub_data_items =
                SnapshotParallelizer::partition(serial_data, self.parallel_size)?;
            self.base_parallelizer
                .sink_dml(serial_sub_data_items, sinkers, self.parallel_size, false)
                .await?;
        }

        if let Some(checker) = &self.checker {
            if !check_data.is_empty() {
                if let Err(err) = checker.check_rows(check_data).await {
                    log::warn!("checker sidecar failed: {}", err);
                }
            }
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
        let data_for_check = if self.checker.is_some() {
            Some(data.clone())
        } else {
            None
        };
        sinkers[0].lock().await.sink_struct(data).await?;
        if let (Some(checker), Some(data_for_check)) = (&self.checker, data_for_check) {
            if let Err(err) = checker.check_struct(data_for_check).await {
                log::warn!("checker sidecar failed: {}", err);
            }
        }
        Ok(data_size)
    }
}
