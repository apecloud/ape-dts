use std::{cmp, sync::Arc};

use super::{base_parallelizer::BaseParallelizer, snapshot_parallelizer::SnapshotParallelizer};
use crate::{DataSize, Merger, Parallelizer};
use async_trait::async_trait;
use dt_common::config::sinker_config::BasicSinkerConfig;
use dt_common::meta::dcl_meta::dcl_data::DclData;
use dt_common::meta::ddl_meta::ddl_data::DdlData;
use dt_common::meta::dt_queue::DtQueue;
use dt_common::meta::{
    dt_data::DtItem, rdb_meta_manager::RdbMetaManager, row_data::RowData, row_type::RowType,
    struct_meta::struct_data::StructData,
};
use dt_connector::Sinker;

// Shared parallelizer for merge and checker flows.
pub struct MergeParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub merger: Box<dyn Merger + Send + Sync>,
    pub meta_manager: Option<RdbMetaManager>,
    pub parallel_size: usize,
    pub sinker_basic_config: BasicSinkerConfig,
    pub sink_mode: MergeSinkMode,
}

#[derive(Clone, Copy)]
pub enum MergeSinkMode {
    AdaptiveBatch,
    Partitioned,
}

enum MergeType {
    Insert,
    Delete,
    Unmerged,
}

pub struct TbMergedData {
    pub delete_rows: Vec<RowData>,
    pub insert_rows: Vec<RowData>,
    pub unmerged_rows: Vec<RowData>,
}

#[async_trait]
impl Parallelizer for MergeParallelizer {
    async fn close(&mut self) -> anyhow::Result<()> {
        if let Some(meta_manager) = &self.meta_manager {
            meta_manager.close().await?;
        }
        self.merger.close().await
    }

    fn get_name(&self) -> String {
        "MergeParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &DtQueue) -> anyhow::Result<Vec<DtItem>> {
        self.base_parallelizer.drain(buffer).await
    }

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        let mut data_size = DataSize::default();
        let mut tb_merged_data = self.merger.merge(data).await?;
        for merge_type in [MergeType::Delete, MergeType::Insert, MergeType::Unmerged] {
            data_size.add(match self.sink_mode {
                MergeSinkMode::AdaptiveBatch => {
                    self.sink_dml_adaptive(&mut tb_merged_data, sinkers, merge_type)
                        .await?
                }
                MergeSinkMode::Partitioned => {
                    self.sink_dml_partitioned(&mut tb_merged_data, sinkers, merge_type)
                        .await?
                }
            });
        }
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

        // ddl should always be executed serially
        self.base_parallelizer
            .sink_ddl(vec![data], sinkers, 1, false)
            .await?;

        Ok(data_size)
    }

    async fn sink_dcl(
        &mut self,
        data: Vec<DclData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        let data_size = DataSize {
            count: data.len() as u64,
            bytes: data.iter().map(|v| v.get_data_size()).sum(),
        };

        self.base_parallelizer
            .sink_dcl(vec![data], sinkers, 1, false)
            .await?;

        Ok(data_size)
    }

    async fn sink_struct(
        &mut self,
        data: Vec<StructData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        let count = data.len() as u64;
        sinkers[0].lock().await.sink_struct(data).await?;
        Ok(DataSize { count, bytes: 0 })
    }
}

impl MergeParallelizer {
    async fn sink_dml_adaptive(
        &mut self,
        tb_merged_data_items: &mut [TbMergedData],
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
        merge_type: MergeType,
    ) -> anyhow::Result<DataSize> {
        let mut futures = Vec::new();
        let mut data_size = DataSize::default();
        for tb_merged_data in tb_merged_data_items.iter_mut() {
            let data: Vec<RowData> = match merge_type {
                MergeType::Delete => tb_merged_data.delete_rows.drain(..).collect(),
                MergeType::Insert => tb_merged_data.insert_rows.drain(..).collect(),
                MergeType::Unmerged => tb_merged_data.unmerged_rows.drain(..).collect(),
            };
            if data.is_empty() {
                continue;
            }

            data_size
                .add_count(data.len() as u64)
                .add_bytes(data.iter().map(|v| v.get_data_size()).sum());

            // make sure NO too much threads generated
            let batch_size = cmp::max(
                data.len() / self.parallel_size,
                cmp::max(self.sinker_basic_config.batch_size, 1),
            );

            match merge_type {
                MergeType::Insert | MergeType::Delete => {
                    let mut remaining = data;
                    while !remaining.is_empty() {
                        let tail = if remaining.len() > batch_size {
                            remaining.split_off(batch_size)
                        } else {
                            Vec::new()
                        };
                        let sub_data = std::mem::replace(&mut remaining, tail);
                        let sinker = sinkers[futures.len() % self.parallel_size].clone();
                        let future = tokio::spawn(async move {
                            sinker.lock().await.sink_dml(sub_data, true).await
                        });
                        futures.push(future);
                    }
                }

                MergeType::Unmerged => {
                    let sinker = sinkers[futures.len() % self.parallel_size].clone();
                    let future =
                        tokio::spawn(async move { Self::sink_unmerged_rows(sinker, data).await });
                    futures.push(future);
                }
            }
        }

        for future in futures {
            future.await??;
        }
        Ok(data_size)
    }

    async fn sink_dml_partitioned(
        &mut self,
        tb_merged_data_items: &mut [TbMergedData],
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
        merge_type: MergeType,
    ) -> anyhow::Result<DataSize> {
        let mut data_size = DataSize::default();
        for tb_merged_data in tb_merged_data_items.iter_mut() {
            let data: Vec<RowData> = match merge_type {
                MergeType::Delete => tb_merged_data.delete_rows.drain(..).collect(),
                MergeType::Insert => tb_merged_data.insert_rows.drain(..).collect(),
                MergeType::Unmerged => tb_merged_data.unmerged_rows.drain(..).collect(),
            };
            if data.is_empty() {
                continue;
            }

            data_size
                .add_count(data.len() as u64)
                .add_bytes(data.iter().map(|v| v.get_data_size()).sum());

            let sub_data_items = SnapshotParallelizer::partition(data, self.parallel_size)?;
            self.base_parallelizer
                .sink_dml(
                    sub_data_items,
                    sinkers,
                    self.parallel_size,
                    matches!(merge_type, MergeType::Insert),
                )
                .await?;
        }

        Ok(data_size)
    }

    async fn sink_unmerged_rows(
        sinker: Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>,
        data: Vec<RowData>,
    ) -> anyhow::Result<()> {
        let mut remaining = data;
        while !remaining.is_empty() {
            let row_type = remaining[0].row_type.clone();
            let len = remaining
                .iter()
                .take_while(|row| row.row_type == row_type)
                .count();
            let tail = remaining.split_off(len);
            let sub_data = std::mem::replace(&mut remaining, tail);
            sinker
                .lock()
                .await
                .sink_dml(sub_data, matches!(row_type, RowType::Insert))
                .await?;
        }
        Ok(())
    }
}
