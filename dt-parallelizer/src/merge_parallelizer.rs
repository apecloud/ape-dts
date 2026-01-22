use std::{cmp, sync::Arc};

use super::base_parallelizer::BaseParallelizer;
use crate::{DataSize, Merger, Parallelizer};
use async_trait::async_trait;
use dt_common::config::sinker_config::BasicSinkerConfig;
use dt_common::meta::dcl_meta::dcl_data::DclData;
use dt_common::meta::ddl_meta::ddl_data::DdlData;
use dt_common::meta::dt_queue::DtQueue;
use dt_common::meta::{
    dt_data::DtItem, rdb_meta_manager::RdbMetaManager, row_data::RowData, row_type::RowType,
};
use dt_connector::Sinker;

pub struct MergeParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub merger: Box<dyn Merger + Send + Sync>,
    pub meta_manager: Option<RdbMetaManager>,
    pub parallel_size: usize,
    pub sinker_basic_config: BasicSinkerConfig,
}

enum MergeType {
    Insert,
    Delete,
    Unmerged,
}

pub struct TbMergedData {
    pub tb: String,
    pub delete_rows: Vec<Arc<RowData>>,
    pub insert_rows: Vec<Arc<RowData>>,
    pub unmerged_rows: Vec<Arc<RowData>>,
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
        data: Vec<Arc<RowData>>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        let mut data_size = DataSize::default();
        // no need to check foreign key since foreign key checks were disabled in MySQL/Postgres connections
        let mut tb_merged_data = self.merger.merge(data).await?;
        data_size.add(
            self.sink_dml_internal(&mut tb_merged_data, sinkers, MergeType::Delete)
                .await?,
        );
        data_size.add(
            self.sink_dml_internal(&mut tb_merged_data, sinkers, MergeType::Insert)
                .await?,
        );
        data_size.add(
            self.sink_dml_internal(&mut tb_merged_data, sinkers, MergeType::Unmerged)
                .await?,
        );
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
}

impl MergeParallelizer {
    async fn sink_dml_internal(
        &self,
        tb_merged_data_items: &mut [TbMergedData],
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
        merge_type: MergeType,
    ) -> anyhow::Result<DataSize> {
        let mut futures = Vec::new();
        let mut data_size = DataSize::default();
        for tb_merged_data in tb_merged_data_items.iter_mut() {
            let data: Vec<Arc<RowData>> = match merge_type {
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
                    let mut i = 0;
                    while i < data.len() {
                        let sub_size = cmp::min(batch_size, data.len() - i);
                        let sub_data = data[i..i + sub_size].to_vec();
                        let sinker = sinkers[futures.len() % self.parallel_size].clone();
                        let future = tokio::spawn(async move {
                            sinker.lock().await.sink_dml(sub_data, true).await.unwrap();
                        });
                        futures.push(future);
                        i += batch_size;
                    }
                }

                MergeType::Unmerged => {
                    let sinker = sinkers[futures.len() % self.parallel_size].clone();
                    let future = tokio::spawn(async move {
                        Self::sink_unmerged_rows(sinker, data).await.unwrap();
                    });
                    futures.push(future);
                }
            }
        }

        // wait for sub sinkers to finish and unwrap errors
        for future in futures {
            future.await.unwrap();
        }
        Ok(data_size)
    }

    async fn sink_unmerged_rows(
        sinker: Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>,
        data: Vec<Arc<RowData>>,
    ) -> anyhow::Result<()> {
        let mut start = 0;
        for i in 1..=data.len() {
            if i == data.len() || data[i].row_type != data[start].row_type {
                let sub_data = data[start..i].to_vec();
                if data[start].row_type == RowType::Insert {
                    sinker.lock().await.sink_dml(sub_data, true).await?;
                } else {
                    // for Delete / Update, the safest way is serial
                    sinker.lock().await.sink_dml(sub_data, false).await?;
                }
                start = i;
            }
        }
        Ok(())
    }
}
