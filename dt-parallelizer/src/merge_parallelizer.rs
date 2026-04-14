use std::{cmp, sync::Arc};

use super::{base_parallelizer::BaseParallelizer, mongo_merger::MongoMerger};
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
            data_size.add(
                self.sink_dml_adaptive(&mut tb_merged_data, sinkers, merge_type)
                    .await?,
            );
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
    pub fn for_rdb_merge(
        base_parallelizer: BaseParallelizer,
        merger: Box<dyn Merger + Send + Sync>,
        parallel_size: usize,
        sinker_basic_config: BasicSinkerConfig,
        meta_manager: Option<RdbMetaManager>,
    ) -> Self {
        Self {
            base_parallelizer,
            merger,
            meta_manager,
            parallel_size,
            sinker_basic_config,
        }
    }

    pub fn for_check(
        base_parallelizer: BaseParallelizer,
        merger: Box<dyn Merger + Send + Sync>,
        parallel_size: usize,
        sinker_basic_config: BasicSinkerConfig,
    ) -> Self {
        Self::for_rdb_merge(
            base_parallelizer,
            merger,
            parallel_size,
            sinker_basic_config,
            None,
        )
    }

    pub fn for_mongo(
        base_parallelizer: BaseParallelizer,
        parallel_size: usize,
        sinker_basic_config: BasicSinkerConfig,
    ) -> Self {
        Self::for_rdb_merge(
            base_parallelizer,
            Box::new(MongoMerger {}),
            parallel_size,
            sinker_basic_config,
            None,
        )
    }

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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use dt_common::{
        config::sinker_config::BasicSinkerConfig,
        meta::{row_data::RowData, row_type::RowType},
    };
    use dt_connector::Sinker;

    use crate::{base_parallelizer::BaseParallelizer, Merger, Parallelizer};

    use super::{MergeParallelizer, TbMergedData};

    #[derive(Debug, PartialEq, Eq)]
    struct SinkCall {
        sinker_id: usize,
        batch: bool,
        row_count: usize,
    }

    struct RecordingSinker {
        sinker_id: usize,
        calls: Arc<Mutex<Vec<SinkCall>>>,
    }

    #[async_trait]
    impl Sinker for RecordingSinker {
        async fn sink_dml(&mut self, data: Vec<RowData>, batch: bool) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push(SinkCall {
                sinker_id: self.sinker_id,
                batch,
                row_count: data.len(),
            });
            Ok(())
        }
    }

    struct PassthroughMerger;

    #[async_trait]
    impl Merger for PassthroughMerger {
        async fn merge(&mut self, data: Vec<RowData>) -> anyhow::Result<Vec<TbMergedData>> {
            Ok(vec![TbMergedData {
                delete_rows: data,
                insert_rows: Vec::new(),
                unmerged_rows: Vec::new(),
            }])
        }
    }

    #[tokio::test]
    async fn check_mode_delete_rows_keep_adaptive_multi_sinker_dispatch() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let sinkers = (0..2)
            .map(|sinker_id| {
                Arc::new(async_mutex::Mutex::new(Box::new(RecordingSinker {
                    sinker_id,
                    calls: calls.clone(),
                })
                    as Box<dyn Sinker + Send>))
            })
            .collect::<Vec<_>>();

        let mut parallelizer = MergeParallelizer::for_check(
            BaseParallelizer::default(),
            Box::new(PassthroughMerger),
            2,
            BasicSinkerConfig {
                batch_size: 1,
                ..Default::default()
            },
        );

        let data = vec![
            RowData::new("test".into(), "tb".into(), RowType::Delete, None, None),
            RowData::new("test".into(), "tb".into(), RowType::Delete, None, None),
            RowData::new("test".into(), "tb".into(), RowType::Delete, None, None),
            RowData::new("test".into(), "tb".into(), RowType::Delete, None, None),
        ];

        parallelizer.sink_dml(data, &sinkers).await.unwrap();

        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert!(calls.iter().all(|call| call.batch));
        assert!(calls.iter().any(|call| call.sinker_id == 0));
        assert!(calls.iter().any(|call| call.sinker_id == 1));
        assert!(calls.iter().all(|call| call.row_count == 2));
    }
}
