use std::sync::Arc;

use async_trait::async_trait;

use crate::{snapshot_parallelizer::SnapshotParallelizer, DataSize, Parallelizer};
use dt_common::{
    config::{sinker_config::SinkerConfig, task_config::TaskConfig},
    meta::{
        dt_data::{DtData, DtItem},
        dt_queue::DtQueue,
    },
    monitor::counter::Counter,
};
use dt_connector::Sinker;

pub struct FoxlakeParallelizer {
    pub task_config: TaskConfig,
    pub snapshot_parallelizer: SnapshotParallelizer,
}

#[async_trait]
impl Parallelizer for FoxlakeParallelizer {
    fn get_name(&self) -> String {
        "FoxlakeParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &DtQueue) -> anyhow::Result<Vec<DtItem>> {
        match self.task_config.sinker {
            SinkerConfig::FoxlakeMerge { .. } => self.drain_foxlake(buffer).await,
            _ => self.snapshot_parallelizer.drain(buffer).await,
        }
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

        match &self.task_config.sinker {
            SinkerConfig::FoxlakePush {
                batch_memory_mb, ..
            } => {
                sinkers[0].lock().await.refresh_meta(Vec::new()).await?;
                // to avoid generating too many small orc files, each sub_data size
                // should be as big as possible(smaller than sinker batch_memory_mb)
                if *batch_memory_mb > 0 {
                    let sub_data_items =
                        Self::partition(data, sinkers.len(), batch_memory_mb * 1024 * 1024)?;
                    self.snapshot_parallelizer
                        .base_parallelizer
                        .sink_raw(sub_data_items, sinkers, sinkers.len(), true)
                        .await?
                }
            }
            _ => {
                self.snapshot_parallelizer.sink_raw(data, sinkers).await?;
            }
        }

        Ok(data_size)
    }
}

impl FoxlakeParallelizer {
    async fn drain_foxlake(&mut self, buffer: &DtQueue) -> anyhow::Result<Vec<DtItem>> {
        let mut record_size_counter = Counter::new(0, 0);
        let base = &mut self.snapshot_parallelizer.base_parallelizer;
        let mut data = Vec::new();

        let mut last_push_epoch = 0;
        let mut first_sequencer_id = 0;
        if let Some(item) = base.popped_data.front() {
            if let DtData::Foxlake { file_meta } = &item.dt_data {
                first_sequencer_id = file_meta.sequencer_id
            }
        }

        // pop to find the push_epoch of the last item
        while let Ok(item) = base.pop(buffer, &mut record_size_counter).await {
            if let DtData::Foxlake { file_meta } = &item.dt_data {
                last_push_epoch = file_meta.push_epoch;
                let sequencer_id = file_meta.sequencer_id;
                base.popped_data.push_back(item);

                if first_sequencer_id == 0 {
                    first_sequencer_id = sequencer_id;
                }
                if sequencer_id != first_sequencer_id {
                    break;
                }
            }
        }

        while let Some(item) = base.popped_data.front() {
            if let DtData::Foxlake { file_meta } = &item.dt_data {
                // To improve foxlake performance:
                // 1, the batch should NOT contain duplicate data, so
                // the batch should only contain orc files from the same sequencer_id,
                // because a different sequencer_id means the pusher process has restarted,
                // the first few files of a new pusher may contain duplicate data
                // with the last few files of the previous pusher.

                // 2, all orc files of the same push_epoch must be merged in the same batch.
                // we choose to not merge the files of last_push_epoch
                // since we are not sure whether all the files of last_push_epoch exist in popped_data.

                // 3. the push_epoch of finished is i64::MAX, which ensures all orc files
                // in popped_data will be merged once the finished file exists in popped_data.
                if file_meta.sequencer_id == first_sequencer_id
                    && file_meta.push_epoch < last_push_epoch
                {
                    data.push(base.popped_data.pop_front().unwrap())
                } else {
                    break;
                }
            }
        }

        base.update_monitor(&record_size_counter).await;
        Ok(data)
    }

    pub fn partition(
        data: Vec<DtItem>,
        parallele_size: usize,
        sinker_batch_memory_bytes: usize,
    ) -> anyhow::Result<Vec<Vec<DtItem>>> {
        let mut sub_data_items = Vec::new();
        sub_data_items.push(Vec::new());

        if parallele_size <= 1 {
            sub_data_items[0] = data;
            return Ok(sub_data_items);
        }

        let mut i = 0;
        let mut sub_data_size = 0;
        for item in data {
            if sub_data_size + item.dt_data.get_data_size() > sinker_batch_memory_bytes as u64 {
                sub_data_items.push(Vec::new());
                i += 1;
                sub_data_size = 0;
            }

            sub_data_size += item.dt_data.get_data_size();
            sub_data_items[i].push(item);
        }
        Ok(sub_data_items)
    }
}
