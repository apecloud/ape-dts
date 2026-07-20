use std::{collections::VecDeque, future::Future, sync::Arc};

use anyhow::bail;
use async_mutex::Mutex;
use concurrent_queue::PopError;
use tokio::task::JoinSet;

use dt_common::{
    meta::{
        dcl_meta::dcl_data::DclData,
        ddl_meta::ddl_data::DdlData,
        dt_data::DtItem,
        dt_queue::{DtQueue, DtQueuePopError},
        row_data::RowData,
    },
    monitor::{
        counter::Counter, counter_type::CounterType, task_monitor_handle::TaskMonitorHandle,
    },
};
use dt_connector::Sinker;

type SharedSinker = Arc<Mutex<Box<dyn Sinker + Send>>>;

#[derive(Default)]
pub struct BaseParallelizer {
    pub popped_data: VecDeque<DtItem>,
    pub monitor: TaskMonitorHandle,
}

impl BaseParallelizer {
    pub async fn drain(&mut self, buffer: &DtQueue) -> anyhow::Result<Vec<DtItem>> {
        let mut data = Vec::new();
        while let Some(item) = self.popped_data.pop_front() {
            data.push(item);
        }

        let mut record_size_counter = Counter::new(0, 0);
        // ddls and dmls should be drained separately
        while let Some(item) = self.pop(buffer, &mut record_size_counter).await? {
            if data.is_empty()
                || (data[0].get_row_sql_type() == item.get_row_sql_type()
                    && data[0].data_origin_node == item.data_origin_node)
            {
                // merge when sql type is the same
                data.push(item);
            } else {
                self.popped_data.push_back(item);
                break;
            }
        }

        self.update_monitor(&record_size_counter).await;
        Ok(data)
    }

    pub async fn drain_by_count(
        &mut self,
        buffer: &DtQueue,
        max_count: usize,
    ) -> anyhow::Result<Vec<DtItem>> {
        let mut data = Vec::new();
        let mut record_size_counter = Counter::new(0, 0);
        while let Some(item) = self.pop(buffer, &mut record_size_counter).await? {
            data.push(item);
            if data.len() >= max_count {
                break;
            }
        }
        self.update_monitor(&record_size_counter).await;
        Ok(data)
    }

    pub async fn pop(
        &self,
        buffer: &DtQueue,
        record_size_counter: &mut Counter,
    ) -> anyhow::Result<Option<DtItem>> {
        match buffer.pop().await {
            Ok(item) => {
                record_size_counter.add(
                    item.dt_data.get_data_size(),
                    item.dt_data.get_data_count() as u64,
                );
                Ok(Some(item))
            }
            Err(DtQueuePopError::Queue(PopError::Empty)) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn update_monitor(&self, record_size_counter: &Counter) {
        if record_size_counter.value > 0 {
            self.monitor
                .add_batch_counter(
                    self.monitor.default_task_id(),
                    CounterType::RecordSize,
                    record_size_counter.value,
                    record_size_counter.count,
                )
                .await;
        }
    }

    pub async fn sink_dml(
        &self,
        sub_data_items: Vec<Vec<RowData>>,
        sinkers: &[SharedSinker],
        parallel_size: usize,
        batch: bool,
    ) -> anyhow::Result<()> {
        let workers_used = self
            .sink_by_available_sinker(
                sub_data_items,
                sinkers,
                parallel_size,
                move |sinker, data| async move { sinker.lock().await.sink_dml(data, batch).await },
            )
            .await?;
        self.record_workers_per_drain(workers_used).await;
        Ok(())
    }

    pub async fn sink_ddl(
        &self,
        sub_data_items: Vec<Vec<DdlData>>,
        sinkers: &[SharedSinker],
        parallel_size: usize,
        batch: bool,
    ) -> anyhow::Result<()> {
        let workers_used = self
            .sink_by_available_sinker(
                sub_data_items,
                sinkers,
                parallel_size,
                move |sinker, data| async move { sinker.lock().await.sink_ddl(data, batch).await },
            )
            .await?;
        self.record_workers_per_drain(workers_used).await;
        Ok(())
    }

    pub async fn sink_dcl(
        &self,
        sub_data_items: Vec<Vec<DclData>>,
        sinkers: &[SharedSinker],
        parallel_size: usize,
        batch: bool,
    ) -> anyhow::Result<()> {
        let workers_used = self
            .sink_by_available_sinker(
                sub_data_items,
                sinkers,
                parallel_size,
                move |sinker, data| async move { sinker.lock().await.sink_dcl(data, batch).await },
            )
            .await?;
        self.record_workers_per_drain(workers_used).await;
        Ok(())
    }

    pub async fn sink_raw(
        &self,
        sub_data_items: Vec<Vec<DtItem>>,
        sinkers: &[SharedSinker],
        parallel_size: usize,
        batch: bool,
    ) -> anyhow::Result<()> {
        let workers_used = self
            .sink_by_available_sinker(
                sub_data_items,
                sinkers,
                parallel_size,
                move |sinker, data| async move { sinker.lock().await.sink_raw(data, batch).await },
            )
            .await?;
        self.record_workers_per_drain(workers_used).await;
        Ok(())
    }

    async fn sink_by_available_sinker<T, Run, Fut>(
        &self,
        sub_data_items: Vec<Vec<T>>,
        sinkers: &[SharedSinker],
        parallel_size: usize,
        run: Run,
    ) -> anyhow::Result<usize>
    where
        T: Send + 'static,
        Run: Fn(SharedSinker, Vec<T>) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        if sub_data_items.is_empty() {
            return Ok(0);
        }
        if parallel_size < 1 {
            bail!("parallel_size must be greater than 0");
        }
        if sinkers.is_empty() {
            bail!("sinkers must not be empty");
        }

        let mut pending = sub_data_items.into_iter();
        let active_sinkers = parallel_size.min(sinkers.len());
        let mut join_set = JoinSet::new();
        let spawn_sink_task = |join_set: &mut JoinSet<anyhow::Result<(usize, bool)>>,
                               sinker_index: usize,
                               worker_used: bool,
                               sinker: SharedSinker,
                               data: Vec<T>,
                               run: Run| {
            join_set.spawn(async move {
                run(sinker, data).await?;
                Ok((sinker_index, worker_used))
            });
        };

        for (sinker_index, sinker) in sinkers.iter().enumerate().take(active_sinkers) {
            let Some(data) = pending.next() else {
                break;
            };
            spawn_sink_task(
                &mut join_set,
                sinker_index,
                !data.is_empty(),
                sinker.clone(),
                data,
                run.clone(),
            );
        }

        let mut workers_used_count = 0;
        while let Some(result) = join_set.join_next().await {
            let (sinker_index, worker_used) = result??;
            if let Some(data) = pending.next() {
                let worker_used = worker_used || !data.is_empty();
                spawn_sink_task(
                    &mut join_set,
                    sinker_index,
                    worker_used,
                    sinkers[sinker_index].clone(),
                    data,
                    run.clone(),
                );
            } else if worker_used {
                workers_used_count += 1;
            }
        }

        Ok(workers_used_count)
    }

    pub async fn record_workers_per_drain(&self, workers_used_count: usize) {
        if workers_used_count == 0 {
            return;
        }
        self.monitor
            .add_batch_counter(
                self.monitor.default_task_id(),
                CounterType::SinkerWorkersPerDrain,
                workers_used_count as u64,
                1,
            )
            .await;
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, hint::black_box, sync::Arc, time::Instant};

    use async_mutex::Mutex;
    use dt_common::{
        config::config_enums::{TaskKind, TaskType},
        meta::dt_queue::DtQueue,
        monitor::{
            counter::Counter, monitor::Monitor, task_monitor::MonitorType,
            task_monitor::TaskMonitor, task_monitor_handle::TaskMonitorHandle,
        },
    };
    use dt_connector::{sinker::dummy_sinker::DummySinker, Sinker};
    use tokio::task::JoinSet;

    use super::BaseParallelizer;

    async fn dispatch_without_worker_count(
        sub_data_items: Vec<Vec<usize>>,
        sinkers: &[Arc<Mutex<Box<dyn Sinker + Send>>>],
        parallel_size: usize,
    ) {
        let mut pending = sub_data_items.into_iter();
        let active_sinkers = parallel_size.min(sinkers.len());
        let mut join_set = JoinSet::new();
        let spawn_sink_task = |join_set: &mut JoinSet<usize>,
                               sinker_index: usize,
                               sinker: Arc<Mutex<Box<dyn Sinker + Send>>>,
                               data: Vec<usize>| {
            join_set.spawn(async move {
                black_box(sinker);
                black_box(data);
                sinker_index
            });
        };

        for (sinker_index, sinker) in sinkers.iter().enumerate().take(active_sinkers) {
            let Some(data) = pending.next() else {
                break;
            };
            spawn_sink_task(&mut join_set, sinker_index, sinker.clone(), data);
        }

        while let Some(result) = join_set.join_next().await {
            let sinker_index = result.unwrap();
            if let Some(data) = pending.next() {
                spawn_sink_task(
                    &mut join_set,
                    sinker_index,
                    sinkers[sinker_index].clone(),
                    data,
                );
            }
        }
    }

    #[tokio::test]
    async fn pop_returns_none_when_queue_is_empty() {
        let parallelizer = BaseParallelizer::default();
        let queue = DtQueue::new(1, 0, None, None);
        let mut counter = Counter::new(0, 0);

        let item = parallelizer.pop(&queue, &mut counter).await.unwrap();

        assert!(item.is_none());
    }

    #[tokio::test]
    async fn sink_by_available_sinker_counts_distinct_workers_with_non_empty_data() {
        let parallelizer = BaseParallelizer::default();
        let sinkers = (0..3)
            .map(|_| {
                Arc::new(Mutex::new(
                    Box::new(DummySinker {}) as Box<dyn Sinker + Send>
                ))
            })
            .collect::<Vec<_>>();

        let workers_used = parallelizer
            .sink_by_available_sinker(
                vec![vec![1_u8], Vec::new(), vec![2_u8]],
                &sinkers,
                3,
                |_sinker, _data| async { Ok(()) },
            )
            .await
            .unwrap();

        assert_eq!(workers_used, 2);

        let reused_worker = parallelizer
            .sink_by_available_sinker(
                vec![Vec::new(), vec![1_u8]],
                &sinkers[..1],
                1,
                |_sinker, _data| async { Ok(()) },
            )
            .await
            .unwrap();

        assert_eq!(reused_worker, 1);
    }
}
