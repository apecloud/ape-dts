use std::{collections::VecDeque, future::Future, sync::Arc};

use async_mutex::Mutex;
use concurrent_queue::PopError;
use dt_common::{
    error::DtError,
    meta::{
        dcl_meta::dcl_data::DclData,
        ddl_meta::ddl_data::DdlData,
        dt_data::DtItem,
        dt_queue::{DtQueue, DtQueuePopError},
        row_data::RowData,
    },
    monitor::{
        counter::Counter, counter_type::CounterType, monitor::Monitor,
        sinker_worker_metrics::PipelineSinkMetricsGuard, task_monitor_handle::TaskMonitorHandle,
    },
};
use dt_connector::Sinker;
use tokio::task::JoinSet;

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
        if record_size_counter
            .value
            .as_u64()
            .is_some_and(|value| value > 0)
        {
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
        let monitor = self.monitor.pipeline_sink_monitor().map(|monitor| {
            let monitor_guard = self
                .monitor
                .sinker_worker_metrics()
                .start_pipeline_sink(parallel_size.min(sinkers.len()));
            (monitor, monitor_guard)
        });
        let workers_used = self
            .sink_by_available_sinker(
                sub_data_items,
                sinkers,
                parallel_size,
                monitor,
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
                None,
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
                None,
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
                None,
                move |sinker, data| async move { sinker.lock().await.sink_raw(data, batch).await },
            )
            .await?;
        self.record_workers_per_drain(workers_used).await;
        Ok(())
    }

    async fn sink_by_available_sinker<T, Run, Fut>(
        &self,
        mut sub_data_items: Vec<Vec<T>>,
        sinkers: &[SharedSinker],
        parallel_size: usize,
        monitor: Option<(Arc<Monitor>, PipelineSinkMetricsGuard)>,
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
            return Err(DtError::invalid_config("parallelizer configuration is invalid").into());
        }
        if sinkers.is_empty() {
            return Err(
                DtError::InvariantViolated("parallelizer invariant violated".to_string()).into(),
            );
        }

        if monitor.is_some() {
            sub_data_items.retain(|data| !data.is_empty());
            if sub_data_items.is_empty() {
                return Ok(0);
            }
        }
        let active_sinkers = parallel_size.min(sinkers.len());
        let mut pending = sub_data_items.into_iter();
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

        if let Some((monitor, monitor_guard)) = monitor {
            TaskMonitorHandle::record_pipeline_sink_metrics(&monitor, monitor_guard).await;
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
    use std::sync::Arc;

    use async_mutex::Mutex;
    use dt_common::{meta::dt_queue::DtQueue, monitor::counter::Counter};
    use dt_connector::{sinker::dummy_sinker::DummySinker, Sinker};

    use super::BaseParallelizer;

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
                None,
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
                None,
                |_sinker, _data| async { Ok(()) },
            )
            .await
            .unwrap();

        assert_eq!(reused_worker, 1);
    }

    use std::time::Duration;

    use async_trait::async_trait;
    use dt_common::{
        meta::{row_data::RowData, row_type::RowType},
        monitor::{
            counter_type::CounterType, monitor::Monitor, sinker_worker_metrics::SinkerWorkerMetrics,
        },
    };
    use dt_connector::sinker::busy_tracking_sinker::BusyTrackingSinker;

    struct TimedSinker;

    #[async_trait]
    impl Sinker for TimedSinker {
        async fn sink_dml(&mut self, data: Vec<RowData>, _batch: bool) -> anyhow::Result<()> {
            let millis = data.iter().map(|row| row.data_size as u64).sum();
            tokio::time::sleep(Duration::from_millis(millis)).await;
            if data.iter().any(|row| row.tb == "fail") {
                anyhow::bail!("test sink failure")
            }
            if data.iter().any(|row| row.tb == "panic") {
                panic!("test sink panic")
            }
            Ok(())
        }
    }

    fn row(millis: usize) -> RowData {
        let mut row = RowData::new(
            String::new(),
            "schema".into(),
            "table".into(),
            1,
            RowType::Insert,
            None,
            None,
        );
        row.data_size = millis;
        row
    }

    fn sinkers(
        count: usize,
        workers: Arc<SinkerWorkerMetrics>,
    ) -> (Vec<super::SharedSinker>, Arc<SinkerWorkerMetrics>) {
        let sinkers = (0..count)
            .map(|_| {
                Arc::new(Mutex::new(Box::new(BusyTrackingSinker::new(
                    Box::new(TimedSinker),
                    workers.register_worker(),
                )) as Box<dyn Sinker + Send>))
            })
            .collect();
        (sinkers, workers)
    }

    #[derive(Clone)]
    struct TestMetrics {
        monitor: Arc<Monitor>,
        workers: Arc<SinkerWorkerMetrics>,
    }

    fn batch_metrics() -> TestMetrics {
        let monitor = Arc::new(Monitor::new("pipeline", "test", 60, 100, 1));
        for counter in [
            CounterType::PipelineSinkOperationsTotal,
            CounterType::PipelineSinkParallelUtilization,
            CounterType::PipelineSinkDurationSeconds,
        ] {
            monitor.init_counter(counter);
        }
        TestMetrics {
            monitor,
            workers: Arc::default(),
        }
    }

    async fn measurements(metrics: &TestMetrics) -> (u64, f64, f64) {
        let operations = metrics
            .monitor
            .no_window_counters
            .get(&CounterType::PipelineSinkOperationsTotal)
            .unwrap()
            .value
            .as_u64()
            .unwrap();
        let utilization = metrics
            .monitor
            .time_window_counters
            .get(&CounterType::PipelineSinkParallelUtilization)
            .unwrap()
            .clone();
        let duration = metrics
            .monitor
            .time_window_counters
            .get(&CounterType::PipelineSinkDurationSeconds)
            .unwrap()
            .clone();
        (
            operations,
            utilization.statistics().await.avg_by_count.as_f64(),
            duration.statistics().await.latest.as_f64(),
        )
    }

    async fn run_batch(
        data: Vec<Vec<RowData>>,
        sinkers: &[super::SharedSinker],
        parallelism: usize,
        metrics: TestMetrics,
    ) -> anyhow::Result<usize> {
        let monitor_guard = metrics
            .workers
            .start_pipeline_sink(parallelism.min(sinkers.len()));
        BaseParallelizer::default()
            .sink_by_available_sinker(
                data,
                sinkers,
                parallelism,
                Some((metrics.monitor, monitor_guard)),
                |sinker, data| async move { sinker.lock().await.sink_dml(data, true).await },
            )
            .await
    }

    #[tokio::test(start_paused = true)]
    async fn measures_balanced_skewed_insufficient_and_serial_batches() {
        for (costs, parallelism, available, utilization, elapsed) in [
            (vec![100, 100, 100, 100], 4, 4, 1.0, 0.1),
            (vec![100, 10, 10, 10], 4, 4, 0.325, 0.1),
            (vec![100, 100], 4, 4, 0.5, 0.1),
            (vec![100, 50], 1, 4, 1.0, 0.15),
            (vec![100, 100], 8, 2, 1.0, 0.1),
            (vec![50, 50, 50, 50, 50, 50, 50, 50], 4, 4, 1.0, 0.1),
        ] {
            let metrics = batch_metrics();
            let (sinkers, workers) = sinkers(available, metrics.workers.clone());
            run_batch(
                costs.into_iter().map(|cost| vec![row(cost)]).collect(),
                &sinkers,
                parallelism,
                metrics.clone(),
            )
            .await
            .unwrap();
            let snapshot = measurements(&metrics).await;
            assert_eq!(snapshot.0, 1);
            assert!((snapshot.1 - utilization).abs() < 1e-10, "{snapshot:?}");
            assert!((snapshot.2 - elapsed).abs() < 1e-10, "{snapshot:?}");
            assert_eq!(workers.snapshot().busy, 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn failed_and_panicked_batches_do_not_publish_metrics() {
        for mode in ["fail", "panic"] {
            let metrics = batch_metrics();
            let (sinkers, workers) = sinkers(2, metrics.workers.clone());
            run_batch(vec![Vec::new()], &sinkers, 2, metrics.clone())
                .await
                .unwrap();
            assert_eq!(measurements(&metrics).await.0, 0);
            let mut bad = row(10);
            bad.tb = mode.into();
            assert!(run_batch(
                vec![vec![bad], vec![row(1000)]],
                &sinkers,
                2,
                metrics.clone()
            )
            .await
            .is_err());
            assert_eq!(measurements(&metrics).await.0, 0);
            // Wait for the other, already-started worker to release its guard.
            for sinker in &sinkers {
                drop(sinker.lock().await);
            }
            assert_eq!(workers.snapshot().busy, 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_and_mutex_wait_are_excluded_from_work() {
        let metrics = batch_metrics();
        let (sinkers, workers) = sinkers(1, metrics.workers.clone());
        let held = sinkers[0].lock().await;
        let task_sinkers = sinkers.clone();
        let task_metrics = metrics.clone();
        let task = tokio::spawn(async move {
            run_batch(vec![vec![row(100)]], &task_sinkers, 1, task_metrics).await
        });
        // First yield dispatches, second lets the partition wait on the mutex.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        assert_eq!(workers.snapshot().busy, 0);
        drop(held);
        task.await.unwrap().unwrap();
        assert_eq!(measurements(&metrics).await.1, 0.5);

        let task_sinkers = sinkers.clone();
        let task_metrics = metrics.clone();
        let task = tokio::spawn(async move {
            run_batch(vec![vec![row(1000)]], &task_sinkers, 1, task_metrics).await
        });
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        assert_eq!(workers.snapshot().busy, 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        assert_eq!(workers.snapshot().busy, 0);
        assert_eq!(measurements(&metrics).await.0, 1);
        run_batch(vec![vec![row(100)]], &sinkers, 1, metrics.clone())
            .await
            .unwrap();
        let (count, utilization, duration) = measurements(&metrics).await;
        assert_eq!(count, 2);
        assert_eq!(utilization, 0.75);
        assert!((duration - 0.1).abs() < 1e-12);
    }
}
