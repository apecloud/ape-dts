use std::{sync::Arc, time::Duration};

use tokio::time::Instant;

use super::sinker_worker_metrics::SinkerWorkerMetrics;

/// Temporary totals for one sink operation; cumulative values stay in Monitor counters.
#[derive(Debug, Default)]
pub(super) struct PipelineSinkMetricsState {
    work_ns: u64,
    calls: usize,
    overflowed: bool,
}

impl PipelineSinkMetricsState {
    pub(super) fn record_work(&mut self, elapsed: Duration) {
        match u64::try_from(elapsed.as_nanos())
            .ok()
            .and_then(|ns| self.work_ns.checked_add(ns))
        {
            Some(work_ns) => self.work_ns = work_ns,
            None => self.overflowed = true,
        }
        self.calls += 1;
    }
}

/// Measures one BaseParallelizer::sink_dml call. Only finish produces a sample.
///
/// Operations on a pipeline run sequentially. After failure/cancellation, the
/// pipeline stops; callers reusing a pool must first wait for all old workers to exit.
pub struct PipelineSinkMetricsGuard {
    started: Instant,
    parallelism: usize,
    expected_calls: usize,
    metrics: Arc<SinkerWorkerMetrics>,
}

impl PipelineSinkMetricsGuard {
    pub fn new(
        metrics: Arc<SinkerWorkerMetrics>,
        parallelism: usize,
        expected_calls: usize,
    ) -> Self {
        {
            let mut state = metrics.pipeline_sink.lock().unwrap();
            // Release the lock before panicking so misuse cannot poison Drop cleanup.
            let idle = state.is_none() && metrics.snapshot().busy == 0;
            if !idle {
                drop(state);
                panic!("pipeline sink metrics require sequential operations and idle workers");
            }
            *state = Some(PipelineSinkMetricsState::default());
        }
        Self {
            started: Instant::now(),
            parallelism,
            expected_calls,
            metrics,
        }
    }

    #[cfg(test)]
    pub(crate) fn record_work(&self, elapsed: Duration) {
        self.metrics.record_work(elapsed);
    }

    /// Return duration and utilization after every partition has joined.
    pub fn finish(self) -> Option<(Duration, f64)> {
        let duration = self.started.elapsed();
        let state = self.metrics.pipeline_sink.lock().unwrap().take()?;
        if self.parallelism == 0 || self.expected_calls == 0 || duration.is_zero() {
            return None;
        }
        let capacity_ns = duration.as_nanos().checked_mul(self.parallelism as u128)?;
        if state.overflowed
            || state.calls != self.expected_calls
            || u128::from(state.work_ns) > capacity_ns
        {
            log::warn!("discarding incomplete or invalid pipeline sink measurement");
            return None;
        }
        Some((duration, state.work_ns as f64 / capacity_ns as f64))
    }
}

impl Drop for PipelineSinkMetricsGuard {
    fn drop(&mut self) {
        // On failure/cancellation, late worker guards see None and only release busy.
        self.metrics.pipeline_sink.lock().unwrap().take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitor::{
        counter_type::CounterType, monitor::Monitor, task_monitor_handle::TaskMonitorHandle,
    };

    fn record_sample(monitor: &Monitor, work_ms: u64, duration_ms: u64, parallelism: usize) {
        let mut guard =
            PipelineSinkMetricsGuard::new(monitor.sinker_worker_metrics(), parallelism, 1);
        guard.started -= Duration::from_millis(duration_ms);
        guard.record_work(Duration::from_millis(work_ms));
        TaskMonitorHandle::record_pipeline_sink_metrics(monitor, guard);
    }

    #[tokio::test(start_paused = true)]
    async fn accumulates_all_batches_without_expiration() {
        let monitor = Monitor::new("pipeline", "test", 1, 1, 1);
        record_sample(&monitor, 400, 100, 4);
        record_sample(&monitor, 400, 400, 4);
        {
            let utilization = monitor
                .no_window_counters
                .get(&CounterType::PipelineSinkParallelUtilization)
                .unwrap();
            assert_eq!(utilization.count, 2);
            assert_eq!(utilization.avg_by_count().as_f64(), 0.625);
            assert_eq!(utilization.min.as_f64(), 0.25);
            assert_eq!(utilization.max.as_f64(), 1.0);
            let duration = monitor
                .no_window_counters
                .get(&CounterType::PipelineSinkDurationSeconds)
                .unwrap();
            assert_eq!(duration.value.as_f64(), 0.5);
            assert_eq!(duration.min.as_f64(), 0.1);
            assert_eq!(duration.max.as_f64(), 0.4);
            assert_eq!(duration.avg_by_count().as_f64(), 0.25);
        }
        tokio::time::advance(Duration::from_secs(86400)).await;
        for _ in 0..2000 {
            record_sample(&monitor, 0, 100, 1);
        }
        let utilization = monitor
            .no_window_counters
            .get(&CounterType::PipelineSinkParallelUtilization)
            .unwrap();
        assert_eq!(utilization.count, 2002);
        assert_eq!(utilization.min.as_f64(), 0.0);
        assert_eq!(utilization.avg_by_count().as_f64(), 1.25 / 2002.0);
        assert!(
            (monitor
                .no_window_counters
                .get(&CounterType::PipelineSinkDurationSeconds)
                .unwrap()
                .value
                .as_f64()
                - 200.5)
                .abs()
                < 1e-9
        );
        assert_eq!(
            monitor
                .no_window_counters
                .get(&CounterType::PipelineSinkOperationsTotal)
                .unwrap()
                .value
                .as_u64(),
            Some(2002)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sink_guard_only_times_nonempty_calls_in_its_pipeline_operation() {
        let workers = Arc::new(SinkerWorkerMetrics::default());
        let worker = workers.register_worker();
        drop(worker.enter_with_timer(true)); // no batch context
        let batch = PipelineSinkMetricsGuard::new(workers.clone(), 1, 1);
        let empty = worker.enter_with_timer(false);
        tokio::time::advance(Duration::from_millis(5)).await;
        drop(empty);
        let control = worker.enter();
        tokio::time::advance(Duration::from_millis(5)).await;
        drop(control);
        let sink = worker.enter_with_timer(true);
        assert_eq!(workers.snapshot().busy, 1);
        tokio::time::advance(Duration::from_millis(10)).await;
        drop(sink);
        assert_eq!(workers.snapshot().busy, 0);
        let (_, utilization) = batch.finish().unwrap();
        assert_eq!(utilization, 0.5);
    }

    #[tokio::test(start_paused = true)]
    async fn rejects_zero_missing_and_invalid_measurements() {
        assert!(PipelineSinkMetricsGuard::new(Arc::default(), 1, 1)
            .finish()
            .is_none());
        for (parallelism, expected, calls, work) in [
            (0, 1, 1, 1),
            (1, 0, 0, 0),
            (1, 1, 0, 0),
            (1, 1, 2, 1),
            (1, 1, 1, 101),
        ] {
            let mut batch = PipelineSinkMetricsGuard::new(Arc::default(), parallelism, expected);
            batch.started -= Duration::from_millis(100);
            for _ in 0..calls {
                batch.record_work(Duration::from_millis(work));
            }
            assert!(batch.finish().is_none());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_operation_discards_late_work_and_allows_reuse_after_workers_exit() {
        let metrics = Arc::new(SinkerWorkerMetrics::default());
        let worker = metrics.register_worker();
        let cancelled = PipelineSinkMetricsGuard::new(metrics.clone(), 1, 1);
        let late_guard = worker.enter_with_timer(true);
        tokio::time::advance(Duration::from_millis(10)).await;
        drop(cancelled);
        assert!(metrics.pipeline_sink.lock().unwrap().is_none());
        tokio::time::advance(Duration::from_millis(20)).await;
        drop(late_guard);
        assert!(metrics.pipeline_sink.lock().unwrap().is_none());
        assert_eq!(metrics.snapshot().busy, 0);

        let next = PipelineSinkMetricsGuard::new(metrics.clone(), 1, 1);
        let work = worker.enter_with_timer(true);
        tokio::time::advance(Duration::from_millis(20)).await;
        drop(work);
        let (_, utilization) = next.finish().unwrap();
        assert_eq!(utilization, 1.0);
    }

    #[test]
    fn rejects_overlapping_operations_or_reuse_before_workers_exit() {
        use std::panic::{catch_unwind, AssertUnwindSafe};

        let metrics = Arc::new(SinkerWorkerMetrics::default());
        let first = PipelineSinkMetricsGuard::new(metrics.clone(), 1, 1);
        assert!(catch_unwind(AssertUnwindSafe(|| {
            PipelineSinkMetricsGuard::new(metrics.clone(), 1, 1)
        }))
        .is_err());
        let worker = metrics.register_worker();
        let late_guard = worker.enter_with_timer(true);
        drop(first);
        assert!(catch_unwind(AssertUnwindSafe(|| {
            PipelineSinkMetricsGuard::new(metrics.clone(), 1, 1)
        }))
        .is_err());
        drop(late_guard);
        drop(PipelineSinkMetricsGuard::new(metrics.clone(), 1, 1));
    }

    #[tokio::test(start_paused = true)]
    async fn rejects_work_duration_overflow() {
        let metrics = Arc::new(SinkerWorkerMetrics::default());
        let guard = PipelineSinkMetricsGuard::new(metrics, 2, 2);
        guard.record_work(Duration::from_nanos(u64::MAX));
        guard.record_work(Duration::from_nanos(1));
        // Capacity exceeds the retained work, so only overflow invalidates it.
        tokio::time::advance(Duration::from_nanos(u64::MAX)).await;
        assert!(guard.finish().is_none());
    }
}
