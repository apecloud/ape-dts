use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::time::Instant;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SinkerWorkerMetricsSnapshot {
    pub configured: u64,
    pub busy: u64,
}

/// Task-wide worker counts and work time for the current pipeline sink operation.
#[derive(Debug)]
pub struct SinkerWorkerMetrics {
    configured: AtomicU64,
    busy: AtomicU64,
    // MAX disables timing, including after an overflow or a cancelled operation.
    work_ns: AtomicU64,
}

#[derive(Debug)]
pub struct SinkerWorkerRecorder {
    metrics: Arc<SinkerWorkerMetrics>,
}

#[derive(Debug)]
pub struct SinkerWorkerBusyGuard<'a> {
    recorder: &'a SinkerWorkerRecorder,
    started_at: Option<Instant>,
}

impl Default for SinkerWorkerMetrics {
    fn default() -> Self {
        Self {
            configured: AtomicU64::new(0),
            busy: AtomicU64::new(0),
            work_ns: AtomicU64::new(u64::MAX),
        }
    }
}

impl SinkerWorkerMetrics {
    /// A task has one pipeline with sequential sink operations. All workers from
    /// the previous operation must have exited before starting another one,
    /// including after errors or cancellation.
    pub fn start_pipeline_sink(self: &Arc<Self>, parallelism: usize) -> PipelineSinkMetricsGuard {
        PipelineSinkMetricsGuard::new(self.clone(), parallelism)
    }

    pub fn register_worker(self: &Arc<Self>) -> SinkerWorkerRecorder {
        self.configured.fetch_add(1, Ordering::Relaxed);
        SinkerWorkerRecorder {
            metrics: self.clone(),
        }
    }

    pub fn snapshot(&self) -> SinkerWorkerMetricsSnapshot {
        SinkerWorkerMetricsSnapshot {
            configured: self.configured.load(Ordering::Relaxed),
            busy: self.busy.load(Ordering::Relaxed),
        }
    }

    fn record_work(&self, elapsed: Duration) {
        let ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        let _ = self
            .work_ns
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                (value != u64::MAX).then(|| value.saturating_add(ns))
            });
    }
}

impl SinkerWorkerRecorder {
    pub fn enter(&self) -> SinkerWorkerBusyGuard<'_> {
        self.metrics.busy.fetch_add(1, Ordering::Relaxed);
        SinkerWorkerBusyGuard {
            recorder: self,
            started_at: None,
        }
    }

    pub fn enter_with_timer(&self) -> SinkerWorkerBusyGuard<'_> {
        let mut guard = self.enter();
        if self.metrics.work_ns.load(Ordering::Relaxed) != u64::MAX {
            guard.started_at = Some(Instant::now());
        }
        guard
    }
}

impl Drop for SinkerWorkerBusyGuard<'_> {
    fn drop(&mut self) {
        if let Some(started_at) = self.started_at {
            self.recorder.metrics.record_work(started_at.elapsed());
        }
        let previous = self.recorder.metrics.busy.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(previous > 0, "sinker worker count underflow");
    }
}

/// Measures one BaseParallelizer::sink_dml call. Only finish produces a sample.
/// Owns the lifecycle of the task's shared work counter for one operation.
pub struct PipelineSinkMetricsGuard {
    started: Instant,
    parallelism: usize,
    metrics: Arc<SinkerWorkerMetrics>,
}

impl PipelineSinkMetricsGuard {
    fn new(metrics: Arc<SinkerWorkerMetrics>, parallelism: usize) -> Self {
        metrics.work_ns.store(0, Ordering::Relaxed);
        Self {
            started: Instant::now(),
            parallelism,
            metrics,
        }
    }

    #[cfg(test)]
    pub(crate) fn record_work(&self, elapsed: Duration) {
        self.metrics.record_work(elapsed);
    }

    /// Called only after every partition has joined successfully.
    pub fn finish(self) -> Option<(Duration, f64)> {
        let duration = self.started.elapsed();
        if self.parallelism == 0 || duration.is_zero() {
            return None;
        }
        let work_ns = self.metrics.work_ns.load(Ordering::Relaxed);
        let capacity_ns = duration.as_nanos().checked_mul(self.parallelism as u128)?;
        // MAX marks an invalid measurement, such as a work duration overflow.
        if work_ns == u64::MAX || u128::from(work_ns) > capacity_ns {
            return None;
        }
        Some((duration, work_ns as f64 / capacity_ns as f64))
    }
}

impl Drop for PipelineSinkMetricsGuard {
    fn drop(&mut self) {
        self.metrics.work_ns.store(u64::MAX, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        hint::black_box,
        sync::{atomic::Ordering, Arc},
        time::{Duration, Instant},
    };

    use super::SinkerWorkerMetrics;

    #[test]
    fn tracks_configured_and_current_busy_workers() {
        let metrics = Arc::new(SinkerWorkerMetrics::default());
        let worker_1 = metrics.register_worker();
        let worker_2 = metrics.register_worker();

        let first = worker_1.enter();
        let second = worker_2.enter();
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.configured, 2);
        assert_eq!(snapshot.busy, 2);

        drop(second);
        assert_eq!(metrics.snapshot().busy, 1);
        drop(first);
        assert_eq!(metrics.snapshot().busy, 0);
    }

    #[test]
    #[ignore = "manual release-mode hot-path measurement"]
    fn measures_tracker_hot_path_cost() {
        const ITERATIONS: u32 = 1_000_000;

        let metrics = Arc::new(SinkerWorkerMetrics::default());
        let worker = metrics.register_worker();
        let started = Instant::now();
        for _ in 0..ITERATIONS {
            black_box(worker.enter());
        }
        let elapsed = started.elapsed();
        let nanoseconds_per_operation = elapsed.as_nanos() as f64 / f64::from(ITERATIONS);

        eprintln!("sinker worker tracker: {nanoseconds_per_operation:.2} ns/enter+drop");
        assert_eq!(metrics.snapshot().busy, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_operation_disables_timing_until_workers_exit_and_next_operation_starts() {
        let metrics = Arc::new(SinkerWorkerMetrics::default());
        let worker = metrics.register_worker();
        let cancelled = metrics.start_pipeline_sink(1);
        let late_guard = worker.enter_with_timer();
        tokio::time::advance(Duration::from_millis(10)).await;
        drop(cancelled);
        tokio::time::advance(Duration::from_millis(20)).await;
        drop(late_guard);
        assert_eq!(metrics.work_ns.load(Ordering::Relaxed), u64::MAX);
        assert_eq!(metrics.snapshot().busy, 0);

        // Reuse is supported only after all workers from the old operation exit.
        let next = metrics.start_pipeline_sink(1);
        let next_guard = worker.enter_with_timer();
        tokio::time::advance(Duration::from_millis(20)).await;
        drop(next_guard);
        assert_eq!(next.finish().unwrap().1, 1.0);
        assert_eq!(metrics.work_ns.load(Ordering::Relaxed), u64::MAX);
    }
}
