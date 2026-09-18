use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use tokio::time::Instant;

use super::pipeline_sink_metrics::PipelineSinkMetricsState;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SinkerWorkerMetricsSnapshot {
    pub configured: u64,
    pub busy: u64,
}

/// One pipeline's sinker pool. Completed samples live in its Monitor counters.
#[derive(Debug, Default)]
pub struct SinkerWorkerMetrics {
    configured: AtomicU64,
    busy: AtomicU64,
    pub(super) pipeline_sink: Mutex<Option<PipelineSinkMetricsState>>,
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

impl SinkerWorkerMetrics {
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

    pub(super) fn record_work(&self, elapsed: Duration) {
        if let Some(state) = self.pipeline_sink.lock().unwrap().as_mut() {
            state.record_work(elapsed);
        }
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

    pub fn enter_with_timer(&self, non_empty: bool) -> SinkerWorkerBusyGuard<'_> {
        let mut guard = self.enter();
        if non_empty && self.metrics.pipeline_sink.lock().unwrap().is_some() {
            guard.started_at = Some(Instant::now());
        }
        guard
    }
}

impl Drop for SinkerWorkerBusyGuard<'_> {
    fn drop(&mut self) {
        if let Some(started) = self.started_at {
            self.recorder.metrics.record_work(started.elapsed());
        }
        let previous = self.recorder.metrics.busy.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(previous > 0, "sinker worker count underflow");
    }
}

#[cfg(test)]
mod tests {
    use std::{hint::black_box, sync::Arc, time::Instant};

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

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "manual release-mode hot-path measurement"]
    async fn measures_tracker_hot_path_cost() {
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
        let started = Instant::now();
        for _ in 0..ITERATIONS {
            black_box(worker.enter_with_timer(true));
        }
        let idle_ns = started.elapsed().as_nanos() as f64 / f64::from(ITERATIONS);
        let batch = crate::monitor::pipeline_sink_metrics::PipelineSinkMetricsGuard::new(
            metrics.clone(),
            1,
            ITERATIONS as usize,
        );
        let started = Instant::now();
        for _ in 0..ITERATIONS {
            black_box(worker.enter_with_timer(true));
        }
        let active_ns = started.elapsed().as_nanos() as f64 / f64::from(ITERATIONS);
        eprintln!("sink guard: idle={idle_ns:.2} ns/call, active={active_ns:.2} ns/call");
        assert!(batch.finish().is_some());
        assert_eq!(metrics.snapshot().busy, 0);
    }
}
