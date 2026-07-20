use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkerWorkerMetricsSnapshot {
    pub configured: u64,
    pub busy: u64,
}

#[derive(Debug, Default)]
pub struct SinkerWorkerMetrics {
    configured: AtomicU64,
    busy: AtomicU64,
}

#[derive(Debug)]
pub struct SinkerWorkerRecorder {
    metrics: Arc<SinkerWorkerMetrics>,
}

#[derive(Debug)]
pub struct SinkerWorkerBusyGuard<'a> {
    recorder: &'a SinkerWorkerRecorder,
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
}

impl SinkerWorkerRecorder {
    pub fn enter(&self) -> SinkerWorkerBusyGuard<'_> {
        self.metrics.busy.fetch_add(1, Ordering::Relaxed);
        SinkerWorkerBusyGuard { recorder: self }
    }
}

impl Drop for SinkerWorkerBusyGuard<'_> {
    fn drop(&mut self) {
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
}
