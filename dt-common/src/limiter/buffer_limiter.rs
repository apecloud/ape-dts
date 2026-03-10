use crate::{
    config::pipeline_config::PipelineConfig,
    limiter::limiter::{Limiter, UnitType},
    meta::dt_data::DtItem,
};

pub struct BufferLimiter {
    limiters: Vec<Box<dyn Limiter + Send + Sync>>,
}

impl BufferLimiter {
    pub fn from_config(config: &PipelineConfig) -> Option<Self> {
        let mut limiters: Vec<Box<dyn Limiter + Send + Sync>> = Vec::new();
        if config.max_rps > 0 && config.max_rps <= u32::MAX as u64 {
            limiters.push(Box::new(crate::limiter::rate_limiter::RateLimiter::new(
                config.max_rps as u32,
                UnitType::Records,
            )));
        }
        if config.max_bps > 0 && config.max_bps <= u32::MAX as u64 {
            limiters.push(Box::new(crate::limiter::rate_limiter::RateLimiter::new(
                config.max_bps as u32,
                UnitType::Bytes,
            )));
        }
        if config.buffer_size > 0 {
            limiters.push(Box::new(
                crate::limiter::capacity_limiter::CapacityLimiter::new(
                    config.buffer_size,
                    UnitType::Records,
                ),
            ));
        }
        if config.buffer_memory_mb > 0
            && (config.buffer_memory_mb as u64) <= (usize::MAX as u64) / (1024 * 1024)
        {
            let capacity_bytes = (config.buffer_memory_mb as u64) * 1024 * 1024;
            limiters.push(Box::new(
                crate::limiter::capacity_limiter::CapacityLimiter::new(
                    capacity_bytes as usize,
                    UnitType::Bytes,
                ),
            ));
        }
        if limiters.is_empty() {
            None
        } else {
            Some(Self { limiters })
        }
    }

    pub async fn acquire(&self, dt_item: &DtItem) -> anyhow::Result<()> {
        for limiter in &self.limiters {
            match limiter.get_unit_type().await {
                UnitType::Bytes => {
                    let size = dt_item.dt_data.get_data_size() as u32;
                    limiter.acquire(size).await?;
                }
                UnitType::Records => {
                    limiter.acquire(1).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn release(&self, dt_item: &DtItem) {
        for limiter in &self.limiters {
            match limiter.get_unit_type().await {
                UnitType::Bytes => {
                    let size = dt_item.dt_data.get_data_size() as u32;
                    limiter.release(size).await;
                }
                UnitType::Records => {
                    limiter.release(1).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::{Duration, Instant};

    use tokio::sync::Barrier;

    use crate::config::config_enums::PipelineType;
    use crate::config::pipeline_config::PipelineConfig;
    use crate::meta::dt_data::{DtData, DtItem};
    use crate::meta::foxlake::s3_file_meta::S3FileMeta;
    use crate::meta::position::Position;

    use super::BufferLimiter;

    // ── helpers ──────────────────────────────────────────────────────────────

    fn build_pipeline_config(
        max_rps: u64,
        max_bps: u64,
        buffer_size: usize,
        buffer_memory_mb: usize,
    ) -> PipelineConfig {
        PipelineConfig {
            pipeline_type: PipelineType::Basic,
            buffer_size,
            buffer_memory_mb,
            max_rps,
            max_bps,
            checkpoint_interval_secs: 1,
            batch_sink_interval_secs: 1,
            counter_time_window_secs: 1,
            counter_max_sub_count: 1000,
            http_host: "0.0.0.0".to_string(),
            http_port: 10231,
            with_field_defs: true,
        }
    }

    /// A record item: `get_data_size()` == 0 → only record-unit limiters apply.
    /// Do NOT combine with an active `max_bps` limiter (would panic on `acquire(0)`).
    fn record_item() -> DtItem {
        DtItem {
            dt_data: DtData::Begin {},
            position: Position::None,
            data_origin_node: "test".to_string(),
        }
    }

    /// A bytes item: `get_data_size()` == `data_size` → both record and byte
    /// limiters apply.
    fn bytes_item(data_size: usize) -> DtItem {
        DtItem {
            dt_data: DtData::Foxlake {
                file_meta: S3FileMeta {
                    data_size,
                    ..Default::default()
                },
            },
            position: Position::None,
            data_origin_node: "test".to_string(),
        }
    }

    /// Atomically update a peak counter with a CAS loop.
    fn update_peak(peak: &AtomicUsize, current: usize) {
        loop {
            let prev = peak.load(Ordering::SeqCst);
            if current <= prev {
                break;
            }
            if peak
                .compare_exchange(prev, current, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
    }

    // ── parameter 1: max_rps (rate limiter – records/s) ──────────────────────
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn max_rps_throttles_record_throughput_multithread() {
        const RPS: u64 = 5;
        const TASKS: usize = 10;

        let limiter =
            Arc::new(BufferLimiter::from_config(&build_pipeline_config(RPS, 0, 0, 0)).unwrap());
        let item = Arc::new(record_item());
        let barrier = Arc::new(Barrier::new(TASKS));

        let start = Instant::now();
        let handles: Vec<_> = (0..TASKS)
            .map(|_| {
                let limiter = limiter.clone();
                let item = item.clone();
                let barrier = barrier.clone();
                tokio::spawn(async move {
                    barrier.wait().await; // all tasks race together
                    limiter.acquire(&item).await.unwrap();
                    limiter.release(&item).await;
                })
            })
            .collect();

        for h in handles {
            h.await.unwrap();
        }

        assert!(
            start.elapsed() >= Duration::from_secs(1),
            "max_rps={RPS} did not throttle: finished in {:?}",
            start.elapsed()
        );
    }

    // ── parameter 2: max_bps (rate limiter – bytes/s) ────────────────────────
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn max_bps_throttles_byte_throughput_multithread() {
        const BPS: u64 = 100; // 100 bytes / sec
        const ITEM_BYTES: usize = 100;
        const TASKS: usize = 3;

        let limiter =
            Arc::new(BufferLimiter::from_config(&build_pipeline_config(0, BPS, 0, 0)).unwrap());
        let item = Arc::new(bytes_item(ITEM_BYTES));
        let barrier = Arc::new(Barrier::new(TASKS));

        let start = Instant::now();
        let handles: Vec<_> = (0..TASKS)
            .map(|_| {
                let limiter = limiter.clone();
                let item = item.clone();
                let barrier = barrier.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    limiter.acquire(&item).await.unwrap();
                    limiter.release(&item).await;
                })
            })
            .collect();

        for h in handles {
            h.await.unwrap();
        }

        // 3 × 100 B at 100 B/s ≈ 3 s; assert ≥ 1 s for CI robustness
        assert!(
            start.elapsed() >= Duration::from_secs(1),
            "max_bps={BPS} did not throttle: finished in {:?}",
            start.elapsed()
        );
    }

    // ── parameter 3: buffer_size (capacity limiter – records) ────────────────
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn buffer_size_caps_concurrent_records_multithread() {
        const CAPACITY: usize = 2;
        const TASKS: usize = 6;

        let limiter = Arc::new(
            BufferLimiter::from_config(&build_pipeline_config(0, 0, CAPACITY, 0)).unwrap(),
        );
        let item = Arc::new(record_item());
        let barrier = Arc::new(Barrier::new(TASKS));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..TASKS)
            .map(|_| {
                let limiter = limiter.clone();
                let item = item.clone();
                let barrier = barrier.clone();
                let in_flight = in_flight.clone();
                let max_in_flight = max_in_flight.clone();

                tokio::spawn(async move {
                    barrier.wait().await;
                    limiter.acquire(&item).await.unwrap();

                    let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    update_peak(&max_in_flight, current);

                    tokio::time::sleep(Duration::from_millis(50)).await;

                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    limiter.release(&item).await;
                })
            })
            .collect();

        for h in handles {
            h.await.unwrap();
        }

        let peak = max_in_flight.load(Ordering::SeqCst);
        assert!(
            peak <= CAPACITY,
            "buffer_size={CAPACITY} violated: {peak} tasks in flight simultaneously"
        );
        assert!(peak >= 1, "no task was ever observed in-flight");
    }

    // ── parameter 4: buffer_memory_mb (capacity limiter – bytes) ─────────────
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn buffer_memory_mb_caps_concurrent_bytes_multithread() {
        const MEM_MB: usize = 1;
        const ITEM_BYTES: usize = 400_000; // floor(1_048_576 / 400_000) == 2
        const CAPACITY: usize = 1_048_576 / ITEM_BYTES; // == 2
        const TASKS: usize = 6;

        let limiter =
            Arc::new(BufferLimiter::from_config(&build_pipeline_config(0, 0, 0, MEM_MB)).unwrap());
        let item = Arc::new(bytes_item(ITEM_BYTES));
        let barrier = Arc::new(Barrier::new(TASKS));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..TASKS)
            .map(|_| {
                let limiter = limiter.clone();
                let item = item.clone();
                let barrier = barrier.clone();
                let in_flight = in_flight.clone();
                let max_in_flight = max_in_flight.clone();

                tokio::spawn(async move {
                    barrier.wait().await;
                    limiter.acquire(&item).await.unwrap();

                    let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    update_peak(&max_in_flight, current);

                    tokio::time::sleep(Duration::from_millis(50)).await;

                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    limiter.release(&item).await;
                })
            })
            .collect();

        for h in handles {
            h.await.unwrap();
        }

        let peak = max_in_flight.load(Ordering::SeqCst);
        assert!(
            peak <= CAPACITY,
            "buffer_memory_mb={MEM_MB} (max {CAPACITY} items) violated: {peak} items in flight"
        );
        assert!(peak >= 1, "no task was ever observed in-flight");
    }

    // ── all four parameters combined ─────────────────────────────────────────
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn all_four_params_enforce_capacity_concurrently_multithread() {
        const ITEM_BYTES: usize = 100;
        const BUF_SIZE: usize = 2;
        const TASKS: usize = 6;

        let limiter = Arc::new(
            BufferLimiter::from_config(&build_pipeline_config(
                10_000,     // max_rps  – high, non-binding
                10_000_000, // max_bps  – high, non-binding
                BUF_SIZE,   // buffer_size – binding
                1,          // buffer_memory_mb – non-binding for 100-byte items
            ))
            .unwrap(),
        );
        let item = Arc::new(bytes_item(ITEM_BYTES));
        let barrier = Arc::new(Barrier::new(TASKS));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..TASKS)
            .map(|_| {
                let limiter = limiter.clone();
                let item = item.clone();
                let barrier = barrier.clone();
                let in_flight = in_flight.clone();
                let max_in_flight = max_in_flight.clone();
                let completed = completed.clone();

                tokio::spawn(async move {
                    barrier.wait().await;
                    limiter.acquire(&item).await.unwrap();

                    let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    update_peak(&max_in_flight, current);

                    tokio::time::sleep(Duration::from_millis(30)).await;

                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    limiter.release(&item).await;
                    completed.fetch_add(1, Ordering::SeqCst);
                })
            })
            .collect();

        for h in handles {
            h.await.unwrap();
        }

        let peak = max_in_flight.load(Ordering::SeqCst);
        assert!(
            peak <= BUF_SIZE,
            "buffer_size={BUF_SIZE} violated: {peak} tasks in flight simultaneously"
        );
        assert_eq!(
            completed.load(Ordering::SeqCst),
            TASKS,
            "not all {TASKS} tasks completed"
        );
    }

    // ── acquire / release permit symmetry ────────────────────────────────────
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn capacity_permits_fully_restored_after_release() {
        const CAPACITY: usize = 3;

        let limiter = Arc::new(
            BufferLimiter::from_config(&build_pipeline_config(0, 0, CAPACITY, 0)).unwrap(),
        );
        let item = Arc::new(record_item());

        // Exhaust and return all permits sequentially.
        for _ in 0..CAPACITY {
            limiter.acquire(&item).await.unwrap();
            limiter.release(&item).await;
        }

        // Now all CAPACITY tasks should acquire without blocking.
        let barrier = Arc::new(Barrier::new(CAPACITY));
        let start = Instant::now();

        let handles: Vec<_> = (0..CAPACITY)
            .map(|_| {
                let limiter = limiter.clone();
                let item = item.clone();
                let barrier = barrier.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    limiter.acquire(&item).await.unwrap();
                    limiter.release(&item).await;
                })
            })
            .collect();

        for h in handles {
            h.await.unwrap();
        }

        assert!(
            start.elapsed() < Duration::from_millis(500),
            "permits not restored after release: second round took {:?}",
            start.elapsed()
        );
    }

    // ── memory capacity: byte-level blocking ─────────────────────────────────
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn buffer_memory_mb_blocks_when_bytes_exhausted() {
        // 1 MB = 1_048_576 bytes; one item = 800_000 bytes.
        // After first item: 248_576 bytes remain → second item blocks.
        const MEM_MB: usize = 1;
        const ITEM_BYTES: usize = 800_000;

        let limiter =
            Arc::new(BufferLimiter::from_config(&build_pipeline_config(0, 0, 0, MEM_MB)).unwrap());
        let item = Arc::new(bytes_item(ITEM_BYTES));

        // First slot – should succeed immediately.
        limiter.acquire(&item).await.unwrap();

        // Second slot – will block until the first is released.
        let limiter2 = limiter.clone();
        let item2 = item.clone();
        let second_acquired = Arc::new(AtomicUsize::new(0));
        let second_acquired2 = second_acquired.clone();

        let handle = tokio::spawn(async move {
            limiter2.acquire(&item2).await.unwrap();
            second_acquired2.store(1, Ordering::SeqCst);
            limiter2.release(&item2).await;
        });

        // Give the spawned task time to reach acquire and block.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            second_acquired.load(Ordering::SeqCst),
            0,
            "second item should still be blocked"
        );

        // Unblock: release the first item.
        limiter.release(&item).await;
        handle.await.unwrap();

        assert_eq!(
            second_acquired.load(Ordering::SeqCst),
            1,
            "second item should have completed after release"
        );
    }
}
