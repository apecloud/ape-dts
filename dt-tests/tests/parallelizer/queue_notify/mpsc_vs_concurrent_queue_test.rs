use std::{
    fmt::Write,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use concurrent_queue::{ConcurrentQueue, PopError, PushError};
use tokio::{
    runtime::Builder,
    sync::{mpsc, Notify, Semaphore},
    task::JoinSet,
};

use super::bench_util::{
    duration_ms, duration_us, expected_checksum, item_value, simulate_producer_sleep,
    simulate_sink, start_timer, write_report, BenchCase, BenchRun, CpuTimer, QueueStats,
};

struct NotifyQueue {
    queue: ConcurrentQueue<u64>,
    not_empty: Notify,
    not_full: Notify,
    empty_waits: AtomicU64,
    full_waits: AtomicU64,
    push_retries_after_wake: AtomicU64,
}

struct SemaphoreQueue {
    queue: ConcurrentQueue<u64>,
    available_slots: Semaphore,
    not_empty: Notify,
    empty_waits: AtomicU64,
    full_waits: AtomicU64,
}

impl NotifyQueue {
    fn new(capacity: usize) -> Self {
        Self {
            queue: ConcurrentQueue::bounded(capacity),
            not_empty: Notify::new(),
            not_full: Notify::new(),
            empty_waits: AtomicU64::new(0),
            full_waits: AtomicU64::new(0),
            push_retries_after_wake: AtomicU64::new(0),
        }
    }

    async fn push(&self, mut item: u64) -> anyhow::Result<()> {
        let mut woke_from_full = false;
        loop {
            match self.queue.push(item) {
                Ok(()) => {
                    self.not_empty.notify_one();
                    return Ok(());
                }
                Err(PushError::Full(returned_item)) => {
                    if woke_from_full {
                        self.push_retries_after_wake.fetch_add(1, Ordering::Relaxed);
                    }
                    item = returned_item;
                    self.full_waits.fetch_add(1, Ordering::Relaxed);
                    self.not_full.notified().await;
                    woke_from_full = true;
                }
                Err(err) => return Err(err.into()),
            }
        }
    }

    async fn pop(&self) -> anyhow::Result<u64> {
        loop {
            match self.queue.pop() {
                Ok(item) => {
                    self.not_full.notify_one();
                    return Ok(item);
                }
                Err(PopError::Empty) => {
                    self.empty_waits.fetch_add(1, Ordering::Relaxed);
                    self.not_empty.notified().await;
                }
                Err(err) => return Err(err.into()),
            }
        }
    }

    fn try_pop(&self) -> anyhow::Result<Option<u64>> {
        match self.queue.pop() {
            Ok(item) => {
                self.not_full.notify_one();
                Ok(Some(item))
            }
            Err(PopError::Empty) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn stats(&self) -> QueueStats {
        QueueStats {
            full_waits: self.full_waits.load(Ordering::Relaxed),
            empty_waits: self.empty_waits.load(Ordering::Relaxed),
            push_retries_after_wake: self.push_retries_after_wake.load(Ordering::Relaxed),
        }
    }
}

impl SemaphoreQueue {
    fn new(capacity: usize) -> Self {
        Self {
            queue: ConcurrentQueue::unbounded(),
            available_slots: Semaphore::new(capacity),
            not_empty: Notify::new(),
            empty_waits: AtomicU64::new(0),
            full_waits: AtomicU64::new(0),
        }
    }

    async fn push(&self, item: u64) -> anyhow::Result<()> {
        match self.available_slots.try_acquire() {
            Ok(slot) => slot.forget(),
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                self.full_waits.fetch_add(1, Ordering::Relaxed);
                let slot = self.available_slots.acquire().await?;
                slot.forget();
            }
            Err(err) => return Err(err.into()),
        }

        self.queue.push(item)?;
        self.not_empty.notify_one();
        Ok(())
    }

    async fn pop(&self) -> anyhow::Result<u64> {
        loop {
            match self.queue.pop() {
                Ok(item) => {
                    self.available_slots.add_permits(1);
                    return Ok(item);
                }
                Err(PopError::Empty) => {
                    self.empty_waits.fetch_add(1, Ordering::Relaxed);
                    self.not_empty.notified().await;
                }
                Err(err) => return Err(err.into()),
            }
        }
    }

    fn try_pop(&self) -> anyhow::Result<Option<u64>> {
        match self.queue.pop() {
            Ok(item) => {
                self.available_slots.add_permits(1);
                Ok(Some(item))
            }
            Err(PopError::Empty) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn stats(&self) -> QueueStats {
        QueueStats {
            full_waits: self.full_waits.load(Ordering::Relaxed),
            empty_waits: self.empty_waits.load(Ordering::Relaxed),
            push_retries_after_wake: 0,
        }
    }
}

fn should_sleep_after_producer_batch(
    item_index: usize,
    items_per_producer: usize,
    producer_batch_size: usize,
) -> bool {
    let produced_items = item_index + 1;
    produced_items < items_per_producer && produced_items % producer_batch_size == 0
}

async fn run_mpsc_case(case: BenchCase) -> anyhow::Result<BenchRun> {
    let (tx, mut rx) = mpsc::channel(case.capacity);
    let cpu_timer = CpuTimer::start();
    let start = start_timer();
    let mut producers = JoinSet::new();

    for producer_id in 0..case.producers {
        let tx = tx.clone();
        let items_per_producer = case.items_per_producer();
        let producer_batch_size = case.producer_batch_size;
        let producer_sleep = case.producer_sleep;
        producers.spawn(async move {
            for item_index in 0..items_per_producer {
                tx.send(item_value(producer_id, item_index)).await?;
                if should_sleep_after_producer_batch(
                    item_index,
                    items_per_producer,
                    producer_batch_size,
                ) {
                    simulate_producer_sleep(producer_sleep).await;
                }
            }
            anyhow::Ok(())
        });
    }
    drop(tx);

    let mut received = 0;
    let mut checksum = 0;
    let mut batch = Vec::with_capacity(case.sink_batch_size);
    while received < case.total_items {
        let item = rx
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("mpsc channel closed before all items were received"))?;
        batch.push(item);
        while batch.len() < case.sink_batch_size {
            match rx.try_recv() {
                Ok(item) => batch.push(item),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        let batch_len = batch.len();
        for item in batch.drain(..) {
            checksum ^= item;
            received += 1;
        }
        simulate_sink(batch_len, case.sink_delay).await;
    }

    while let Some(result) = producers.join_next().await {
        result??;
    }

    Ok(BenchRun {
        elapsed: start.elapsed(),
        cpu_elapsed: cpu_timer.elapsed(),
        checksum,
        stats: QueueStats::default(),
    })
}

async fn run_notify_queue_case(case: BenchCase) -> anyhow::Result<BenchRun> {
    let queue = Arc::new(NotifyQueue::new(case.capacity));
    let cpu_timer = CpuTimer::start();
    let start = start_timer();
    let mut producers = JoinSet::new();

    for producer_id in 0..case.producers {
        let queue = queue.clone();
        let items_per_producer = case.items_per_producer();
        let producer_batch_size = case.producer_batch_size;
        let producer_sleep = case.producer_sleep;
        producers.spawn(async move {
            for item_index in 0..items_per_producer {
                queue.push(item_value(producer_id, item_index)).await?;
                if should_sleep_after_producer_batch(
                    item_index,
                    items_per_producer,
                    producer_batch_size,
                ) {
                    simulate_producer_sleep(producer_sleep).await;
                }
            }
            anyhow::Ok(())
        });
    }

    let mut received = 0;
    let mut checksum = 0;
    let mut batch = Vec::with_capacity(case.sink_batch_size);
    while received < case.total_items {
        batch.push(queue.pop().await?);
        while batch.len() < case.sink_batch_size {
            let Some(item) = queue.try_pop()? else {
                break;
            };
            batch.push(item);
        }

        let batch_len = batch.len();
        for item in batch.drain(..) {
            checksum ^= item;
            received += 1;
        }
        simulate_sink(batch_len, case.sink_delay).await;
    }

    while let Some(result) = producers.join_next().await {
        result??;
    }

    Ok(BenchRun {
        elapsed: start.elapsed(),
        cpu_elapsed: cpu_timer.elapsed(),
        checksum,
        stats: queue.stats(),
    })
}

async fn run_semaphore_queue_case(case: BenchCase) -> anyhow::Result<BenchRun> {
    let queue = Arc::new(SemaphoreQueue::new(case.capacity));
    let cpu_timer = CpuTimer::start();
    let start = start_timer();
    let mut producers = JoinSet::new();

    for producer_id in 0..case.producers {
        let queue = queue.clone();
        let items_per_producer = case.items_per_producer();
        let producer_batch_size = case.producer_batch_size;
        let producer_sleep = case.producer_sleep;
        producers.spawn(async move {
            for item_index in 0..items_per_producer {
                queue.push(item_value(producer_id, item_index)).await?;
                if should_sleep_after_producer_batch(
                    item_index,
                    items_per_producer,
                    producer_batch_size,
                ) {
                    simulate_producer_sleep(producer_sleep).await;
                }
            }
            anyhow::Ok(())
        });
    }

    let mut received = 0;
    let mut checksum = 0;
    let mut batch = Vec::with_capacity(case.sink_batch_size);
    while received < case.total_items {
        batch.push(queue.pop().await?);
        while batch.len() < case.sink_batch_size {
            let Some(item) = queue.try_pop()? else {
                break;
            };
            batch.push(item);
        }

        let batch_len = batch.len();
        for item in batch.drain(..) {
            checksum ^= item;
            received += 1;
        }
        simulate_sink(batch_len, case.sink_delay).await;
    }

    while let Some(result) = producers.join_next().await {
        result??;
    }

    Ok(BenchRun {
        elapsed: start.elapsed(),
        cpu_elapsed: cpu_timer.elapsed(),
        checksum,
        stats: queue.stats(),
    })
}

fn run_case(case: BenchCase) -> anyhow::Result<(BenchRun, BenchRun, BenchRun)> {
    let runtime = Builder::new_multi_thread()
        .worker_threads(case.worker_threads)
        .enable_time()
        .build()?;

    runtime.block_on(async {
        let mpsc = run_mpsc_case(case).await?;
        let notify_queue = run_notify_queue_case(case).await?;
        let semaphore_queue = run_semaphore_queue_case(case).await?;
        Ok((mpsc, notify_queue, semaphore_queue))
    })
}

#[test]
fn both_queues_transfer_all_items_once() -> anyhow::Result<()> {
    let case = BenchCase {
        worker_threads: 4,
        producers: 4,
        total_items: 1_000,
        capacity: 8,
        producer_batch_size: 16,
        sink_batch_size: 16,
        producer_sleep: Duration::ZERO,
        sink_delay: Duration::ZERO,
    };

    let (mpsc, notify_queue, semaphore_queue) = run_case(case)?;
    let expected = expected_checksum(case);
    assert_eq!(mpsc.checksum, expected);
    assert_eq!(notify_queue.checksum, expected);
    assert_eq!(semaphore_queue.checksum, expected);
    Ok(())
}

struct ReportRow {
    queue: &'static str,
    case: BenchCase,
    run: BenchRun,
}

fn write_benchmark_report(rows: &[ReportRow]) -> anyhow::Result<String> {
    let mut report = String::new();
    writeln!(report, "# MPSC vs ConcurrentQueue Notify Benchmark\n")?;
    writeln!(
        report,
        "| queue | worker_threads | producers | capacity | total_items | producer_batch_size | sink_batch_size | producer_sleep_us | sink_delay_us | elapsed_ms | cpu_ms | cpu_per_wall | items_per_sec | full_waits | empty_waits | push_retries_after_wake |"
    )?;
    writeln!(
        report,
        "| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    )?;

    for row in rows {
        writeln!(
            report,
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {:.3} | {:.3} | {:.3} | {:.3} | {} | {} | {} |",
            row.queue,
            row.case.worker_threads,
            row.case.producers,
            row.case.capacity,
            row.case.total_items,
            row.case.producer_batch_size,
            row.case.sink_batch_size,
            duration_us(row.case.producer_sleep),
            duration_us(row.case.sink_delay),
            duration_ms(row.run.elapsed),
            duration_ms(row.run.cpu_elapsed),
            row.run.cpu_per_wall(),
            row.run.items_per_sec(row.case.total_items),
            row.run.stats.full_waits,
            row.run.stats.empty_waits,
            row.run.stats.push_retries_after_wake,
        )?;
    }

    Ok(report)
}

#[test]
#[ignore = "micro-benchmark; run with --ignored --nocapture, preferably with --release"]
fn bench_mpsc_vs_concurrent_queue_pipeline_shape() -> anyhow::Result<()> {
    let mut rows = Vec::new();
    let worker_threads = [1, 2, 4];
    let producer_counts = [4];
    let capacities = [8192];
    let producer_batch_sizes = [2048];
    let sink_batch_sizes = [8192];
    let producer_sleeps = [Duration::from_millis(30)];
    let sink_delays = [Duration::from_millis(5)];

    for worker_threads in worker_threads {
        for producers in producer_counts {
            for capacity in capacities {
                for producer_batch_size in producer_batch_sizes {
                    for sink_batch_size in sink_batch_sizes {
                        for producer_sleep in producer_sleeps {
                            for sink_delay in sink_delays {
                                let case = BenchCase {
                                    worker_threads,
                                    producers,
                                    total_items: 1_048_576,
                                    capacity,
                                    producer_batch_size,
                                    sink_batch_size,
                                    producer_sleep,
                                    sink_delay,
                                };
                                let (mpsc, notify_queue, semaphore_queue) = run_case(case)?;
                                let expected = expected_checksum(case);
                                assert_eq!(mpsc.checksum, expected);
                                assert_eq!(notify_queue.checksum, expected);
                                assert_eq!(semaphore_queue.checksum, expected);

                                rows.push(ReportRow {
                                    queue: "mpsc",
                                    case,
                                    run: mpsc,
                                });
                                rows.push(ReportRow {
                                    queue: "concurrent_queue_notify",
                                    case,
                                    run: notify_queue,
                                });
                                rows.push(ReportRow {
                                    queue: "semaphore_concurrent_queue",
                                    case,
                                    run: semaphore_queue,
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    let report = write_benchmark_report(&rows)?;
    let path = write_report("mpsc_vs_concurrent_queue_result.md", &report)?;
    println!("wrote {}", path.display());
    Ok(())
}
