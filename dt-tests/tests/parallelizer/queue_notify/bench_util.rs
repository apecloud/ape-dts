use std::{
    fs,
    path::PathBuf,
    time::{Duration, Instant},
};

#[derive(Clone, Copy)]
pub(crate) struct BenchCase {
    pub(crate) worker_threads: usize,
    pub(crate) producers: usize,
    pub(crate) total_items: usize,
    pub(crate) capacity: usize,
    pub(crate) producer_batch_size: usize,
    pub(crate) sink_batch_size: usize,
    pub(crate) producer_sleep: Duration,
    pub(crate) sink_delay: Duration,
}

impl BenchCase {
    pub(crate) fn items_per_producer(&self) -> usize {
        assert_eq!(
            self.total_items % self.producers,
            0,
            "total_items must divide evenly across producers"
        );
        self.total_items / self.producers
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct QueueStats {
    pub(crate) full_waits: u64,
    pub(crate) empty_waits: u64,
    pub(crate) push_retries_after_wake: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct BenchRun {
    pub(crate) elapsed: Duration,
    pub(crate) cpu_elapsed: Duration,
    pub(crate) checksum: u64,
    pub(crate) stats: QueueStats,
}

impl BenchRun {
    pub(crate) fn items_per_sec(&self, total_items: usize) -> f64 {
        total_items as f64 / self.elapsed.as_secs_f64()
    }

    pub(crate) fn cpu_per_wall(&self) -> f64 {
        self.cpu_elapsed.as_secs_f64() / self.elapsed.as_secs_f64()
    }
}

pub(crate) fn item_value(producer_id: usize, item_index: usize) -> u64 {
    ((producer_id as u64) << 32) | item_index as u64
}

pub(crate) fn expected_checksum(case: BenchCase) -> u64 {
    let items_per_producer = case.items_per_producer();
    let mut checksum = 0;
    for producer_id in 0..case.producers {
        for item_index in 0..items_per_producer {
            checksum ^= item_value(producer_id, item_index);
        }
    }
    checksum
}

pub(crate) async fn simulate_sink(batch_len: usize, sink_delay: Duration) {
    if batch_len > 0 && !sink_delay.is_zero() {
        tokio::time::sleep(sink_delay).await;
    }
}

pub(crate) async fn simulate_producer_sleep(producer_sleep: Duration) {
    if !producer_sleep.is_zero() {
        tokio::time::sleep(producer_sleep).await;
    }
}

pub(crate) fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

pub(crate) fn duration_us(duration: Duration) -> u64 {
    duration.as_micros() as u64
}

pub(crate) fn start_timer() -> Instant {
    Instant::now()
}

pub(crate) struct CpuTimer {
    start_micros: u128,
}

impl CpuTimer {
    pub(crate) fn start() -> Self {
        Self {
            start_micros: process_cpu_micros(),
        }
    }

    pub(crate) fn elapsed(&self) -> Duration {
        let elapsed_micros = process_cpu_micros().saturating_sub(self.start_micros);
        Duration::from_micros(elapsed_micros as u64)
    }
}

fn process_cpu_micros() -> u128 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    assert_eq!(result, 0, "getrusage failed");
    let usage = unsafe { usage.assume_init() };
    timeval_micros(usage.ru_utime) + timeval_micros(usage.ru_stime)
}

fn timeval_micros(timeval: libc::timeval) -> u128 {
    timeval.tv_sec as u128 * 1_000_000 + timeval.tv_usec as u128
}

pub(crate) fn write_report(file_name: &str, contents: &str) -> anyhow::Result<PathBuf> {
    let report_dir = project_root::get_project_root()?
        .join("dt-tests")
        .join("tests")
        .join("parallelizer")
        .join("queue_notify");
    fs::create_dir_all(&report_dir)?;
    let path = report_dir.join(file_name);
    fs::write(&path, contents)?;
    Ok(path)
}
