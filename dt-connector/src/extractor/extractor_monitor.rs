use std::sync::Arc;

use tokio::time::Instant;

use dt_common::monitor::{counter_type::CounterType, monitor::Monitor};

#[derive(Clone, Default)]
pub struct ExtractorCounters {
    pub pushed_record_count: u64,
    pub pushed_data_size: u64,
}

impl ExtractorCounters {
    pub fn new() -> Self {
        Self {
            pushed_record_count: 0,
            pushed_data_size: 0,
        }
    }
}

pub struct ExtractorMonitor {
    pub monitor: Arc<Monitor>,
    pub count_window: u64,
    pub time_window_secs: u64,
    pub last_flush_time: Instant,
    pub flushed_counters: ExtractorCounters,
    pub counters: ExtractorCounters,
}

impl ExtractorMonitor {
    pub async fn new(monitor: Arc<Monitor>) -> Self {
        let count_window = monitor.count_window;
        let time_window_secs = monitor.time_window_secs;
        Self {
            monitor,
            last_flush_time: Instant::now(),
            count_window,
            time_window_secs,
            flushed_counters: ExtractorCounters::new(),
            counters: ExtractorCounters::new(),
        }
    }

    pub async fn try_flush(&mut self, force: bool) {
        let pushed_record_count =
            self.counters.pushed_record_count - self.flushed_counters.pushed_record_count;
        let pushed_record_size =
            self.counters.pushed_data_size - self.flushed_counters.pushed_data_size;
        // to avoid too many sub counters, add counter by batch
        if force
            || pushed_record_count >= self.count_window
            || self.last_flush_time.elapsed().as_secs() >= self.time_window_secs
        {
            self.monitor
                .add_counter(CounterType::RecordCount, pushed_record_count)
                .await
                .add_counter(CounterType::DataBytes, pushed_record_size)
                .await;

            self.last_flush_time = Instant::now();
            self.flushed_counters = self.counters.clone();
        }
    }
}
