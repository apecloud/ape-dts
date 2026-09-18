use std::{
    collections::{BTreeMap, LinkedList},
    sync::Arc,
    time::Instant,
};

use tokio::sync::RwLock;

use super::counter::Counter;
use crate::utils::limit_queue::LimitedQueue;

#[derive(Default)]
pub struct WindowCounterStatistics {
    pub sum: u64,
    pub max: u64,
    pub min: u64,
    pub avg_by_count: u64,
    pub max_by_sec: u64,
    pub min_by_sec: u64,
    pub avg_by_sec: u64,
    pub count: u64,
}

pub struct TimeWindowCounter {
    pub time_window_secs: u64,
    pub max_sub_count: u64,
    pub counters: RwLock<LinkedList<Counter>>,
}

impl TimeWindowCounter {
    pub fn new(time_window_secs: u64, max_sub_count: u64) -> Self {
        Self {
            time_window_secs,
            max_sub_count,
            counters: RwLock::new(LinkedList::new()),
        }
    }

    #[inline(always)]
    pub async fn adds(&self, values: &LimitedQueue<(u64, u64)>) -> &Self {
        if values.is_empty() {
            return self;
        }

        let mut counters = self.counters.write().await;
        while let Some(front) = counters.front() {
            if front.timestamp.elapsed().as_secs() >= self.time_window_secs {
                counters.pop_front();
            } else {
                break;
            }
        }

        while counters.len() as u64 + values.len() as u64 >= self.max_sub_count {
            counters.pop_front();
        }

        for (value, count) in values.iter() {
            if *count == 0 {
                continue;
            }
            counters.push_back(Counter::new(*value, *count));
        }
        self
    }

    #[inline(always)]
    pub async fn add(&self, value: u64, count: u64) -> &Self {
        let mut counters = self.counters.write().await;

        while let Some(front) = counters.front() {
            if front.timestamp.elapsed().as_secs() >= self.time_window_secs {
                counters.pop_front();
            } else {
                break;
            }
        }

        while counters.len() as u64 >= self.max_sub_count {
            counters.pop_front();
        }
        counters.push_back(Counter::new(value, count));
        self
    }

    #[inline(always)]
    pub async fn statistics(&self) -> WindowCounterStatistics {
        self.statistics_in_window(self.time_window_secs).await
    }

    #[inline(always)]
    pub async fn statistics_in_window(&self, time_window_secs: u64) -> WindowCounterStatistics {
        let counters = self.counters.read().await;
        let now = Instant::now();
        let mut statistics = WindowStatisticsBuilder::default();
        for counter in counters.iter() {
            statistics.add(counter, time_window_secs, now);
        }
        statistics.finish()
    }

    /// Align all component samples to one sampling instant before aggregation.
    pub async fn statistics_across(windows: &[Arc<Self>], now: Instant) -> WindowCounterStatistics {
        let mut statistics = WindowStatisticsBuilder::default();
        for window in windows {
            let counters = window.counters.read().await;
            for counter in counters.iter() {
                statistics.add(counter, window.time_window_secs, now);
            }
        }
        statistics.finish()
    }

    #[inline(always)]
    pub async fn has_live_data(&self) -> bool {
        self.has_live_data_in_window(self.time_window_secs).await
    }

    #[inline(always)]
    pub async fn has_live_data_in_window(&self, time_window_secs: u64) -> bool {
        let counters = self.counters.read().await;
        counters
            .iter()
            .any(|counter| counter.timestamp.elapsed().as_secs() < time_window_secs)
    }
}

#[derive(Default)]
struct WindowStatisticsBuilder {
    statistics: WindowCounterStatistics,
    seconds: BTreeMap<u64, u64>,
}

impl WindowStatisticsBuilder {
    fn add(&mut self, counter: &Counter, time_window_secs: u64, now: Instant) {
        let Some(age) = now.checked_duration_since(counter.timestamp) else {
            return;
        };
        if counter.count == 0 || age.as_secs() >= time_window_secs {
            return;
        }
        let value = counter
            .value
            .as_u64()
            .expect("time-window samples must be integers");
        if self.statistics.count == 0 {
            self.statistics.min = value;
        } else {
            self.statistics.min = self.statistics.min.min(value);
        }
        self.statistics.max = self.statistics.max.max(value);
        self.statistics.sum += value;
        self.statistics.count += counter.count;
        *self.seconds.entry(age.as_secs()).or_default() += value;
    }

    fn finish(mut self) -> WindowCounterStatistics {
        if let Some(average) = self.statistics.sum.checked_div(self.statistics.count) {
            self.statistics.avg_by_count = average;
            self.statistics.avg_by_sec = self.statistics.sum / self.seconds.len() as u64;
            self.statistics.min_by_sec = *self.seconds.values().min().unwrap();
            self.statistics.max_by_sec = *self.seconds.values().max().unwrap();
        }
        self.statistics
    }
}
