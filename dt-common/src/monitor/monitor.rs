use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::time::Instant;

use super::counter::Counter;
use super::counter_type::{CounterType, WindowType};
use super::task_metrics::TaskMetricValue;
use super::time_window_counter::TimeWindowCounter;
use super::FlushableMonitor;
use crate::log_monitor;
use crate::monitor::counter_type::AggregateType;
use crate::utils::limit_queue::LimitedQueue;

pub struct Monitor {
    pub name: String,
    pub description: String,
    pub no_window_counters: DashMap<CounterType, Counter>,
    pub time_window_counters: DashMap<CounterType, Arc<TimeWindowCounter>>,
    pub time_window_secs: u64,
    pub max_sub_count: u64,
    pub count_window: u64,
    tombstone: AtomicBool,
}

#[async_trait]
impl FlushableMonitor for Monitor {
    async fn flush(&self) {
        self.flush().await;
    }
}

impl Monitor {
    pub fn new(
        name: &str,
        description: &str,
        time_window_secs: u64,
        max_sub_count: u64,
        count_window: u64,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            no_window_counters: DashMap::new(),
            time_window_counters: DashMap::new(),
            time_window_secs,
            max_sub_count,
            count_window,
            tombstone: AtomicBool::new(false),
        }
    }

    pub fn clear_tombstone(&self) {
        self.tombstone.store(false, Ordering::Release);
    }

    pub fn init_counter(&self, counter_type: CounterType) {
        match counter_type.get_window_type() {
            WindowType::NoWindow => {
                self.no_window_counters
                    .entry(counter_type)
                    .or_insert_with(|| Counter::new(0, 0));
            }
            WindowType::TimeWindow => {
                self.time_window_counter(counter_type);
            }
        }
    }

    fn time_window_counter(&self, counter_type: CounterType) -> Arc<TimeWindowCounter> {
        self.time_window_counters
            .entry(counter_type)
            .or_insert_with(|| {
                Arc::new(TimeWindowCounter::new(
                    self.time_window_secs,
                    self.max_sub_count,
                ))
            })
            .clone()
    }

    /// Measure one synchronous call in seconds, including error returns.
    pub async fn measure_duration<T>(
        &self,
        counter_type: CounterType,
        operation: impl FnOnce() -> T,
    ) -> T {
        let started = Instant::now();
        let result = operation();
        self.add_counter(counter_type, started.elapsed().as_secs_f64())
            .await;
        result
    }

    pub fn mark_tombstone(&self) {
        self.tombstone.store(true, Ordering::Release);
    }

    pub fn is_tombstone(&self) -> bool {
        self.tombstone.load(Ordering::Acquire)
    }

    pub async fn has_live_time_window_data(&self) -> bool {
        self.has_live_time_window_data_in(self.time_window_secs)
            .await
    }

    pub async fn has_live_time_window_data_in(&self, time_window_secs: u64) -> bool {
        let counter_types: Vec<CounterType> = self
            .time_window_counters
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        for counter_type in counter_types {
            let counter = self
                .time_window_counters
                .get(&counter_type)
                .map(|r| r.value().clone());
            if let Some(counter) = counter {
                if counter.has_live_data_in_window(time_window_secs).await {
                    return true;
                }
            }
        }
        false
    }

    pub async fn is_tombstone_and_expired(&self) -> bool {
        if !self.is_tombstone() {
            return false;
        }

        !self.has_live_time_window_data().await
    }

    pub async fn flush(&self) {
        let window_counter_types = self
            .time_window_counters
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for counter_type in window_counter_types {
            let counter = self
                .time_window_counters
                .get(&counter_type)
                .map(|r| r.value().clone());
            if let Some(counter) = counter {
                let statistics = counter.statistics().await;
                let mut log = format!("{} | {} | {}", self.name, self.description, counter_type);
                for aggregate_type in counter_type.get_aggregate_types() {
                    let aggregate_value = match aggregate_type {
                        AggregateType::Latest => statistics.latest,
                        AggregateType::AvgByCount => statistics.avg_by_count,
                        AggregateType::AvgBySec => statistics.avg_by_sec,
                        AggregateType::Sum => statistics.sum,
                        AggregateType::MaxBySec => statistics.max_by_sec,
                        AggregateType::MaxByCount => statistics.max,
                        AggregateType::MinByCount => statistics.min,
                        AggregateType::Count => statistics.count.into(),
                        _ => continue,
                    };
                    log = format!("{} | {}={}", log, aggregate_type, aggregate_value);
                }
                log_monitor!("{}", log);
            }
        }

        let no_window_counter_types = self
            .no_window_counters
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for counter_type in no_window_counter_types {
            if let Some(counter) = self.no_window_counters.get(&counter_type) {
                log_monitor!(
                    "{}",
                    counter.log_line(&self.name, &self.description, &counter_type)
                );
            }
        }
    }

    pub(crate) async fn add_batch_counter(
        &self,
        counter_type: CounterType,
        value: impl Into<TaskMetricValue> + Send,
        count: u64,
    ) -> &Self {
        if count == 0 {
            return self;
        }
        self.add_counter_internal(counter_type, value, count).await
    }

    pub(crate) async fn add_counter(
        &self,
        counter_type: CounterType,
        value: impl Into<TaskMetricValue> + Send,
    ) -> &Self {
        self.add_counter_internal(counter_type, value, 1).await
    }

    pub(crate) fn set_counter(
        &self,
        counter_type: CounterType,
        value: impl Into<TaskMetricValue> + Send,
    ) -> &Self {
        let value = value.into();
        if let WindowType::NoWindow = counter_type.get_window_type() {
            self.no_window_counters
                .entry(counter_type)
                .and_modify(|counter| counter.set(value, 1))
                .or_insert_with(|| Counter::new(value, 1));
        }
        self
    }

    pub(crate) async fn add_multi_counter(
        &self,
        counter_type: CounterType,
        entry: &LimitedQueue<(u64, u64)>,
    ) -> &Self {
        self.add_muilti_counter_internal(counter_type, entry).await
    }

    async fn add_counter_internal(
        &self,
        counter_type: CounterType,
        value: impl Into<TaskMetricValue> + Send,
        count: u64,
    ) -> &Self {
        let value = value.into();
        match counter_type.get_window_type() {
            WindowType::NoWindow => {
                self.no_window_counters
                    .entry(counter_type)
                    .or_insert_with(|| Counter::new(0, 0))
                    .add(value, count);
            }

            WindowType::TimeWindow => {
                self.time_window_counter(counter_type)
                    .add(value, count)
                    .await;
            }
        }
        self
    }

    async fn add_muilti_counter_internal(
        &self,
        counter_type: CounterType,
        entry: &LimitedQueue<(u64, u64)>,
    ) -> &Self {
        match counter_type.get_window_type() {
            WindowType::NoWindow => {
                self.no_window_counters
                    .entry(counter_type)
                    .or_insert_with(|| Counter::new(0, 0))
                    .adds(entry);
            }

            WindowType::TimeWindow => {
                self.time_window_counter(counter_type).adds(entry).await;
            }
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn measure_duration_preserves_results_and_records_errors() {
        let monitor = Monitor::new("pipeline", "test", 60, 100, 1);
        let counter_type = CounterType::PartitionerDurationSeconds;
        assert_eq!(
            monitor.measure_duration(counter_type.clone(), || 42).await,
            42
        );
        assert_eq!(
            monitor
                .measure_duration(counter_type.clone(), || Err::<(), _>("partition failed"))
                .await,
            Err("partition failed")
        );
        let counter = monitor
            .time_window_counters
            .get(&counter_type)
            .unwrap()
            .clone();
        let statistics = counter.statistics().await;
        assert_eq!(statistics.count, 2);
        assert_eq!(statistics.sum, TaskMetricValue::Float(0.0));
    }
}
