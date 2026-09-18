use std::{collections::HashMap, sync::Arc, time::Instant};

use async_trait::async_trait;
use dashmap::DashMap;

use super::counter::Counter;
use super::counter_type::CounterType;
use super::monitor::Monitor;
use super::time_window_counter::{TimeWindowCounter, WindowCounterStatistics};
use super::FlushableMonitor;
use crate::log_monitor;
use crate::monitor::counter_type::AggregateType;

#[derive(Clone, Default)]
pub struct GroupMonitor {
    name: String,
    description: String,
    monitors: DashMap<String, Arc<Monitor>>,
    no_window_counter_statistics_map: DashMap<CounterType, Counter>,
}

#[async_trait]
impl FlushableMonitor for GroupMonitor {
    async fn flush(&self) {
        self.flush().await;
    }
}

impl GroupMonitor {
    pub fn new(name: &str, description: &str) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            monitors: DashMap::new(),
            no_window_counter_statistics_map: DashMap::new(),
        }
    }

    pub fn add_monitor(&self, id: &str, monitor: Arc<Monitor>) {
        monitor.clear_tombstone();
        self.monitors.insert(id.to_string(), monitor);
    }

    pub fn remove_monitor(&self, id: &str) {
        self.monitors.remove(id);
    }

    pub fn settle_no_window_monitor(&self, monitor: &Arc<Monitor>) {
        for entry in monitor.no_window_counters.iter() {
            self.no_window_counter_statistics_map
                .entry(entry.key().clone())
                .and_modify(|counter| counter.merge(entry.value()))
                .or_insert_with(|| entry.value().clone());
        }
    }

    pub(crate) fn no_window_statistics(&self) -> HashMap<CounterType, Counter> {
        let mut statistics = self
            .no_window_counter_statistics_map
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        for entry in self.monitors.iter() {
            let monitor = entry.value();
            if !monitor.is_tombstone() {
                monitor.merge_no_window_counters(&mut statistics);
            }
        }
        statistics
    }

    pub async fn flush(&self) {
        let no_window_counter_statistics_map = self.no_window_statistics();
        let monitors: Vec<Arc<Monitor>> = self
            .monitors
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        let window_counter_statistics_map =
            Self::window_statistics(&monitors, Instant::now()).await;

        for (counter_type, statistics) in window_counter_statistics_map {
            if statistics.count == 0 {
                continue;
            }
            let mut log = format!("{} | {} | {}", self.name, self.description, counter_type);
            for aggregate_type in counter_type.get_aggregate_types() {
                let aggregate_value = match aggregate_type {
                    AggregateType::AvgByCount => statistics.avg_by_count,
                    AggregateType::AvgBySec => statistics.avg_by_sec,
                    AggregateType::Sum => statistics.sum,
                    AggregateType::MaxBySec => statistics.max_by_sec,
                    AggregateType::MinBySec => statistics.min_by_sec,
                    AggregateType::MaxByCount => statistics.max,
                    AggregateType::MinByCount => statistics.min,
                    AggregateType::Count => statistics.count,
                    _ => continue,
                };
                log = format!("{} | {}={}", log, aggregate_type, aggregate_value);
            }
            log_monitor!("{}", log);
        }

        for (counter_type, counter) in no_window_counter_statistics_map {
            log_monitor!(
                "{}",
                counter.log_line(&self.name, &self.description, &counter_type)
            );
        }
    }

    pub(crate) async fn window_statistics(
        monitors: &[Arc<Monitor>],
        now: Instant,
    ) -> HashMap<CounterType, WindowCounterStatistics> {
        let mut windows: HashMap<CounterType, Vec<Arc<TimeWindowCounter>>> = HashMap::new();
        for monitor in monitors {
            for entry in monitor.time_window_counters.iter() {
                windows
                    .entry(entry.key().clone())
                    .or_default()
                    .push(entry.value().clone());
            }
        }
        let mut result = HashMap::new();
        for (counter_type, counters) in windows {
            result.insert(
                counter_type,
                TimeWindowCounter::statistics_across(&counters, now).await,
            );
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn no_window_statistics_merge_raw_samples_and_keep_settled_totals() {
        let group = GroupMonitor::new("pipeline", "global");
        let monitors = ["a", "b", "empty"].map(|id| {
            let monitor = Arc::new(Monitor::new("pipeline", id, 0, 0, 1));
            monitor.init_counter(CounterType::PipelineSinkParallelUtilization);
            group.add_monitor(id, monitor.clone());
            monitor
        });
        for (index, utilization, seconds) in [(0, 0.25, 0.25), (1, 0.5, 0.5), (1, 1.0, 1.0)] {
            monitors[index].add_no_window_counter(
                CounterType::PipelineSinkParallelUtilization,
                utilization,
                1,
            );
            monitors[index].add_no_window_counter(
                CounterType::PipelineSinkDurationSeconds,
                seconds,
                1,
            );
            monitors[index].add_no_window_counter(CounterType::PipelineSinkOperationsTotal, 1, 1);
        }
        monitors[0].add_no_window_counter(CounterType::SinkedRecordTotal, u64::MAX - 1, 1);
        monitors[1].add_no_window_counter(CounterType::SinkedRecordTotal, 1, 1);

        let assert_totals = || {
            let counters = group.no_window_statistics();
            let utilization = &counters[&CounterType::PipelineSinkParallelUtilization];
            assert_eq!(utilization.count, 3);
            assert!((utilization.avg_by_count().as_f64() - 7.0 / 12.0).abs() < 1e-12);
            assert_eq!(
                (utilization.min.as_f64(), utilization.max.as_f64()),
                (0.25, 1.0)
            );
            let duration = &counters[&CounterType::PipelineSinkDurationSeconds];
            assert_eq!(duration.value.as_f64(), 1.75);
            assert!((duration.avg_by_count().as_f64() - 1.75 / 3.0).abs() < 1e-12);
            assert_eq!((duration.min.as_f64(), duration.max.as_f64()), (0.25, 1.0));
            assert_eq!(
                counters[&CounterType::PipelineSinkOperationsTotal]
                    .value
                    .as_u64(),
                Some(3)
            );
            assert_eq!(
                counters[&CounterType::SinkedRecordTotal].value.as_u64(),
                Some(u64::MAX)
            );
        };
        assert_totals();
        assert_totals(); // Repeated reads must not accumulate the live samples again.
        for (id, monitor) in ["a", "b", "empty"].iter().zip(&monitors) {
            monitor.mark_tombstone();
            group.settle_no_window_monitor(monitor);
            assert_totals();
            group.remove_monitor(id);
            group.remove_monitor(id);
            assert_totals();
        }
    }

    #[test]
    fn concurrent_settlement_preserves_each_monitors_samples() {
        let group = GroupMonitor::new("pipeline", "global");
        std::thread::scope(|scope| {
            for _ in 0..16 {
                let group = &group;
                scope.spawn(move || {
                    let monitor = Arc::new(Monitor::new("pipeline", "test", 0, 0, 1));
                    monitor.add_no_window_counter(
                        CounterType::PipelineSinkDurationSeconds,
                        0.25,
                        1,
                    );
                    group.settle_no_window_monitor(&monitor);
                });
            }
        });
        let counters = group.no_window_statistics();
        let duration = &counters[&CounterType::PipelineSinkDurationSeconds];
        assert_eq!(duration.count, 16);
        assert_eq!(duration.value.as_f64(), 4.0);
        assert_eq!(duration.avg_by_count().as_f64(), 0.25);
    }

    #[tokio::test]
    async fn count_statistics_merge_samples_instead_of_monitor_averages() {
        let a = Arc::new(Monitor::new("sinker", "a", 60, 100, 10));
        let b = Arc::new(Monitor::new("sinker", "b", 60, 100, 10));
        let empty = Arc::new(Monitor::new("sinker", "empty", 60, 100, 10));
        a.add_counter(CounterType::RtPerQuery, 90).await;
        for value in [10, 20, 0] {
            b.add_counter(CounterType::RtPerQuery, value).await;
        }
        b.mark_tombstone(); // GroupMonitor keeps finished tables' live samples.
        for monitors in [vec![a.clone(), b.clone(), empty.clone()], vec![empty, b, a]] {
            let statistics = GroupMonitor::window_statistics(&monitors, Instant::now()).await;
            let result = &statistics[&CounterType::RtPerQuery];
            assert_eq!(result.count, 4);
            assert_eq!(result.avg_by_count, 30);
            assert_eq!(result.min, 0);
            assert_eq!(result.max, 90);
        }
    }

    #[tokio::test]
    async fn rate_statistics_align_seconds_and_ignore_expired_samples() {
        let now = Instant::now();
        let mut monitors = Vec::new();
        for (id, values) in [("a", [100, 20]), ("b", [10, 200])] {
            let monitor = Arc::new(Monitor::new("sinker", id, 60, 100, 10));
            for (age, value) in [(1, values[0]), (2, values[1]), (60, 999)] {
                monitor.add_counter(CounterType::RecordCount, value).await;
                let window = monitor
                    .time_window_counters
                    .get(&CounterType::RecordCount)
                    .unwrap()
                    .clone();
                window.counters.write().await.back_mut().unwrap().timestamp =
                    now - Duration::from_secs(age);
            }
            monitors.push(monitor);
        }
        let statistics = GroupMonitor::window_statistics(&monitors, now).await;
        let result = &statistics[&CounterType::RecordCount];
        assert_eq!(result.sum, 330);
        assert_eq!(result.min_by_sec, 110);
        assert_eq!(result.max_by_sec, 220);
        assert_eq!(result.avg_by_sec, 165);
    }
}
