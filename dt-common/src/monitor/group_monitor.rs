use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use dashmap::DashMap;

use super::counter::Counter;
use super::counter_type::CounterType;
use super::monitor::Monitor;
use super::time_window_counter::WindowCounterStatistics;
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
        let window_counter_statistics_map: DashMap<CounterType, Vec<WindowCounterStatistics>> =
            DashMap::new();
        let no_window_counter_statistics_map = self.no_window_statistics();

        let monitors: Vec<Arc<Monitor>> = self
            .monitors
            .iter()
            .map(|entry| entry.value().clone())
            .collect();

        for monitor in monitors {
            let counter_types: Vec<CounterType> = monitor
                .time_window_counters
                .iter()
                .map(|entry| entry.key().clone())
                .collect();

            for counter_type in counter_types {
                let counter = monitor
                    .time_window_counters
                    .get(&counter_type)
                    .map(|r| r.value().clone());
                if let Some(counter) = counter {
                    if !counter.has_live_data().await {
                        continue;
                    }
                    let statistics = counter.statistics().await;
                    window_counter_statistics_map
                        .entry(counter_type)
                        .or_default()
                        .push(statistics);
                }
            }
        }

        for (counter_type, statistics_vec) in window_counter_statistics_map {
            let mut log = format!("{} | {} | {}", self.name, self.description, counter_type);
            for aggregate_type in counter_type.get_aggregate_types() {
                let mut aggregate_value = 0;
                for statistics in statistics_vec.iter() {
                    aggregate_value += match aggregate_type {
                        AggregateType::AvgByCount => statistics.avg_by_count,
                        AggregateType::AvgBySec => statistics.avg_by_sec,
                        AggregateType::Sum => statistics.sum,
                        AggregateType::MaxBySec => statistics.max_by_sec,
                        AggregateType::MaxByCount => statistics.max,
                        AggregateType::Count => statistics.count,
                        _ => continue,
                    };
                }
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
}

#[cfg(test)]
mod tests {
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
            let duration = &counters[&CounterType::PipelineSinkDurationSeconds];
            assert_eq!(duration.value.as_f64(), 1.75);
            assert!((duration.avg_by_count().as_f64() - 1.75 / 3.0).abs() < 1e-12);
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
}
