use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;

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
    no_window_counter_statistics_map: DashMap<CounterType, DashMap<AggregateType, u64>>,
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
        self.monitors.insert(id.to_string(), monitor);
    }

    pub fn remove_monitor(&self, id: &str) {
        // keep statistics of no_window counters before removing:
        // eg. 2025-02-18 05:43:37.028889 | pipeline | global | sinked_count | latest=4199364
        if let Some((_, monitor)) = self.monitors.remove(id) {
            Self::refresh_no_window_counter_statistics_map(
                &self.no_window_counter_statistics_map,
                &monitor,
            );
        }
    }

    pub async fn flush(&self) {
        let window_counter_statistics_map: DashMap<CounterType, Vec<WindowCounterStatistics>> =
            DashMap::new();
        let no_window_counter_statistics_map = self.no_window_counter_statistics_map.clone();

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
                if let Some(counter) = monitor.time_window_counters.get(&counter_type) {
                    let statistics = counter.statistics().await;
                    window_counter_statistics_map
                        .entry(counter_type)
                        .or_default()
                        .push(statistics);
                }
            }
            Self::refresh_no_window_counter_statistics_map(
                &no_window_counter_statistics_map,
                &monitor,
            );
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

        for entry in no_window_counter_statistics_map.iter() {
            let (counter_type, aggregate_value_map) = entry.pair();
            let mut log = format!("{} | {} | {}", self.name, self.description, counter_type);
            for aggregate_type in counter_type.get_aggregate_types().iter() {
                if let Some(aggregate_value) = aggregate_value_map.get(aggregate_type).map(|v| *v) {
                    log = format!("{} | {}={}", log, aggregate_type, aggregate_value);
                } else {
                    log = format!("{} | {}={}", log, aggregate_type, 0);
                }
            }
            log_monitor!("{}", log);
        }
    }

    fn refresh_no_window_counter_statistics_map(
        no_window_counter_statistics_map: &DashMap<CounterType, DashMap<AggregateType, u64>>,
        monitor: &Arc<Monitor>,
    ) {
        let no_window_counter_types = monitor
            .no_window_counters
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for counter_type in no_window_counter_types {
            if let Some(counter) = monitor.no_window_counters.get(&counter_type) {
                for aggregate_type in counter_type.get_aggregate_types().iter() {
                    let aggregate_value = match aggregate_type {
                        AggregateType::Latest => counter.value,
                        AggregateType::AvgByCount => counter.avg_by_count(),
                        _ => continue,
                    };

                    no_window_counter_statistics_map
                        .entry(counter_type.to_owned())
                        .or_default()
                        .entry(aggregate_type.to_owned())
                        .and_modify(|v| *v += aggregate_value)
                        .or_insert(aggregate_value);
                }
            }
        }
    }
}
