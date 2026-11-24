use std::{collections::HashMap, sync::Arc, time::Instant};

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

    pub async fn flush(&self) {
        let mut window_counter_statistics_map: HashMap<CounterType, WindowCounterStatistics> =
            HashMap::new();
        let now = Instant::now();
        let mut no_window_counter_statistics_map: HashMap<CounterType, Counter> = self
            .no_window_counter_statistics_map
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        let monitors: Vec<Arc<Monitor>> = self
            .monitors
            .iter()
            .map(|entry| entry.value().clone())
            .collect();

        for monitor in monitors {
            // Tombstoned monitors have already settled their cumulative counters.
            // Exclude them from the windowed aggregates, as TaskMonitor::calc does.
            if monitor.is_tombstone() {
                continue;
            }

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
                    let statistics = counter.statistics_at(now).await;
                    window_counter_statistics_map
                        .entry(counter_type)
                        .or_default()
                        .merge(&statistics);
                }
            }

            for counter in monitor.no_window_counters.iter() {
                no_window_counter_statistics_map
                    .entry(counter.key().clone())
                    .and_modify(|total| total.merge(counter.value()))
                    .or_insert_with(|| counter.value().clone());
            }
        }

        for (counter_type, mut statistics) in window_counter_statistics_map {
            statistics.recalculate();
            let mut log = format!("{} | {} | {}", self.name, self.description, counter_type);
            for aggregate_type in counter_type.get_aggregate_types() {
                let aggregate_value = match aggregate_type {
                    AggregateType::Latest => statistics.latest,
                    AggregateType::AvgByCount => statistics.avg_by_count,
                    AggregateType::AvgBySec => statistics.avg_by_sec,
                    AggregateType::Sum => statistics.sum,
                    AggregateType::MaxBySec => statistics.max_by_sec,
                    AggregateType::MinBySec => statistics.min_by_sec,
                    AggregateType::MaxByCount => statistics.max,
                    AggregateType::MinByCount => statistics.min,
                    AggregateType::Count => statistics.count.into(),
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
}
