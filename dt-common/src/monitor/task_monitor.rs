use std::{cmp, collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use dashmap::DashMap;

use super::{
    group_monitor::GroupMonitor, monitor::Monitor, sinker_worker_metrics::SinkerWorkerMetrics,
    task_metrics::TaskMetricValue,
};
#[cfg(feature = "metrics")]
use crate::monitor::prometheus_metrics::PrometheusMetrics;
use crate::{
    config::config_enums::{TaskKind, TaskType},
    log_task,
    monitor::{
        counter_type::{AggregateType, CounterType},
        task_metrics::TaskMetricsType,
        FlushableMonitor,
    },
    utils::limit_queue::LimitedQueue,
};

#[derive(Clone)]
pub struct TaskMonitor {
    task_type: Option<TaskType>,
    extractor_group_monitor: Option<Arc<GroupMonitor>>,
    pipeline_group_monitor: Option<Arc<GroupMonitor>>,
    sinker_group_monitor: Option<Arc<GroupMonitor>>,
    checker_group_monitor: Option<Arc<GroupMonitor>>,

    extractors: DashMap<String, Arc<Monitor>>,
    pipelines: DashMap<String, Arc<Monitor>>,
    sinkers: DashMap<String, Arc<Monitor>>,
    checkers: DashMap<String, Arc<Monitor>>,

    no_window_metrics_map: DashMap<TaskMetricsType, u64>,
    sinker_worker_metrics: Arc<SinkerWorkerMetrics>,
    #[cfg(feature = "metrics")]
    pub prometheus_metrics: Arc<PrometheusMetrics>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum MonitorType {
    Extractor,
    Pipeline,
    Sinker,
    Checker,
}

enum CalcType {
    #[allow(dead_code)]
    Add,
    Max,
    Avg,
    Min,
    Latest,
}

#[async_trait]
impl FlushableMonitor for TaskMonitor {
    // TaskUtil awaits each flush, including the final flush, in one monitor task.
    async fn flush(&self) {
        if self.task_type.is_none() {
            return;
        }

        self.cleanup_monitors().await;

        for monitor in self.collect_monitors() {
            monitor.flush().await;
        }

        self.flush_global().await;

        self.reset_before_calc();
        if let Some(metrics) = self.calc().await {
            if let Ok(serialized) = serde_json::to_string(&metrics) {
                log_task!("{}", serialized);
            }
            #[cfg(feature = "metrics")]
            self.prometheus_metrics.set_metrics(&metrics);
        }
    }
}

impl TaskMonitor {
    #[cfg(not(feature = "metrics"))]
    pub fn new(task_type: Option<TaskType>) -> Self {
        Self {
            task_type,
            extractor_group_monitor: Self::build_group_monitor(task_type, "extractor"),
            pipeline_group_monitor: Self::build_group_monitor(task_type, "pipeline"),
            sinker_group_monitor: Self::build_group_monitor(task_type, "sinker"),
            checker_group_monitor: Self::build_group_monitor(task_type, "checker"),
            extractors: DashMap::new(),
            pipelines: DashMap::new(),
            sinkers: DashMap::new(),
            checkers: DashMap::new(),
            no_window_metrics_map: DashMap::new(),
            sinker_worker_metrics: Arc::new(SinkerWorkerMetrics::default()),
        }
    }

    #[cfg(feature = "metrics")]
    pub fn new(task_type: Option<TaskType>, prometheus_metrics: Arc<PrometheusMetrics>) -> Self {
        Self {
            task_type,
            extractor_group_monitor: Self::build_group_monitor(task_type, "extractor"),
            pipeline_group_monitor: Self::build_group_monitor(task_type, "pipeline"),
            sinker_group_monitor: Self::build_group_monitor(task_type, "sinker"),
            checker_group_monitor: Self::build_group_monitor(task_type, "checker"),
            extractors: DashMap::new(),
            pipelines: DashMap::new(),
            sinkers: DashMap::new(),
            checkers: DashMap::new(),
            no_window_metrics_map: DashMap::new(),
            sinker_worker_metrics: Arc::new(SinkerWorkerMetrics::default()),
            prometheus_metrics,
        }
    }

    fn build_group_monitor(task_type: Option<TaskType>, name: &str) -> Option<Arc<GroupMonitor>> {
        matches!(task_type, Some(task_type) if task_type.kind == TaskKind::Snapshot)
            .then(|| Arc::new(GroupMonitor::new(name, "global")))
    }

    pub fn get_task_type(&self) -> Option<&TaskType> {
        self.task_type.as_ref()
    }

    pub fn sinker_worker_metrics(&self) -> Arc<SinkerWorkerMetrics> {
        self.sinker_worker_metrics.clone()
    }

    pub fn register(&self, task_id: &str, monitors: Vec<(MonitorType, Arc<Monitor>)>) {
        if self.task_type.is_none() {
            return;
        }

        for (monitor_type, monitor) in monitors {
            match monitor_type {
                MonitorType::Extractor => {
                    monitor.clear_tombstone();
                    if let Some(group_monitor) = &self.extractor_group_monitor {
                        group_monitor.add_monitor(task_id, monitor.clone());
                    }
                    self.extractors.insert(task_id.to_string(), monitor);
                }
                MonitorType::Pipeline => {
                    monitor.clear_tombstone();
                    if let Some(group_monitor) = &self.pipeline_group_monitor {
                        group_monitor.add_monitor(task_id, monitor.clone());
                    }
                    self.pipelines.insert(task_id.to_string(), monitor);
                }
                MonitorType::Sinker => {
                    monitor.clear_tombstone();
                    if let Some(group_monitor) = &self.sinker_group_monitor {
                        group_monitor.add_monitor(task_id, monitor.clone());
                    }
                    self.sinkers.insert(task_id.to_string(), monitor);
                }
                MonitorType::Checker => {
                    monitor.clear_tombstone();
                    if let Some(group_monitor) = &self.checker_group_monitor {
                        group_monitor.add_monitor(task_id, monitor.clone());
                    }
                    self.checkers.insert(task_id.to_string(), monitor);
                }
            }
        }
    }

    pub fn ensure_monitor(
        &self,
        task_id: &str,
        monitor_type: MonitorType,
        time_window_secs: u64,
        max_sub_count: u64,
        count_window: u64,
    ) {
        if self.task_type.is_none() || task_id.is_empty() {
            return;
        }

        let (monitors, group) = match monitor_type {
            MonitorType::Extractor => (&self.extractors, &self.extractor_group_monitor),
            MonitorType::Pipeline => (&self.pipelines, &self.pipeline_group_monitor),
            MonitorType::Sinker => (&self.sinkers, &self.sinker_group_monitor),
            MonitorType::Checker => (&self.checkers, &self.checker_group_monitor),
        };
        monitors.entry(task_id.to_owned()).or_insert_with(|| {
            let monitor = Arc::new(Monitor::new(
                monitor_type.as_str(),
                task_id,
                time_window_secs,
                max_sub_count,
                count_window,
            ));
            if let Some(group) = group {
                group.add_monitor(task_id, monitor.clone());
            }
            monitor
        });
    }

    pub fn unregister(&self, task_id: &str, monitors: Vec<MonitorType>) {
        if self.task_type.is_none() {
            return;
        }

        let mut calc_monitors = Vec::new();
        for monitor_type in monitors {
            match monitor_type {
                MonitorType::Extractor => {
                    if let Some(monitor) = self
                        .extractors
                        .get(task_id)
                        .map(|entry| entry.value().clone())
                    {
                        monitor.mark_tombstone();
                        if let Some(group_monitor) = &self.extractor_group_monitor {
                            group_monitor.settle_no_window_monitor(&monitor);
                        }
                        calc_monitors.push((MonitorType::Extractor, monitor.clone()));
                    }
                }
                MonitorType::Pipeline => {
                    if let Some(monitor) = self
                        .pipelines
                        .get(task_id)
                        .map(|entry| entry.value().clone())
                    {
                        monitor.mark_tombstone();
                        if let Some(group_monitor) = &self.pipeline_group_monitor {
                            group_monitor.settle_no_window_monitor(&monitor);
                        }
                        calc_monitors.push((MonitorType::Pipeline, monitor.clone()));
                    }
                }
                MonitorType::Sinker => {
                    if let Some(monitor) =
                        self.sinkers.get(task_id).map(|entry| entry.value().clone())
                    {
                        monitor.mark_tombstone();
                        if let Some(group_monitor) = &self.sinker_group_monitor {
                            group_monitor.settle_no_window_monitor(&monitor);
                        }
                        calc_monitors.push((MonitorType::Sinker, monitor.clone()));
                    }
                }
                MonitorType::Checker => {
                    if let Some(monitor) = self
                        .checkers
                        .get(task_id)
                        .map(|entry| entry.value().clone())
                    {
                        monitor.mark_tombstone();
                        if let Some(group_monitor) = &self.checker_group_monitor {
                            group_monitor.settle_no_window_monitor(&monitor);
                        }
                        calc_monitors.push((MonitorType::Checker, monitor.clone()));
                    }
                }
            }
        }
        calc_nowindow_metrics(&self.no_window_metrics_map, calc_monitors);
    }

    pub async fn flush_monitors(&self, task_id: &str, monitor_types: &[MonitorType]) {
        for monitor_type in monitor_types {
            if let Some(monitor) = self.get_monitor(task_id, monitor_type) {
                monitor.flush().await;
            }
        }
    }

    pub async fn add_counter(
        &self,
        task_id: &str,
        monitor_type: MonitorType,
        counter_type: CounterType,
        value: impl Into<TaskMetricValue> + Send,
    ) {
        if let Some(monitor) = self.get_monitor(task_id, &monitor_type) {
            monitor.add_counter(counter_type, value).await;
        }
    }

    pub fn set_counter(
        &self,
        task_id: &str,
        monitor_type: MonitorType,
        counter_type: CounterType,
        value: impl Into<TaskMetricValue> + Send,
    ) {
        if let Some(monitor) = self.get_monitor(task_id, &monitor_type) {
            monitor.set_counter(counter_type, value);
        }
    }

    pub async fn add_batch_counter(
        &self,
        task_id: &str,
        monitor_type: MonitorType,
        counter_type: CounterType,
        value: impl Into<TaskMetricValue> + Send,
        count: u64,
    ) {
        if let Some(monitor) = self.get_monitor(task_id, &monitor_type) {
            monitor.add_batch_counter(counter_type, value, count).await;
        }
    }

    pub async fn add_multi_counter(
        &self,
        task_id: &str,
        monitor_type: MonitorType,
        counter_type: CounterType,
        entry: &LimitedQueue<(u64, u64)>,
    ) {
        if let Some(monitor) = self.get_monitor(task_id, &monitor_type) {
            monitor.add_multi_counter(counter_type, entry).await;
        }
    }

    pub fn add_no_window_metrics(&self, metrics_type: TaskMetricsType, value: u64) {
        self.no_window_metrics_map
            .entry(metrics_type)
            .and_modify(|v| *v += value)
            .or_insert(value);
    }

    pub fn get_no_window_metric(&self, metrics_type: TaskMetricsType) -> u64 {
        self.no_window_metrics_map
            .get(&metrics_type)
            .map(|entry| *entry.value())
            .unwrap_or_default()
    }

    async fn calc(&self) -> Option<BTreeMap<TaskMetricsType, TaskMetricValue>> {
        self.task_type.as_ref()?;

        let mut metrics: BTreeMap<TaskMetricsType, TaskMetricValue> = BTreeMap::new();
        let mut calc_handler = |calc_type: CalcType,
                                task_metrics_type: TaskMetricsType,
                                val: TaskMetricValue| match calc_type
        {
            CalcType::Min => {
                metrics
                    .entry(task_metrics_type)
                    .and_modify(|v| *v = (*v).min(val))
                    .or_insert(val);
            }
            CalcType::Max => {
                metrics
                    .entry(task_metrics_type)
                    .and_modify(|v| *v = (*v).max(val))
                    .or_insert(val);
            }
            CalcType::Avg => {
                metrics
                    .entry(task_metrics_type)
                    .and_modify(|v| *v = ((*v) + val) / 2)
                    .or_insert(val);
            }
            CalcType::Latest => {
                metrics.insert(task_metrics_type, val);
            }
            _ => {}
        };

        let mut calc_monitors = Vec::new();

        let extractors: Vec<Arc<Monitor>> = self
            .extractors
            .iter()
            .map(|item| item.value().clone())
            .collect();

        for monitor in extractors {
            if monitor.is_tombstone() {
                continue;
            }
            calc_monitors.push((MonitorType::Extractor, monitor.clone()));
            // extractor rps
            let counter = monitor
                .time_window_counters
                .get(&CounterType::ExtractedRecords)
                .map(|r| r.value().clone());
            if let Some(counter) = counter {
                let statics = counter.statistics().await;
                calc_handler(
                    CalcType::Min,
                    TaskMetricsType::ExtractorRpsMin,
                    statics.min_by_sec.into(),
                );
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::ExtractorRpsMax,
                    statics.max_by_sec.into(),
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::ExtractorRpsAvg,
                    statics.avg_by_sec.into(),
                );
            }
            // extractor bps
            let counter = monitor
                .time_window_counters
                .get(&CounterType::ExtractedBytes)
                .map(|r| r.value().clone());
            if let Some(counter) = counter {
                let statics = counter.statistics().await;
                calc_handler(
                    CalcType::Min,
                    TaskMetricsType::ExtractorBpsMin,
                    statics.min_by_sec.into(),
                );
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::ExtractorBpsMax,
                    statics.max_by_sec.into(),
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::ExtractorBpsAvg,
                    statics.avg_by_sec.into(),
                );
            }
            // extractor pushed records
            let counter = monitor
                .time_window_counters
                .get(&CounterType::RecordCount)
                .map(|r| r.value().clone());
            if let Some(counter) = counter {
                let statics = counter.statistics().await;
                calc_handler(
                    CalcType::Min,
                    TaskMetricsType::ExtractorPushedRpsMin,
                    statics.min_by_sec.into(),
                );
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::ExtractorPushedRpsMax,
                    statics.max_by_sec.into(),
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::ExtractorPushedRpsAvg,
                    statics.avg_by_sec.into(),
                );
            }
            // extractor pushed bytes
            let counter = monitor
                .time_window_counters
                .get(&CounterType::DataBytes)
                .map(|r| r.value().clone());
            if let Some(counter) = counter {
                let statics = counter.statistics().await;
                calc_handler(
                    CalcType::Min,
                    TaskMetricsType::ExtractorPushedBpsMin,
                    statics.min_by_sec.into(),
                );
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::ExtractorPushedBpsMax,
                    statics.max_by_sec.into(),
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::ExtractorPushedBpsAvg,
                    statics.avg_by_sec.into(),
                );
            }
        }

        let pipelines: Vec<Arc<Monitor>> = self
            .pipelines
            .iter()
            .map(|item| item.value().clone())
            .collect();

        for monitor in pipelines {
            if monitor.is_tombstone() {
                continue;
            }
            calc_monitors.push((MonitorType::Pipeline, monitor.clone()));
            let counter = monitor
                .time_window_counters
                .get(&CounterType::SinkerWorkersPerDrain)
                .map(|r| r.value().clone());
            if let Some(counter) = counter {
                let statics = counter.statistics().await;
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::SinkerWorkersPerDrainMax,
                    statics.max.into(),
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::SinkerWorkersPerDrainAvg,
                    statics.avg_by_count.into(),
                );
            }

            for counter_type in [
                CounterType::PipelineSinkOperationsTotal,
                CounterType::PipelineSinkParallelUtilization,
                CounterType::PipelineSinkDurationSeconds,
                CounterType::PartitionerDurationSeconds,
            ] {
                if let Some(counter) = monitor.no_window_counters.get(&counter_type) {
                    for aggregate_type in counter_type.get_aggregate_types() {
                        let metrics_type = match (&counter_type, &aggregate_type) {
                            (CounterType::PipelineSinkOperationsTotal, AggregateType::Latest) => {
                                TaskMetricsType::PipelineSinkOperationsTotal
                            }
                            (
                                CounterType::PipelineSinkParallelUtilization,
                                AggregateType::AvgByCount,
                            ) => TaskMetricsType::PipelineSinkParallelUtilizationAvg,
                            (CounterType::PipelineSinkDurationSeconds, AggregateType::Sum) => {
                                TaskMetricsType::PipelineSinkDurationSecondsSum
                            }
                            (
                                CounterType::PipelineSinkDurationSeconds,
                                AggregateType::AvgByCount,
                            ) => TaskMetricsType::PipelineSinkDurationSecondsAvg,
                            (CounterType::PartitionerDurationSeconds, AggregateType::Sum) => {
                                TaskMetricsType::PartitionerDurationSecondsSum
                            }
                            (
                                CounterType::PartitionerDurationSeconds,
                                AggregateType::AvgByCount,
                            ) => TaskMetricsType::PartitionerDurationSecondsAvg,
                            _ => continue,
                        };
                        // The task has one pipeline; its counters cover the entire run.
                        calc_handler(
                            CalcType::Latest,
                            metrics_type,
                            counter.aggregate(&aggregate_type),
                        );
                    }
                }
            }
        }

        let sinkers: Vec<Arc<Monitor>> = self
            .sinkers
            .iter()
            .map(|item| item.value().clone())
            .collect();

        for monitor in sinkers {
            if monitor.is_tombstone() {
                continue;
            }
            calc_monitors.push((MonitorType::Sinker, monitor.clone()));
            // sinker rt
            let counter = monitor
                .time_window_counters
                .get(&CounterType::RtPerQuery)
                .map(|r| r.value().clone());
            if let Some(counter) = counter {
                let statics = counter.statistics().await;
                calc_handler(
                    CalcType::Min,
                    TaskMetricsType::SinkerRtMin,
                    statics.min_by_sec.into(),
                );
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::SinkerRtMax,
                    statics.max_by_sec.into(),
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::SinkerRtAvg,
                    statics.avg_by_sec.into(),
                );
            }
            // sinker rps
            let counter = monitor
                .time_window_counters
                .get(&CounterType::RecordsPerQuery)
                .map(|r| r.value().clone());
            if let Some(counter) = counter {
                let statics = counter.statistics().await;
                calc_handler(
                    CalcType::Min,
                    TaskMetricsType::SinkerRpsMin,
                    statics.min_by_sec.into(),
                );
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::SinkerRpsMax,
                    statics.max_by_sec.into(),
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::SinkerRpsAvg,
                    statics.avg_by_sec.into(),
                );
            }
            // sinker bps
            let counter = monitor
                .time_window_counters
                .get(&CounterType::DataBytes)
                .map(|r| r.value().clone());
            if let Some(counter) = counter {
                let statics = counter.statistics().await;
                calc_handler(
                    CalcType::Min,
                    TaskMetricsType::SinkerBpsMin,
                    statics.min_by_sec.into(),
                );
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::SinkerBpsMax,
                    statics.max_by_sec.into(),
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::SinkerBpsAvg,
                    statics.avg_by_sec.into(),
                );
            }
        }

        let checkers: Vec<Arc<Monitor>> = self
            .checkers
            .iter()
            .map(|item| item.value().clone())
            .collect();

        for monitor in checkers {
            if monitor.is_tombstone() {
                continue;
            }
            calc_monitors.push((MonitorType::Checker, monitor.clone()));
            // checker checked records
            let counter = monitor
                .time_window_counters
                .get(&CounterType::RecordCount)
                .map(|r| r.value().clone());
            if let Some(counter) = counter {
                let statics = counter.statistics().await;
                calc_handler(
                    CalcType::Min,
                    TaskMetricsType::CheckerRpsMin,
                    statics.min_by_sec.into(),
                );
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::CheckerRpsMax,
                    statics.max_by_sec.into(),
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::CheckerRpsAvg,
                    statics.avg_by_sec.into(),
                );
            }
            // checker miss
            let counter = monitor
                .time_window_counters
                .get(&CounterType::CheckerMissCount)
                .map(|r| r.value().clone());
            if let Some(counter) = counter {
                let statics = counter.statistics().await;
                calc_handler(
                    CalcType::Min,
                    TaskMetricsType::CheckerMissRpsMin,
                    statics.min_by_sec.into(),
                );
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::CheckerMissRpsMax,
                    statics.max_by_sec.into(),
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::CheckerMissRpsAvg,
                    statics.avg_by_sec.into(),
                );
            }
            // checker diff
            let counter = monitor
                .time_window_counters
                .get(&CounterType::CheckerDiffCount)
                .map(|r| r.value().clone());
            if let Some(counter) = counter {
                let statics = counter.statistics().await;
                calc_handler(
                    CalcType::Min,
                    TaskMetricsType::CheckerDiffRpsMin,
                    statics.min_by_sec.into(),
                );
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::CheckerDiffRpsMax,
                    statics.max_by_sec.into(),
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::CheckerDiffRpsAvg,
                    statics.avg_by_sec.into(),
                );
            }
        }

        calc_nowindow_metrics(&self.no_window_metrics_map, calc_monitors);

        let mut total_progress_count = 0;
        let mut finished_progress_count = 0;
        for item in self.no_window_metrics_map.iter() {
            metrics.insert(*item.key(), (*item.value()).into());
            match item.key() {
                TaskMetricsType::TotalProgressCount => {
                    total_progress_count = *item.value();
                }
                TaskMetricsType::FinishedProgressCount => {
                    finished_progress_count = *item.value();
                }
                _ => {}
            }
        }
        if total_progress_count > 0 {
            metrics.insert(
                TaskMetricsType::Progress,
                cmp::min(finished_progress_count * 100 / total_progress_count, 100).into(),
            );
        }
        collect_sinker_worker_metrics(&self.sinker_worker_metrics, &mut metrics);

        Some(metrics)
    }

    fn reset_before_calc(&self) {
        self.no_window_metrics_map
            .remove(&TaskMetricsType::PipelineQueueSize);
        self.no_window_metrics_map
            .remove(&TaskMetricsType::PipelineQueueBytes);
    }

    async fn cleanup_monitors(&self) {
        self.cleanup_monitor_map(&self.extractors, self.extractor_group_monitor.as_ref())
            .await;
        self.cleanup_monitor_map(&self.pipelines, self.pipeline_group_monitor.as_ref())
            .await;
        self.cleanup_monitor_map(&self.sinkers, self.sinker_group_monitor.as_ref())
            .await;
        self.cleanup_monitor_map(&self.checkers, self.checker_group_monitor.as_ref())
            .await;
    }

    async fn cleanup_monitor_map(
        &self,
        monitors: &DashMap<String, Arc<Monitor>>,
        group_monitor: Option<&Arc<GroupMonitor>>,
    ) {
        let monitor_entries: Vec<(String, Arc<Monitor>)> = monitors
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        for (task_id, monitor) in monitor_entries {
            if !monitor.is_tombstone_and_expired().await {
                continue;
            }

            if let Some((_, _removed)) = monitors.remove_if(&task_id, |_, current| {
                Arc::ptr_eq(current, &monitor) && current.is_tombstone()
            }) {
                if let Some(group_monitor) = group_monitor {
                    group_monitor.remove_monitor(&task_id);
                }
            }
        }
    }

    async fn flush_global(&self) {
        if let Some(group_monitor) = &self.extractor_group_monitor {
            group_monitor.flush().await;
        }
        if let Some(group_monitor) = &self.pipeline_group_monitor {
            group_monitor.flush().await;
        }
        if let Some(group_monitor) = &self.sinker_group_monitor {
            group_monitor.flush().await;
        }
        if let Some(group_monitor) = &self.checker_group_monitor {
            group_monitor.flush().await;
        }
    }

    pub(crate) fn get_monitor(
        &self,
        task_id: &str,
        monitor_type: &MonitorType,
    ) -> Option<Arc<Monitor>> {
        let monitor =
            match monitor_type {
                MonitorType::Extractor => self.extractors.get(task_id).and_then(|entry| {
                    (!entry.value().is_tombstone()).then(|| entry.value().clone())
                }),
                MonitorType::Pipeline => self.pipelines.get(task_id).and_then(|entry| {
                    (!entry.value().is_tombstone()).then(|| entry.value().clone())
                }),
                MonitorType::Sinker => self.sinkers.get(task_id).and_then(|entry| {
                    (!entry.value().is_tombstone()).then(|| entry.value().clone())
                }),
                MonitorType::Checker => self.checkers.get(task_id).and_then(|entry| {
                    (!entry.value().is_tombstone()).then(|| entry.value().clone())
                }),
            };

        if monitor.is_none() {
            log::debug!(
                "task monitor route missed: task_id={}, monitor_type={:?}",
                task_id,
                monitor_type
            );
        }

        monitor
    }

    fn collect_monitors(&self) -> Vec<Arc<Monitor>> {
        let mut monitors = Vec::new();
        monitors.extend(self.extractors.iter().map(|item| item.value().clone()));
        monitors.extend(self.pipelines.iter().map(|item| item.value().clone()));
        monitors.extend(self.sinkers.iter().map(|item| item.value().clone()));
        monitors.extend(self.checkers.iter().map(|item| item.value().clone()));
        monitors
    }
}

fn collect_sinker_worker_metrics(
    tracker: &SinkerWorkerMetrics,
    metrics: &mut BTreeMap<TaskMetricsType, TaskMetricValue>,
) {
    let snapshot = tracker.snapshot();
    metrics.insert(
        TaskMetricsType::SinkerWorkersConfigured,
        snapshot.configured.into(),
    );
    metrics.insert(TaskMetricsType::SinkerWorkersBusy, snapshot.busy.into());
}

impl MonitorType {
    pub fn as_str(&self) -> &'static str {
        match self {
            MonitorType::Extractor => "extractor",
            MonitorType::Pipeline => "pipeline",
            MonitorType::Sinker => "sinker",
            MonitorType::Checker => "checker",
        }
    }
}

fn calc_nowindow_metrics(
    result_map: &DashMap<TaskMetricsType, u64>,
    calc_monitors: Vec<(MonitorType, Arc<Monitor>)>,
) {
    let batch_metrics = DashMap::<TaskMetricsType, u64>::new();
    let metric_handler = |monitor: &Arc<Monitor>,
                          counter_type: CounterType,
                          metrics_type: TaskMetricsType,
                          calc_type: CalcType| {
        if let Some(counter) = monitor.no_window_counters.get(&counter_type) {
            let value = counter
                .value
                .as_u64()
                .expect("legacy task counters must be integers");
            match calc_type {
                CalcType::Add => {
                    result_map
                        .entry(metrics_type)
                        .and_modify(|v| *v += value)
                        .or_insert(value);
                }
                CalcType::Max => {
                    result_map
                        .entry(metrics_type)
                        .and_modify(|v| *v = (*v).max(value))
                        .or_insert(value);
                }
                CalcType::Latest => {
                    result_map
                        .entry(metrics_type)
                        .and_modify(|v| *v = value)
                        .or_insert(value);
                }
                _ => {}
            }
        }
    };
    let batch_metrics_handler =
        |monitor: &Arc<Monitor>, counter_type: CounterType, metrics_type: TaskMetricsType| {
            if let Some(counter) = monitor.no_window_counters.get(&counter_type) {
                let value = counter
                    .value
                    .as_u64()
                    .expect("legacy task counters must be integers");
                batch_metrics
                    .entry(metrics_type)
                    .and_modify(|v| *v += value)
                    .or_insert(value);
            }
        };

    for (monitor_type, monitor) in calc_monitors {
        match monitor_type {
            MonitorType::Extractor => {}
            MonitorType::Sinker => {}
            MonitorType::Checker => {
                metric_handler(
                    &monitor,
                    CounterType::CheckerPending,
                    TaskMetricsType::CheckerPending,
                    CalcType::Latest,
                );
            }
            MonitorType::Pipeline => {
                metric_handler(
                    &monitor,
                    CounterType::Timestamp,
                    TaskMetricsType::Timestamp,
                    CalcType::Max,
                );
                metric_handler(
                    &monitor,
                    CounterType::QueuedRecordCurrent,
                    TaskMetricsType::PipelineQueueSize,
                    CalcType::Latest,
                );
                metric_handler(
                    &monitor,
                    CounterType::QueuedByteCurrent,
                    TaskMetricsType::PipelineQueueBytes,
                    CalcType::Latest,
                );
                batch_metrics_handler(
                    &monitor,
                    CounterType::DDLRecordTotal,
                    TaskMetricsType::SinkerDdlCount,
                );
                batch_metrics_handler(
                    &monitor,
                    CounterType::SinkedRecordTotal,
                    TaskMetricsType::SinkerSinkedRecords,
                );
                batch_metrics_handler(
                    &monitor,
                    CounterType::SinkedByteTotal,
                    TaskMetricsType::SinkerSinkedBytes,
                );
            }
        }
    }
    for (metrics_type, value) in batch_metrics {
        result_map
            .entry(metrics_type)
            .and_modify(|v| *v = (*v).max(value))
            .or_insert(value);
    }
}

#[cfg(test)]
mod sinker_worker_tests {
    use std::{collections::BTreeMap, sync::Arc};

    use super::{collect_sinker_worker_metrics, MonitorType, TaskMonitor};
    use crate::{
        config::config_enums::{TaskKind, TaskType},
        monitor::{counter_type::CounterType, monitor::Monitor, task_metrics::TaskMetricsType},
    };

    fn build_task_monitor() -> TaskMonitor {
        build_task_monitor_for(TaskKind::Cdc)
    }

    fn build_task_monitor_for(kind: TaskKind) -> TaskMonitor {
        let task_type = TaskType::new(kind, None);
        #[cfg(not(feature = "metrics"))]
        {
            TaskMonitor::new(Some(task_type))
        }
        #[cfg(feature = "metrics")]
        {
            use std::collections::HashMap;

            use crate::{
                config::metrics_config::MetricsConfig,
                monitor::prometheus_metrics::PrometheusMetrics,
            };

            let prometheus = Arc::new(PrometheusMetrics::new(
                Some(task_type),
                MetricsConfig {
                    http_host: "127.0.0.1".to_owned(),
                    http_port: 0,
                    workers: 1,
                    metrics_labels: HashMap::new(),
                },
            ));
            TaskMonitor::new(Some(task_type), prometheus)
        }
    }

    #[test]
    fn maps_all_sinker_worker_values_to_task_metrics() {
        let task = build_task_monitor();
        let metrics = task.sinker_worker_metrics();
        let worker = metrics.register_worker();
        let guard = worker.enter();
        let mut result = BTreeMap::new();

        collect_sinker_worker_metrics(&metrics, &mut result);

        assert_eq!(result.len(), 2);
        assert_eq!(
            result[&TaskMetricsType::SinkerWorkersConfigured].as_u64(),
            Some(1)
        );
        assert_eq!(
            result[&TaskMetricsType::SinkerWorkersBusy].as_u64(),
            Some(1)
        );

        drop(guard);
    }

    #[tokio::test]
    async fn aggregates_sinker_workers_used_per_drain_by_max_and_average() {
        let task_monitor = build_task_monitor();
        let pipeline_monitor = Arc::new(Monitor::new("pipeline", "task", 60, 1000, 10));
        task_monitor.register(
            "task",
            vec![(MonitorType::Pipeline, pipeline_monitor.clone())],
        );
        pipeline_monitor
            .add_batch_counter(CounterType::SinkerWorkersPerDrain, 2, 1)
            .await
            .add_batch_counter(CounterType::SinkerWorkersPerDrain, 4, 1)
            .await;

        let metrics = task_monitor.calc().await.unwrap();

        assert_eq!(
            metrics[&TaskMetricsType::SinkerWorkersPerDrainMax],
            super::TaskMetricValue::Integer(4)
        );
        assert_eq!(
            metrics[&TaskMetricsType::SinkerWorkersPerDrainAvg],
            super::TaskMetricValue::Integer(3)
        );
    }
    #[tokio::test]
    async fn ensure_monitor_preserves_samples_across_repeated_and_concurrent_calls() {
        let task = build_task_monitor_for(TaskKind::Snapshot);
        let barrier = std::sync::Barrier::new(8);
        let monitors = std::thread::scope(|scope| {
            (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        task.ensure_monitor("table", MonitorType::Sinker, 60, 1000, 10);
                        task.get_monitor("table", &MonitorType::Sinker).unwrap()
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(monitors
            .iter()
            .all(|monitor| Arc::ptr_eq(monitor, &monitors[0])));
        for rows in [100, 200] {
            task.ensure_monitor("table", MonitorType::Sinker, 60, 1000, 10);
            task.add_counter("table", MonitorType::Sinker, CounterType::RecordCount, rows)
                .await;
        }
        let counter = monitors[0]
            .time_window_counters
            .get(&CounterType::RecordCount)
            .unwrap()
            .clone();
        assert_eq!(counter.statistics().await.sum, 300);
    }

    // Capture real log targets on this test's current-thread runtime, without
    // collecting records emitted by concurrently running tests.
    std::thread_local! {
        static CAPTURED_LOGS: std::cell::RefCell<Option<Vec<(String, String)>>> = const {
            std::cell::RefCell::new(None)
        };
    }

    struct CaptureLogger;

    impl log::Log for CaptureLogger {
        fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
            matches!(metadata.target(), "monitor_logger" | "task_logger")
        }

        fn log(&self, record: &log::Record<'_>) {
            if self.enabled(record.metadata()) {
                CAPTURED_LOGS.with(|logs| {
                    if let Some(logs) = logs.borrow_mut().as_mut() {
                        logs.push((record.target().to_string(), record.args().to_string()));
                    }
                });
            }
        }

        fn flush(&self) {}
    }

    #[tokio::test(start_paused = true)]
    async fn flush_groups_new_metrics_with_the_pipeline_id_and_existing_counters() {
        use std::time::Duration;

        use crate::monitor::{task_monitor_handle::TaskMonitorHandle, FlushableMonitor};
        static LOGGER: CaptureLogger = CaptureLogger;
        log::set_logger(&LOGGER).unwrap();
        log::set_max_level(log::LevelFilter::Info);
        for kind in [TaskKind::Cdc, TaskKind::Snapshot] {
            let task = Arc::new(build_task_monitor_for(kind));
            let id = "f49dc9ee7d863b59";
            let handle =
                TaskMonitorHandle::new(task.clone(), MonitorType::Pipeline, id.into(), 10, 100, 10);
            handle.register_monitor(id, handle.build_monitor("pipeline", id));
            let metrics = handle.pipeline_sink_monitor().unwrap();
            for (work_ms, duration_ms) in [(400, 100), (400, 400)] {
                let batch = task.sinker_worker_metrics().start_pipeline_sink(4);
                batch.record_work(Duration::from_millis(work_ms));
                tokio::time::advance(Duration::from_millis(duration_ms)).await;
                TaskMonitorHandle::record_pipeline_sink_metrics(&metrics, batch);
            }
            if let Some(partitioner) = handle.partitioner_monitor() {
                partitioner.add_no_window_counter(
                    CounterType::PartitionerDurationSeconds,
                    0.00025,
                    1,
                );
            }
            handle.add_counter(id, CounterType::BufferSize, 4).await;
            handle
                .add_counter(id, CounterType::SinkerWorkersPerDrain, 2)
                .await;
            handle.set_counter(id, CounterType::SinkedRecordTotal, 141);
            CAPTURED_LOGS.with(|logs| *logs.borrow_mut() = Some(Vec::new()));
            task.flush().await;
            task.unregister(id, vec![MonitorType::Pipeline]);
            let captured = CAPTURED_LOGS.with(|logs| logs.borrow_mut().take().unwrap());
            let monitor_lines = captured
                .iter()
                .filter(|(target, _)| target == "monitor_logger")
                .map(|(_, line)| line.as_str())
                .collect::<Vec<_>>();
            for suffix in [
                "buffer_size | sum=4 | avg=4 | max=4",
                "sinker_workers_per_drain | sum=2 | avg=2 | max=2",
                "sinked_records | latest=141",
                "pipeline_sink_parallel_utilization | avg=0.625",
                "pipeline_sink_duration_seconds | sum=0.5 | avg=0.25",
                "pipeline_sink_operations_total | latest=2",
            ] {
                let expected = format!("pipeline | {id} | {suffix}");
                assert!(monitor_lines.contains(&expected.as_str()), "{expected}");
            }
            let partition = format!(
                "pipeline | {id} | partitioner_duration_seconds | sum=0.00025 | avg=0.00025"
            );
            assert_eq!(
                monitor_lines.contains(&partition.as_str()),
                kind == TaskKind::Snapshot
            );
            assert_eq!(
                monitor_lines
                    .contains(&"pipeline | global | pipeline_sink_operations_total | latest=2"),
                kind == TaskKind::Snapshot,
            );
            let json: serde_json::Value = serde_json::from_str(
                &captured
                    .iter()
                    .find(|(target, _)| target == "task_logger")
                    .unwrap()
                    .1,
            )
            .unwrap();
            assert_eq!(json["pipeline_sink_operations_total"], 2);
            assert_eq!(json["pipeline_sink_parallel_utilization_avg"], 0.625);
            assert_eq!(json["pipeline_sink_duration_seconds_sum"], 0.5);
            assert_eq!(json["pipeline_sink_duration_seconds_avg"], 0.25);
            if kind == TaskKind::Snapshot {
                assert_eq!(json["partitioner_duration_seconds_sum"], 0.00025);
                assert_eq!(json["partitioner_duration_seconds_avg"], 0.00025);
            } else {
                assert!(json.get("partitioner_duration_seconds_sum").is_none());
            }
        }
    }
}
