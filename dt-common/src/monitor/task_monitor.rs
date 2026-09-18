use std::{
    cmp,
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Instant,
};

use async_trait::async_trait;
use dashmap::DashMap;

use super::{
    counter::Counter, group_monitor::GroupMonitor, monitor::Monitor, task_metrics::TaskMetricValue,
};
#[cfg(feature = "metrics")]
use crate::monitor::prometheus_metrics::PrometheusMetrics;
use crate::{
    config::config_enums::{TaskKind, TaskType},
    log_task,
    monitor::{counter_type::CounterType, task_metrics::TaskMetricsType, FlushableMonitor},
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

    fn settle_pipeline_monitor(&self, monitor: &Arc<Monitor>) {
        if let Some(group_monitor) = &self.pipeline_group_monitor {
            group_monitor.settle_no_window_monitor(monitor);
        }
        self.add_no_window_metrics(
            TaskMetricsType::SinkerWorkersConfigured,
            monitor.sinker_worker_snapshot().configured,
        );
    }

    fn collect_pipeline_worker_metrics(&self, metrics: &mut BTreeMap<TaskMetricsType, u64>) {
        let mut configured = self.get_no_window_metric(TaskMetricsType::SinkerWorkersConfigured);
        let mut busy = 0;
        for entry in self.pipelines.iter() {
            if !entry.value().is_tombstone() {
                let snapshot = entry.value().sinker_worker_snapshot();
                configured += snapshot.configured;
                busy += snapshot.busy;
            }
        }
        metrics.insert(TaskMetricsType::SinkerWorkersConfigured, configured);
        metrics.insert(TaskMetricsType::SinkerWorkersBusy, busy);
    }

    fn pipeline_counter_statistics(&self) -> HashMap<CounterType, Counter> {
        // The task's pipeline remains registered through the final flush.
        let mut statistics = HashMap::new();
        for entry in self.pipelines.iter() {
            let monitor = entry.value();
            if !monitor.is_tombstone() {
                monitor.merge_no_window_counters(&mut statistics);
            }
        }
        statistics
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
                    if let Some(previous) =
                        self.pipelines.insert(task_id.to_string(), monitor.clone())
                    {
                        if !Arc::ptr_eq(&previous, &monitor) && !previous.is_tombstone() {
                            self.settle_pipeline_monitor(&previous);
                        }
                    }
                    if let Some(group_monitor) = &self.pipeline_group_monitor {
                        group_monitor.add_monitor(task_id, monitor);
                    }
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
                        if monitor.is_tombstone() {
                            continue;
                        }
                        monitor.mark_tombstone();
                        self.settle_pipeline_monitor(&monitor);
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

        let mut metrics: BTreeMap<TaskMetricsType, u64> = BTreeMap::new();
        let mut calc_handler =
            |calc_type: CalcType, task_metrics_type: TaskMetricsType, val: u64| match calc_type {
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
                    statics.min_by_sec,
                );
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::ExtractorRpsMax,
                    statics.max_by_sec,
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::ExtractorRpsAvg,
                    statics.avg_by_sec,
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
                    statics.min_by_sec,
                );
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::ExtractorBpsMax,
                    statics.max_by_sec,
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::ExtractorBpsAvg,
                    statics.avg_by_sec,
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
                    statics.min_by_sec,
                );
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::ExtractorPushedRpsMax,
                    statics.max_by_sec,
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::ExtractorPushedRpsAvg,
                    statics.avg_by_sec,
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
                    statics.min_by_sec,
                );
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::ExtractorPushedBpsMax,
                    statics.max_by_sec,
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::ExtractorPushedBpsAvg,
                    statics.avg_by_sec,
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
                    statics.max,
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::SinkerWorkersPerDrainAvg,
                    statics.avg_by_count,
                );
            }
        }

        let sinkers: Vec<Arc<Monitor>> = self
            .sinkers
            .iter()
            .map(|item| item.value().clone())
            .collect();

        // Keep the existing task-level tombstone filter; only fix aggregation.
        let sinkers: Vec<_> = sinkers
            .into_iter()
            .filter(|monitor| !monitor.is_tombstone())
            .collect();
        let statistics = GroupMonitor::window_statistics(&sinkers, Instant::now()).await;
        for monitor in sinkers {
            calc_monitors.push((MonitorType::Sinker, monitor));
        }
        for (counter_type, min_type, max_type, avg_type) in [
            (
                CounterType::RtPerQuery,
                TaskMetricsType::SinkerRtMin,
                TaskMetricsType::SinkerRtMax,
                TaskMetricsType::SinkerRtAvg,
            ),
            (
                CounterType::RecordCount,
                TaskMetricsType::SinkerRpsMin,
                TaskMetricsType::SinkerRpsMax,
                TaskMetricsType::SinkerRpsAvg,
            ),
            (
                CounterType::DataBytes,
                TaskMetricsType::SinkerBpsMin,
                TaskMetricsType::SinkerBpsMax,
                TaskMetricsType::SinkerBpsAvg,
            ),
        ] {
            if let Some(statistics) = statistics.get(&counter_type) {
                calc_handler(CalcType::Min, min_type, statistics.min_by_sec);
                calc_handler(CalcType::Max, max_type, statistics.max_by_sec);
                calc_handler(CalcType::Avg, avg_type, statistics.avg_by_sec);
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
                    statics.min_by_sec,
                );
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::CheckerRpsMax,
                    statics.max_by_sec,
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::CheckerRpsAvg,
                    statics.avg_by_sec,
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
                    statics.min_by_sec,
                );
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::CheckerMissRpsMax,
                    statics.max_by_sec,
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::CheckerMissRpsAvg,
                    statics.avg_by_sec,
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
                    statics.min_by_sec,
                );
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::CheckerDiffRpsMax,
                    statics.max_by_sec,
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::CheckerDiffRpsAvg,
                    statics.avg_by_sec,
                );
            }
        }
        calc_nowindow_metrics(&self.no_window_metrics_map, calc_monitors);

        let mut total_progress_count = 0;
        let mut finished_progress_count = 0;
        for item in self.no_window_metrics_map.iter() {
            metrics.insert(*item.key(), *item.value());
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
                cmp::min(finished_progress_count * 100 / total_progress_count, 100),
            );
        }
        self.collect_pipeline_worker_metrics(&mut metrics);

        let mut metrics = metrics
            .into_iter()
            .map(|(key, value)| (key, TaskMetricValue::Integer(value)))
            .collect::<BTreeMap<_, _>>();
        for (counter_type, counter) in self.pipeline_counter_statistics() {
            for (aggregate, metric) in counter_type.task_metrics() {
                metrics.insert(metric, counter.aggregate(&aggregate));
            }
        }
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

    use super::{MonitorType, TaskMonitor};
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
        let monitor = Arc::new(Monitor::new("pipeline", "test", 1, 1, 1));
        task.register("test", vec![(MonitorType::Pipeline, monitor.clone())]);
        let metrics = monitor.sinker_worker_metrics();
        let worker = metrics.register_worker();
        let guard = worker.enter();
        let mut result = BTreeMap::new();

        task.collect_pipeline_worker_metrics(&mut result);

        assert_eq!(result.len(), 2);
        assert_eq!(result[&TaskMetricsType::SinkerWorkersConfigured], 1);
        assert_eq!(result[&TaskMetricsType::SinkerWorkersBusy], 1);

        drop(guard);
    }

    #[tokio::test]
    async fn pipeline_workers_are_shared_with_wrappers_and_settled_once() {
        use crate::monitor::task_monitor_handle::TaskMonitorHandle;
        for kind in [TaskKind::Snapshot, TaskKind::Cdc] {
            let task = Arc::new(build_task_monitor_for(kind));
            let a =
                TaskMonitorHandle::new(task.clone(), MonitorType::Pipeline, "a".into(), 1, 1, 1);
            let b =
                TaskMonitorHandle::new(task.clone(), MonitorType::Pipeline, "b".into(), 1, 1, 1);
            // Sinker construction may ask for its tracker before the parallelizer.
            let a_workers = a.with_type(MonitorType::Sinker).sinker_worker_metrics();
            let a_monitor = a.pipeline_sink_monitor().unwrap();
            assert!(Arc::ptr_eq(&a_workers, &a_monitor.sinker_worker_metrics()));
            a.register_monitor("a", a_monitor.clone());
            assert!(Arc::ptr_eq(&a_workers, &a.sinker_worker_metrics()));
            let b_workers = b.sinker_worker_metrics();
            assert!(!Arc::ptr_eq(&a_workers, &b_workers));
            let a_worker = a_workers.register_worker();
            let b_worker = b_workers.register_worker();
            let _other_b_worker = b_workers.register_worker();
            let active_a = a_worker.enter();
            let active_b = b_worker.enter();
            let values = task.calc().await.unwrap();
            assert_eq!(
                values[&TaskMetricsType::SinkerWorkersConfigured].as_u64(),
                Some(3)
            );
            assert_eq!(
                values[&TaskMetricsType::SinkerWorkersBusy].as_u64(),
                Some(2)
            );
            assert_eq!(a_workers.snapshot().configured, 1);
            assert_eq!(b_workers.snapshot().configured, 2);
            drop(active_a);
            a.unregister_monitor("a");
            a.unregister_monitor("a");
            task.cleanup_monitors().await;
            let values = task.calc().await.unwrap();
            assert_eq!(
                values[&TaskMetricsType::SinkerWorkersConfigured].as_u64(),
                Some(3)
            );
            assert_eq!(
                values[&TaskMetricsType::SinkerWorkersBusy].as_u64(),
                Some(1)
            );
            drop(active_b);
            // Replacing an active pool also settles its configured count once.
            let replacement = Arc::new(Monitor::new("pipeline", "b", 1, 1, 1));
            b.register_monitor("b", replacement.clone());
            let _new_worker = replacement.sinker_worker_metrics().register_worker();
            b.unregister_monitor("b");
            b.unregister_monitor("b");
            task.cleanup_monitors().await;
            let values = task.calc().await.unwrap();
            assert_eq!(
                values[&TaskMetricsType::SinkerWorkersConfigured].as_u64(),
                Some(4)
            );
            assert_eq!(
                values[&TaskMetricsType::SinkerWorkersBusy].as_u64(),
                Some(0)
            );
        }
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
    async fn ensure_preserves_batches_and_does_not_revive_completed_tables() {
        let monitor = build_task_monitor_for(TaskKind::Snapshot);
        for rows in [100, 200] {
            monitor.ensure_monitor("table", MonitorType::Sinker, 60, 1000, 10);
            monitor
                .add_counter("table", MonitorType::Sinker, CounterType::RecordCount, rows)
                .await;
        }
        let sinker = monitor.get_monitor("table", &MonitorType::Sinker).unwrap();
        let counter = sinker
            .time_window_counters
            .get(&CounterType::RecordCount)
            .unwrap()
            .clone();
        assert_eq!(counter.statistics().await.sum, 300);
        monitor.unregister("table", vec![MonitorType::Sinker]);
        monitor.ensure_monitor("table", MonitorType::Sinker, 60, 1000, 10);
        assert!(sinker.is_tombstone());
        // Tombstone filtering is explicitly outside this change's scope.
        assert!(!monitor
            .calc()
            .await
            .unwrap()
            .contains_key(&TaskMetricsType::SinkerRpsAvg));
    }

    #[test]
    fn concurrent_ensure_does_not_replace_another_worker_monitor() {
        let monitor = build_task_monitor_for(TaskKind::Snapshot);
        let barrier = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            let threads = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        monitor.ensure_monitor("table", MonitorType::Sinker, 60, 1000, 10);
                        monitor.get_monitor("table", &MonitorType::Sinker).unwrap()
                    })
                })
                .collect::<Vec<_>>();
            let workers = threads
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .collect::<Vec<_>>();
            assert!(workers
                .iter()
                .all(|worker| Arc::ptr_eq(worker, &workers[0])));
        });
    }

    #[tokio::test]
    async fn task_sinker_traffic_merges_seconds_and_preserves_rt_units() {
        use std::time::{Duration, Instant};
        let now = Instant::now();
        let task = build_task_monitor_for(TaskKind::Snapshot);
        for (id, first, second) in [("a", 100, 20), ("b", 10, 200), ("c", 0, 0)] {
            let monitor = Arc::new(Monitor::new("sinker", id, 60, 1000, 10));
            task.register(id, vec![(MonitorType::Sinker, monitor.clone())]);
            for kind in [
                CounterType::RecordCount,
                CounterType::DataBytes,
                CounterType::RtPerQuery,
            ] {
                for (age, value) in [(1, first), (2, second)] {
                    monitor.add_counter(kind.clone(), value).await;
                    let counter = monitor.time_window_counters.get(&kind).unwrap().clone();
                    counter.counters.write().await.back_mut().unwrap().timestamp =
                        now - Duration::from_secs(age);
                }
            }
            monitor
                .add_counter(CounterType::RecordsPerQuery, 9999)
                .await;
        }
        let result = task.calc().await.unwrap();
        for key in [
            TaskMetricsType::SinkerRpsAvg,
            TaskMetricsType::SinkerBpsAvg,
            TaskMetricsType::SinkerRtAvg,
        ] {
            assert_eq!(result[&key], super::TaskMetricValue::Integer(165));
        }
        assert_eq!(
            result[&TaskMetricsType::SinkerRpsMin],
            super::TaskMetricValue::Integer(110)
        );
        assert_eq!(
            result[&TaskMetricsType::SinkerRpsMax],
            super::TaskMetricValue::Integer(220)
        );
    }

    fn counter_values(
        counters: &std::collections::HashMap<CounterType, super::Counter>,
    ) -> BTreeMap<
        String,
        (
            u64,
            super::TaskMetricValue,
            super::TaskMetricValue,
            super::TaskMetricValue,
        ),
    > {
        counters
            .iter()
            .map(|(key, counter)| {
                (
                    key.to_string(),
                    (counter.count, counter.value, counter.min, counter.max),
                )
            })
            .collect()
    }

    fn counter_logs(
        counters: &std::collections::HashMap<CounterType, super::Counter>,
    ) -> Vec<String> {
        let mut logs = counters
            .iter()
            .map(|(key, counter)| counter.log_line("pipeline", "global", key))
            .collect::<Vec<_>>();
        logs.sort();
        logs
    }

    #[tokio::test(start_paused = true)]
    async fn batch_measurement_is_available_to_snapshot_and_cdc_handles() {
        use std::time::Duration;

        use crate::monitor::{
            pipeline_sink_metrics::PipelineSinkMetricsGuard, task_monitor_handle::TaskMonitorHandle,
        };
        for kind in [TaskKind::Snapshot, TaskKind::Cdc] {
            let task = Arc::new(build_task_monitor_for(kind));
            let handle = TaskMonitorHandle::new(
                task.clone(),
                MonitorType::Pipeline,
                "pipeline".into(),
                1,
                100,
                10,
            );
            let monitor = handle.pipeline_sink_monitor().unwrap();
            let batch = PipelineSinkMetricsGuard::new(monitor.sinker_worker_metrics(), 2, 1);
            batch.record_work(Duration::from_millis(100));
            tokio::time::advance(Duration::from_millis(100)).await;
            TaskMonitorHandle::record_pipeline_sink_metrics(&monitor, batch);
            let values = task.calc().await.unwrap();
            assert_eq!(
                values[&TaskMetricsType::PipelineSinkOperationsTotal],
                super::TaskMetricValue::Integer(1)
            );
            assert_eq!(
                values[&TaskMetricsType::PipelineSinkParallelUtilizationAvg],
                super::TaskMetricValue::Float(0.5)
            );
            let counters = task.pipeline_counter_statistics();
            assert_eq!(counters.len(), 3);
            assert!(!counters.contains_key(&CounterType::PartitionerDurationSeconds));
        }
        assert!(TaskMonitorHandle::noop(MonitorType::Pipeline)
            .pipeline_sink_monitor()
            .is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn partitioner_metrics_use_pipeline_counters_in_json() {
        use std::time::Duration;

        use crate::monitor::task_monitor_handle::TaskMonitorHandle;
        let task = Arc::new(build_task_monitor_for(TaskKind::Snapshot));
        assert!(!task
            .calc()
            .await
            .unwrap()
            .contains_key(&TaskMetricsType::PartitionerDurationSecondsSum));
        let handle = TaskMonitorHandle::new(
            task.clone(),
            MonitorType::Pipeline,
            "pipeline".into(),
            1,
            1,
            1,
        );
        let monitor = handle.partitioner_monitor().unwrap();
        assert!(Arc::ptr_eq(
            &monitor,
            &handle.partitioner_monitor().unwrap()
        ));
        for seconds in [0.0009, 0.0001, 0.0002] {
            handle
                .add_counter("pipeline", CounterType::PartitionerDurationSeconds, seconds)
                .await;
        }
        assert_eq!(
            monitor
                .no_window_counters
                .get(&CounterType::PartitionerDurationSeconds)
                .unwrap()
                .count,
            3
        );
        let counters = task.pipeline_counter_statistics();
        let logs = counter_logs(&counters);
        assert_eq!(logs.len(), 1);
        let fields = logs[0]
            .strip_prefix("pipeline | global | partitioner_duration_seconds | ")
            .unwrap()
            .split(" | ")
            .map(|field| field.split_once('=').unwrap())
            .collect::<BTreeMap<_, _>>();
        let json = serde_json::to_value(task.calc().await.unwrap()).unwrap();
        for (suffix, expected) in [
            ("sum", 0.0012),
            ("min", 0.0001),
            ("max", 0.0009),
            ("avg", 0.0004),
        ] {
            let key = format!("partitioner_duration_seconds_{suffix}");
            assert!((json[&key].as_f64().unwrap() - expected).abs() < 1e-12);
            assert_eq!(
                fields[suffix].parse::<f64>().unwrap(),
                json[&key].as_f64().unwrap()
            );
        }
        assert!(json.get("pipeline_sink_operations_total").is_none());
        tokio::time::advance(Duration::from_secs(86400)).await;
        task.cleanup_monitors().await;
        assert_eq!(counter_logs(&task.pipeline_counter_statistics()), logs);
        assert_eq!(
            serde_json::to_value(task.calc().await.unwrap()).unwrap(),
            json
        );
        assert_eq!(
            counter_values(
                &task
                    .pipeline_group_monitor
                    .as_ref()
                    .unwrap()
                    .no_window_statistics()
            ),
            counter_values(&counters)
        );
        assert!(TaskMonitorHandle::noop(MonitorType::Pipeline)
            .partitioner_monitor()
            .is_none());
        let cdc = TaskMonitorHandle::new(
            Arc::new(build_task_monitor()),
            MonitorType::Pipeline,
            "cdc".into(),
            1,
            100,
            10,
        );
        assert!(cdc.partitioner_monitor().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn task_exports_fractional_batch_metrics_and_keeps_old_integer_json() {
        use std::time::Duration;

        use crate::monitor::{
            pipeline_sink_metrics::PipelineSinkMetricsGuard, task_monitor_handle::TaskMonitorHandle,
        };
        let task = Arc::new(build_task_monitor_for(TaskKind::Snapshot));
        assert!(task.pipeline_counter_statistics().is_empty());
        assert!(!task
            .calc()
            .await
            .unwrap()
            .contains_key(&TaskMetricsType::PipelineSinkOperationsTotal));
        let handle = TaskMonitorHandle::new(
            task.clone(),
            MonitorType::Pipeline,
            "pipeline".into(),
            1,
            100,
            10,
        );
        let monitor = handle.pipeline_sink_monitor().unwrap();
        let empty = serde_json::to_value(task.calc().await.unwrap()).unwrap();
        assert_eq!(empty["pipeline_sink_operations_total"].as_u64(), Some(0));
        assert_eq!(
            empty["pipeline_sink_parallel_utilization_min"].as_f64(),
            Some(0.0)
        );
        for (work, duration) in [(400, 100), (400, 400)] {
            let batch = PipelineSinkMetricsGuard::new(monitor.sinker_worker_metrics(), 4, 1);
            batch.record_work(Duration::from_millis(work));
            tokio::time::advance(Duration::from_millis(duration)).await;
            TaskMonitorHandle::record_pipeline_sink_metrics(&monitor, batch);
        }
        // Exercise the generic counter too: no integer-to-float round trip is allowed.
        handle.set_counter("pipeline", CounterType::SinkedRecordTotal, u64::MAX);
        let result = task.calc().await.unwrap();
        let logs = counter_logs(&task.pipeline_counter_statistics());
        assert!(logs.contains(
            &"pipeline | global | pipeline_sink_parallel_utilization | avg=0.625 | min=0.25 | max=1"
                .into()
        ));
        assert!(logs.contains(&"pipeline | global | pipeline_sink_duration_seconds | sum=0.5 | avg=0.25 | min=0.1 | max=0.4".into()));
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(
            json["pipeline_sink_parallel_utilization_avg"].as_f64(),
            Some(0.625)
        );
        assert_eq!(
            json["pipeline_sink_duration_seconds_sum"].as_f64(),
            Some(0.5)
        );
        assert_eq!(json["pipeline_sink_operations_total"].as_u64(), Some(2));
        assert_eq!(json["sinker_sinked_records"].as_u64(), Some(u64::MAX));
        assert!(!json
            .as_object()
            .unwrap()
            .keys()
            .any(|key| key.contains("sink_work")));
        #[cfg(feature = "metrics")]
        {
            task.prometheus_metrics.initialization().unwrap();
            task.prometheus_metrics.set_metrics(&result);
        }
        tokio::time::advance(Duration::from_secs(86400)).await;
        task.cleanup_monitors().await;
        assert_eq!(task.pipelines.len(), 1);
        assert_eq!(
            serde_json::to_value(task.calc().await.unwrap()).unwrap(),
            json
        );
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

        use crate::monitor::{
            pipeline_sink_metrics::PipelineSinkMetricsGuard,
            task_monitor_handle::TaskMonitorHandle, FlushableMonitor,
        };
        static LOGGER: CaptureLogger = CaptureLogger;
        log::set_logger(&LOGGER).unwrap();
        log::set_max_level(log::LevelFilter::Info);
        for kind in [TaskKind::Cdc, TaskKind::Snapshot] {
            let task = Arc::new(build_task_monitor_for(kind));
            let id = "f49dc9ee7d863b59";
            let handle =
                TaskMonitorHandle::new(task.clone(), MonitorType::Pipeline, id.into(), 10, 100, 10);
            let metrics = handle.pipeline_sink_monitor().unwrap();
            let batch = PipelineSinkMetricsGuard::new(metrics.sinker_worker_metrics(), 4, 1);
            batch.record_work(Duration::from_millis(500));
            tokio::time::advance(Duration::from_millis(125)).await;
            TaskMonitorHandle::record_pipeline_sink_metrics(&metrics, batch);
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
                "pipeline_sink_parallel_utilization | avg=1 | min=1 | max=1",
                "pipeline_sink_duration_seconds | sum=0.125 | avg=0.125 | min=0.125 | max=0.125",
                "pipeline_sink_operations_total | latest=1",
            ] {
                let expected = format!("pipeline | {id} | {suffix}");
                assert_eq!(
                    monitor_lines
                        .iter()
                        .filter(|line| **line == expected)
                        .count(),
                    1,
                    "{expected}"
                );
            }
            let partition = format!("pipeline | {id} | partitioner_duration_seconds | sum=0.00025 | avg=0.00025 | min=0.00025 | max=0.00025");
            assert_eq!(
                monitor_lines.contains(&partition.as_str()),
                kind == TaskKind::Snapshot
            );
            let global_count = monitor_lines
                .iter()
                .filter(|line| {
                    **line == "pipeline | global | pipeline_sink_operations_total | latest=1"
                })
                .count();
            assert_eq!(global_count, usize::from(kind == TaskKind::Snapshot));
            if kind == TaskKind::Cdc {
                assert!(!monitor_lines
                    .iter()
                    .any(|line| line.starts_with("pipeline | global |")));
            }
            let json: serde_json::Value = serde_json::from_str(
                &captured
                    .iter()
                    .find(|(target, _)| target == "task_logger")
                    .unwrap()
                    .1,
            )
            .unwrap();
            assert_eq!(json["pipeline_sink_operations_total"], 1);
            assert_eq!(json["pipeline_sink_parallel_utilization_avg"], 1.0);
            assert_eq!(json["pipeline_sink_duration_seconds_avg"], 0.125);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn batch_total_uses_pipeline_counters_without_expiration() {
        use crate::monitor::counter_type::{AggregateType, WindowType};

        assert!(matches!(
            CounterType::PipelineSinkOperationsTotal.get_window_type(),
            WindowType::NoWindow
        ));
        assert!(
            CounterType::PipelineSinkOperationsTotal.get_aggregate_types()
                == [AggregateType::Latest]
        );
        for kind in [TaskKind::Snapshot, TaskKind::Cdc] {
            let task = build_task_monitor_for(kind);
            for _ in 0..3 {
                task.ensure_monitor("pipeline", MonitorType::Pipeline, 1, 1, 1);
                task.add_counter(
                    "pipeline",
                    MonitorType::Pipeline,
                    CounterType::PipelineSinkOperationsTotal,
                    1,
                )
                .await;
            }
            // Recording the total alone does not enable timing counters.
            assert!(!task
                .pipeline_counter_statistics()
                .contains_key(&CounterType::PipelineSinkParallelUtilization));
            tokio::time::advance(std::time::Duration::from_secs(86400)).await;
            task.cleanup_monitors().await;
            assert_eq!(
                task.calc().await.unwrap()[&TaskMetricsType::PipelineSinkOperationsTotal].as_u64(),
                Some(3)
            );
            if let Some(group) = &task.pipeline_group_monitor {
                assert_eq!(
                    group.no_window_statistics()[&CounterType::PipelineSinkOperationsTotal]
                        .value
                        .as_u64(),
                    Some(3)
                );
            }
            assert!(!build_task_monitor_for(kind)
                .calc()
                .await
                .unwrap()
                .contains_key(&TaskMetricsType::PipelineSinkOperationsTotal));
        }
    }
}
