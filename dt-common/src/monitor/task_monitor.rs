use std::{cmp, collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use dashmap::DashMap;

use super::monitor::Monitor;
#[cfg(feature = "metrics")]
use crate::monitor::prometheus_metrics::PrometheusMetrics;
use crate::{
    config::config_enums::TaskType,
    log_task,
    monitor::{counter_type::CounterType, task_metrics::TaskMetricsType, FlushableMonitor},
};

#[derive(Clone)]
pub struct TaskMonitor {
    task_type: Option<TaskType>,

    extractors: DashMap<String, Arc<Monitor>>,
    pipelines: DashMap<String, Arc<Monitor>>,
    sinkers: DashMap<String, Arc<Monitor>>,
    checkers: DashMap<String, Arc<Monitor>>,

    no_window_metrics_map: DashMap<TaskMetricsType, u64>,
    #[cfg(feature = "metrics")]
    pub prometheus_metrics: Arc<PrometheusMetrics>,
}

#[derive(PartialEq, Eq, Hash, Clone)]
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
    async fn flush(&self) {
        if self.task_type.is_none() {
            return;
        }

        self.reset_before_calc();
        if let Some(metrics) = self.calc().await {
            log_task!("{}", serde_json::to_string(&metrics).unwrap());
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
            extractors: DashMap::new(),
            pipelines: DashMap::new(),
            sinkers: DashMap::new(),
            checkers: DashMap::new(),
            no_window_metrics_map: DashMap::new(),
            prometheus_metrics,
        }
    }

    pub fn get_task_type(&self) -> Option<&TaskType> {
        self.task_type.as_ref()
    }

    pub fn register(&self, task_id: &str, monitors: Vec<(MonitorType, Arc<Monitor>)>) {
        if self.task_type.is_none() {
            return;
        }

        for (monitor_type, monitor) in monitors {
            match monitor_type {
                MonitorType::Extractor => {
                    self.extractors.insert(task_id.to_string(), monitor);
                }
                MonitorType::Pipeline => {
                    self.pipelines.insert(task_id.to_string(), monitor);
                }
                MonitorType::Sinker => {
                    self.sinkers.insert(task_id.to_string(), monitor);
                }
                MonitorType::Checker => {
                    self.checkers.insert(task_id.to_string(), monitor);
                }
            }
        }
    }

    pub fn unregister(&self, task_id: &str, monitors: Vec<MonitorType>) {
        if self.task_type.is_none() {
            return;
        }

        let mut calc_monitors = Vec::new();
        for monitor_type in monitors {
            match monitor_type {
                MonitorType::Extractor => {
                    if let Some((_, monitor)) = self.extractors.remove(task_id) {
                        calc_monitors.push((MonitorType::Extractor, monitor.clone()));
                    };
                }
                MonitorType::Pipeline => {
                    if let Some((_, monitor)) = self.pipelines.remove(task_id) {
                        calc_monitors.push((MonitorType::Pipeline, monitor.clone()));
                    };
                }
                MonitorType::Sinker => {
                    if let Some((_, monitor)) = self.sinkers.remove(task_id) {
                        calc_monitors.push((MonitorType::Sinker, monitor.clone()));
                    };
                }
                MonitorType::Checker => {
                    if let Some((_, monitor)) = self.checkers.remove(task_id) {
                        calc_monitors.push((MonitorType::Checker, monitor.clone()));
                    };
                }
            }
        }
        calc_nowindow_metrics(&self.no_window_metrics_map, calc_monitors);
    }

    pub fn add_no_window_metrics(&self, metrics_type: TaskMetricsType, value: u64) {
        self.no_window_metrics_map
            .entry(metrics_type)
            .and_modify(|v| *v += value)
            .or_insert(value);
    }

    async fn calc(&self) -> Option<BTreeMap<TaskMetricsType, u64>> {
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
            calc_monitors.push((MonitorType::Extractor, monitor.clone()));
            // extractor rps
            if let Some(counter) = monitor
                .time_window_counters
                .get(&CounterType::ExtractedRecords)
            {
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
            if let Some(counter) = monitor
                .time_window_counters
                .get(&CounterType::ExtractedBytes)
            {
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
            if let Some(counter) = monitor.time_window_counters.get(&CounterType::RecordCount) {
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
            if let Some(counter) = monitor.time_window_counters.get(&CounterType::DataBytes) {
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
            calc_monitors.push((MonitorType::Pipeline, monitor.clone()));
            if let Some(counter) = monitor
                .time_window_counters
                .get(&CounterType::CheckerLagSeconds)
            {
                let statics = counter.statistics().await;
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::CheckerLagAvgSeconds,
                    statics.avg_by_count,
                );
            }
        }

        let sinkers: Vec<Arc<Monitor>> = self
            .sinkers
            .iter()
            .map(|item| item.value().clone())
            .collect();

        for monitor in sinkers {
            calc_monitors.push((MonitorType::Sinker, monitor.clone()));
            // sinker rt
            if let Some(counter) = monitor.time_window_counters.get(&CounterType::RtPerQuery) {
                let statics = counter.statistics().await;
                calc_handler(
                    CalcType::Min,
                    TaskMetricsType::SinkerRtMin,
                    statics.min_by_sec,
                );
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::SinkerRtMax,
                    statics.max_by_sec,
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::SinkerRtAvg,
                    statics.avg_by_sec,
                );
            }
            // sinker rps
            if let Some(counter) = monitor
                .time_window_counters
                .get(&CounterType::RecordsPerQuery)
            {
                let statics = counter.statistics().await;
                calc_handler(
                    CalcType::Min,
                    TaskMetricsType::SinkerRpsMin,
                    statics.min_by_sec,
                );
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::SinkerRpsMax,
                    statics.max_by_sec,
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::SinkerRpsAvg,
                    statics.avg_by_sec,
                );
            }
            // sinker bps
            if let Some(counter) = monitor.time_window_counters.get(&CounterType::DataBytes) {
                let statics = counter.statistics().await;
                calc_handler(
                    CalcType::Min,
                    TaskMetricsType::SinkerBpsMin,
                    statics.min_by_sec,
                );
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::SinkerBpsMax,
                    statics.max_by_sec,
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::SinkerBpsAvg,
                    statics.avg_by_sec,
                );
            }
        }

        let checkers: Vec<Arc<Monitor>> = self
            .checkers
            .iter()
            .map(|item| item.value().clone())
            .collect();

        for monitor in checkers {
            calc_monitors.push((MonitorType::Checker, monitor.clone()));
            // checker checked records
            if let Some(counter) = monitor.time_window_counters.get(&CounterType::RecordCount) {
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
            if let Some(counter) = monitor
                .time_window_counters
                .get(&CounterType::CheckerMissCount)
            {
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
            if let Some(counter) = monitor
                .time_window_counters
                .get(&CounterType::CheckerDiffCount)
            {
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
            // checker generated sql
            if let Some(counter) = monitor
                .time_window_counters
                .get(&CounterType::CheckerGenerateSqlCount)
            {
                let statics = counter.statistics().await;
                calc_handler(
                    CalcType::Min,
                    TaskMetricsType::CheckerGenerateSqlRpsMin,
                    statics.min_by_sec,
                );
                calc_handler(
                    CalcType::Max,
                    TaskMetricsType::CheckerGenerateSqlRpsMax,
                    statics.max_by_sec,
                );
                calc_handler(
                    CalcType::Avg,
                    TaskMetricsType::CheckerGenerateSqlRpsAvg,
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
            #[cfg(feature = "metrics")]
            self.prometheus_metrics.set_metrics(&metrics);
        }
        if total_progress_count > 0 {
            metrics.insert(
                TaskMetricsType::Progress,
                cmp::min(finished_progress_count * 100 / total_progress_count, 100),
            );
        }

        Some(metrics)
    }

    fn reset_before_calc(&self) {
        self.no_window_metrics_map
            .remove(&TaskMetricsType::PipelineQueueSize);
        self.no_window_metrics_map
            .remove(&TaskMetricsType::PipelineQueueBytes);
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
            match calc_type {
                CalcType::Add => {
                    result_map
                        .entry(metrics_type)
                        .and_modify(|v| *v += counter.value)
                        .or_insert(counter.value);
                }
                CalcType::Max => {
                    result_map
                        .entry(metrics_type)
                        .and_modify(|v| *v = (*v).max(counter.value))
                        .or_insert(counter.value);
                }
                CalcType::Latest => {
                    result_map
                        .entry(metrics_type)
                        .and_modify(|v| *v = counter.value)
                        .or_insert(counter.value);
                }
                _ => {}
            }
        }
    };
    let batch_metrics_handler =
        |monitor: &Arc<Monitor>, counter_type: CounterType, metrics_type: TaskMetricsType| {
            if let Some(counter) = monitor.no_window_counters.get(&counter_type) {
                batch_metrics
                    .entry(metrics_type)
                    .and_modify(|v| *v += counter.value)
                    .or_insert(counter.value);
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
                metric_handler(
                    &monitor,
                    CounterType::CheckerAsyncDropCount,
                    TaskMetricsType::CheckerAsyncDropCount,
                    CalcType::Add,
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
