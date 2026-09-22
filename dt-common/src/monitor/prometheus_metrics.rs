#[cfg(feature = "tracing")]
use std::sync::OnceLock;
#[cfg(feature = "metrics")]
use std::{collections::BTreeMap, sync::Arc};

use actix_web::{middleware::Logger, web, App, HttpResponse, HttpServer, Responder, Result};
use anyhow::Context;
use dashmap::DashMap;
use prometheus::{Gauge, Opts, Registry, TextEncoder};

#[cfg(feature = "tracing")]
use super::runtime_trace_monitor::RuntimeTraceMetrics;
use crate::config::config_enums::{TaskKind, TaskType};
use crate::config::metrics_config::MetricsConfig;
use crate::error::DtError;
use crate::monitor::task_metrics::{TaskMetricValue, TaskMetricsType};

pub struct PrometheusMetrics {
    registry: Arc<Registry>,
    metrics: DashMap<TaskMetricsType, Gauge>,
    task_type: Option<TaskType>,
    config: MetricsConfig,
    #[cfg(feature = "tracing")]
    runtime_trace_metrics: OnceLock<RuntimeTraceMetrics>,
}

impl PrometheusMetrics {
    pub fn new(task_type: Option<TaskType>, config: MetricsConfig) -> Self {
        Self {
            registry: Arc::new(Registry::new()),
            metrics: DashMap::new(),
            task_type,
            #[cfg(feature = "tracing")]
            runtime_trace_metrics: OnceLock::new(),
            config,
        }
    }

    #[cfg(feature = "tracing")]
    pub(super) fn runtime_trace_metrics(&self) -> Option<&RuntimeTraceMetrics> {
        self.runtime_trace_metrics.get()
    }

    pub fn initialization(&self) -> anyhow::Result<&Self> {
        let register_handler = |metrics_name: &str,
                                metrics_desc: &str,
                                metrics_type: TaskMetricsType|
         -> anyhow::Result<()> {
            let metrics = Gauge::with_opts(
                Opts::new(metrics_name, metrics_desc)
                    .const_labels(self.config.metrics_labels.to_owned()),
            )
            .context(DtError::MetricsInitializationFailed(format!(
                "Failed to initialize metric [{metrics_name}]"
            )))?;

            self.registry.register(Box::new(metrics.clone())).context(
                DtError::MetricsInitializationFailed(format!(
                    "Failed to initialize metric [{metrics_name}]"
                )),
            )?;
            self.metrics.insert(metrics_type, metrics);
            Ok(())
        };

        #[cfg(feature = "tracing")]
        self.runtime_trace_metrics
            .set(RuntimeTraceMetrics::initialization(
                &self.config,
                &self.registry,
            )?)
            .map_err(|_| {
                DtError::MetricsInitializationFailed(
                    "runtime trace metrics already initialized".to_owned(),
                )
            })?;

        register_handler(
            "extractor_rps_max",
            "the max records per second of extractor",
            TaskMetricsType::ExtractorRpsMax,
        )?;
        register_handler(
            "extractor_rps_min",
            "the min records per second of extractor",
            TaskMetricsType::ExtractorRpsMin,
        )?;
        register_handler(
            "extractor_rps_avg",
            "the average records per second of extractor",
            TaskMetricsType::ExtractorRpsAvg,
        )?;
        register_handler(
            "extractor_bps_max",
            "the max bytes per second of extractor",
            TaskMetricsType::ExtractorBpsMax,
        )?;
        register_handler(
            "extractor_bps_min",
            "the min bytes per second of extractor",
            TaskMetricsType::ExtractorBpsMin,
        )?;
        register_handler(
            "extractor_bps_avg",
            "the average bytes per second of extractor",
            TaskMetricsType::ExtractorBpsAvg,
        )?;

        register_handler(
            "extractor_pushed_rps_max",
            "the max pushed records per second of extractor",
            TaskMetricsType::ExtractorPushedRpsMax,
        )?;
        register_handler(
            "extractor_pushed_rps_min",
            "the min pushed records per second of extractor",
            TaskMetricsType::ExtractorPushedRpsMin,
        )?;
        register_handler(
            "extractor_pushed_rps_avg",
            "the average pushed records per second of extractor",
            TaskMetricsType::ExtractorPushedRpsAvg,
        )?;
        register_handler(
            "extractor_pushed_bps_max",
            "the max pushed bytes per second of extractor",
            TaskMetricsType::ExtractorPushedBpsMax,
        )?;
        register_handler(
            "extractor_pushed_bps_min",
            "the min pushed bytes per second of extractor",
            TaskMetricsType::ExtractorPushedBpsMin,
        )?;
        register_handler(
            "extractor_pushed_bps_avg",
            "the average pushed bytes per second of extractor",
            TaskMetricsType::ExtractorPushedBpsAvg,
        )?;

        register_handler(
            "pipeline_queue_size",
            "the records size of pipeline queue",
            TaskMetricsType::PipelineQueueSize,
        )?;
        register_handler(
            "pipeline_queue_bytes",
            "the bytes in pipeline queue",
            TaskMetricsType::PipelineQueueBytes,
        )?;

        register_handler(
            "sinker_rt_max",
            "the max response time of sinker, the unit is millisecond",
            TaskMetricsType::SinkerRtMax,
        )?;
        register_handler(
            "sinker_rt_min",
            "the min response time of sinker, the unit is millisecond",
            TaskMetricsType::SinkerRtMin,
        )?;
        register_handler(
            "sinker_rt_avg",
            "the average response time of sinker, the unit is millisecond",
            TaskMetricsType::SinkerRtAvg,
        )?;

        register_handler(
            "sinker_rps_max",
            "the max records per second of sinker",
            TaskMetricsType::SinkerRpsMax,
        )?;
        register_handler(
            "sinker_rps_min",
            "the min records per second of sinker",
            TaskMetricsType::SinkerRpsMin,
        )?;
        register_handler(
            "sinker_rps_avg",
            "the average records per second of sinker",
            TaskMetricsType::SinkerRpsAvg,
        )?;
        register_handler(
            "sinker_bps_max",
            "the max bytes per second of sinker",
            TaskMetricsType::SinkerBpsMax,
        )?;
        register_handler(
            "sinker_bps_min",
            "the min bytes per second of sinker",
            TaskMetricsType::SinkerBpsMin,
        )?;
        register_handler(
            "sinker_bps_avg",
            "the average bytes per second of sinker",
            TaskMetricsType::SinkerBpsAvg,
        )?;
        register_handler(
            "sinker_workers_configured",
            "the number of configured sinker workers",
            TaskMetricsType::SinkerWorkersConfigured,
        )?;
        register_handler(
            "sinker_workers_busy",
            "the number of sinker workers currently executing sinker operations",
            TaskMetricsType::SinkerWorkersBusy,
        )?;
        register_handler(
            "sinker_workers_per_drain_max",
            "the max distinct sinker workers receiving non-empty data per pipeline drain",
            TaskMetricsType::SinkerWorkersPerDrainMax,
        )?;
        register_handler(
            "sinker_workers_per_drain_avg",
            "the average distinct sinker workers receiving non-empty data per pipeline drain",
            TaskMetricsType::SinkerWorkersPerDrainAvg,
        )?;
        register_handler(
            "sinker_sinked_records",
            "the number of records sinked",
            TaskMetricsType::SinkerSinkedRecords,
        )?;
        register_handler(
            "sinker_sinked_bytes",
            "the bytes of records sinked",
            TaskMetricsType::SinkerSinkedBytes,
        )?;
        register_handler(
            "checker_miss_total",
            "the total miss count detected by checker",
            TaskMetricsType::CheckerMissCount,
        )?;
        register_handler(
            "checker_diff_total",
            "the total diff count detected by checker",
            TaskMetricsType::CheckerDiffCount,
        )?;
        register_handler(
            "checker_queue_size",
            "the unresolved rows currently tracked by checker",
            TaskMetricsType::CheckerPending,
        )?;
        register_handler(
            "checker_rps_min",
            "the min checked records per second of checker",
            TaskMetricsType::CheckerRpsMin,
        )?;
        register_handler(
            "checker_rps_max",
            "the max checked records per second of checker",
            TaskMetricsType::CheckerRpsMax,
        )?;
        register_handler(
            "checker_rps_avg",
            "the average checked records per second of checker",
            TaskMetricsType::CheckerRpsAvg,
        )?;
        register_handler(
            "checker_miss_rps_min",
            "the min miss records per second of checker",
            TaskMetricsType::CheckerMissRpsMin,
        )?;
        register_handler(
            "checker_miss_rps_max",
            "the max miss records per second of checker",
            TaskMetricsType::CheckerMissRpsMax,
        )?;
        register_handler(
            "checker_miss_rps_avg",
            "the average miss records per second of checker",
            TaskMetricsType::CheckerMissRpsAvg,
        )?;
        register_handler(
            "checker_diff_rps_min",
            "the min diff records per second of checker",
            TaskMetricsType::CheckerDiffRpsMin,
        )?;
        register_handler(
            "checker_diff_rps_max",
            "the max diff records per second of checker",
            TaskMetricsType::CheckerDiffRpsMax,
        )?;
        register_handler(
            "checker_diff_rps_avg",
            "the average diff records per second of checker",
            TaskMetricsType::CheckerDiffRpsAvg,
        )?;

        register_handler(
            "pipeline_sink_parallel_utilization_latest",
            "latest successful pipeline sink operation utilization in the current time window (0 to 1)",
            TaskMetricsType::PipelineSinkParallelUtilizationLatest,
        )?;
        register_handler(
            "pipeline_sink_parallel_utilization_avg",
            "mean utilization per successful pipeline sink operation in the current time window (0 to 1)",
            TaskMetricsType::PipelineSinkParallelUtilizationAvg,
        )?;
        register_handler(
            "pipeline_sink_parallel_utilization_min",
            "minimum successful pipeline sink operation utilization in the current time window (0 to 1)",
            TaskMetricsType::PipelineSinkParallelUtilizationMin,
        )?;
        register_handler(
            "pipeline_sink_parallel_utilization_max",
            "maximum successful pipeline sink operation utilization in the current time window (0 to 1)",
            TaskMetricsType::PipelineSinkParallelUtilizationMax,
        )?;
        register_handler(
            "pipeline_sink_duration_seconds_latest",
            "latest successful pipeline sink operation wall-clock duration in the current time window, in seconds",
            TaskMetricsType::PipelineSinkDurationSecondsLatest,
        )?;
        register_handler(
            "pipeline_sink_duration_seconds_avg",
            "mean successful pipeline sink operation wall-clock duration in the current time window, in seconds",
            TaskMetricsType::PipelineSinkDurationSecondsAvg,
        )?;
        register_handler(
            "pipeline_sink_duration_seconds_min",
            "minimum successful pipeline sink operation wall-clock duration in the current time window, in seconds",
            TaskMetricsType::PipelineSinkDurationSecondsMin,
        )?;
        register_handler(
            "pipeline_sink_duration_seconds_max",
            "maximum successful pipeline sink operation wall-clock duration in the current time window, in seconds",
            TaskMetricsType::PipelineSinkDurationSecondsMax,
        )?;
        register_handler(
            "pipeline_sink_operations_total",
            "number of successful pipeline sink operations since task start",
            TaskMetricsType::PipelineSinkOperationsTotal,
        )?;

        if let Some(task_type) = &self.task_type {
            match task_type.kind {
                TaskKind::Snapshot => {
                    register_handler(
                        "partitioner_duration_seconds_latest",
                        "latest chunk partition call duration in the current time window, in seconds",
                        TaskMetricsType::PartitionerDurationSecondsLatest,
                    )?;
                    register_handler(
                        "partitioner_duration_seconds_avg",
                        "mean chunk partition call duration in the current time window, in seconds",
                        TaskMetricsType::PartitionerDurationSecondsAvg,
                    )?;
                    register_handler(
                        "partitioner_duration_seconds_min",
                        "minimum chunk partition call duration in the current time window, in seconds",
                        TaskMetricsType::PartitionerDurationSecondsMin,
                    )?;
                    register_handler(
                        "partitioner_duration_seconds_max",
                        "maximum chunk partition call duration in the current time window, in seconds",
                        TaskMetricsType::PartitionerDurationSecondsMax,
                    )?;
                    register_handler(
                        "extractor_plan_records",
                        "the records estimated by extractor plan",
                        TaskMetricsType::ExtractorPlanRecords,
                    )?;
                    register_handler(
                        "progress",
                        "the progress of task",
                        TaskMetricsType::Progress,
                    )?;
                }
                TaskKind::Cdc => {
                    register_handler(
                        "timestamp",
                        "the timestamp of task",
                        TaskMetricsType::Timestamp,
                    )?;
                    register_handler(
                        "sinker_ddl_count",
                        "the count of DDL operations",
                        TaskMetricsType::SinkerDdlCount,
                    )?;
                }
                TaskKind::Struct => {}
            }
        }
        Ok(self)
    }

    pub fn set_metrics(&self, metrics: &BTreeMap<TaskMetricsType, TaskMetricValue>) {
        for (metrics_type, value) in metrics.iter() {
            if let Some(metric) = self.metrics.get_mut(metrics_type) {
                metric.set(value.as_f64());
            }
        }
    }

    pub async fn start_metrics(&self) -> tokio::task::JoinHandle<Result<(), std::io::Error>> {
        let registry = self.registry.clone();
        let addr = format!("{}:{}", self.config.http_host, self.config.http_port);
        let server = HttpServer::new(move || {
            App::new()
                .wrap(Logger::default())
                .app_data(web::Data::new(registry.clone()))
                .service(web::resource("/metrics").route(web::get().to(metrics_handler)))
                .service(web::resource("/healthz").route(web::get().to(healthz_handler)))
                .default_service(web::route().to(not_found_handler))
        })
        .workers(self.config.workers as usize)
        .shutdown_timeout(10);

        match server.bind(&addr) {
            Ok(server) => tokio::spawn(server.run()),
            Err(err) => {
                log::warn!(
                    "Failed to bind metrics server on {} (metrics disabled): {}",
                    addr,
                    err
                );
                tokio::spawn(async move { Err(err) })
            }
        }
    }
}

async fn metrics_handler(registry: web::Data<Arc<Registry>>) -> impl Responder {
    let mut buffer = String::new();
    let encoder = TextEncoder::new();

    match encoder.encode_utf8(&registry.gather(), &mut buffer) {
        Ok(_) => HttpResponse::Ok()
            .content_type("text/plain; charset=utf-8; version=0.0.4")
            .body(buffer),
        Err(e) => {
            log::error!("Failed to encode metrics: {}", e);
            HttpResponse::InternalServerError().body("Failed to encode metrics")
        }
    }
}

async fn healthz_handler() -> Result<impl Responder> {
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(r#"{"status":"ok","service":"ape-dts"}"#))
}

async fn not_found_handler() -> Result<impl Responder> {
    Ok(HttpResponse::NotFound()
        .content_type("application/json")
        .body(r#"{"error":"Not Found","message":"The requested endpoint does not exist"}"#))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use prometheus::TextEncoder;

    use super::PrometheusMetrics;
    use crate::{config::metrics_config::MetricsConfig, monitor::task_metrics::TaskMetricsType};

    #[test]
    fn exports_sinker_worker_metrics_with_public_units() -> anyhow::Result<()> {
        let prometheus = PrometheusMetrics::new(
            None,
            MetricsConfig {
                http_host: "127.0.0.1".to_owned(),
                http_port: 0,
                workers: 1,
                metrics_labels: HashMap::new(),
            },
        );
        prometheus.initialization()?;

        let metrics = BTreeMap::from([
            (TaskMetricsType::SinkerWorkersConfigured, 10),
            (TaskMetricsType::SinkerWorkersBusy, 4),
            (TaskMetricsType::SinkerWorkersPerDrainMax, 8),
            (TaskMetricsType::SinkerWorkersPerDrainAvg, 6),
        ]);
        let metrics = metrics
            .into_iter()
            .map(|(key, value)| {
                (
                    key,
                    crate::monitor::task_metrics::TaskMetricValue::Integer(value),
                )
            })
            .collect();
        prometheus.set_metrics(&metrics);

        let mut output = String::new();
        TextEncoder::new()
            .encode_utf8(&prometheus.registry.gather(), &mut output)
            .unwrap();

        assert!(output.contains("sinker_workers_configured 10"));
        assert!(output.contains("sinker_workers_busy 4"));
        assert!(output.contains("sinker_workers_per_drain_max 8"));
        assert!(output.contains("sinker_workers_per_drain_avg 6"));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn exports_window_metrics_and_preserves_total_through_final_flush() {
        use std::{sync::Arc, time::Duration};

        use crate::{
            config::config_enums::{TaskKind, TaskType},
            monitor::{
                task_monitor::{MonitorType, TaskMonitor},
                task_monitor_handle::TaskMonitorHandle,
                FlushableMonitor,
            },
        };
        let task_type = TaskType::new(TaskKind::Snapshot, None);
        let prometheus = Arc::new(PrometheusMetrics::new(
            Some(task_type),
            MetricsConfig {
                http_host: "127.0.0.1".into(),
                http_port: 0,
                workers: 1,
                metrics_labels: HashMap::new(),
            },
        ));
        prometheus.initialization().unwrap();
        let task = Arc::new(TaskMonitor::new(Some(task_type), prometheus.clone()));
        let handle = TaskMonitorHandle::new(
            task.clone(),
            MonitorType::Pipeline,
            "pipeline".into(),
            1,
            100,
            10,
        );
        handle.register_monitor("pipeline", handle.build_monitor("pipeline", "pipeline"));
        let partition = handle.partitioner_monitor().unwrap();
        for micros in [100, 200, 900] {
            partition
                .add_counter(
                    crate::monitor::counter_type::CounterType::PartitionerDurationSeconds,
                    Duration::from_micros(micros).as_secs_f64(),
                )
                .await;
        }
        let metrics = handle.pipeline_sink_monitor().unwrap();
        let batch = task.sinker_worker_metrics().start_pipeline_sink(4);
        batch.record_work(Duration::from_millis(130));
        tokio::time::advance(Duration::from_millis(100)).await;
        TaskMonitorHandle::record_pipeline_sink_metrics(&metrics, batch).await;
        task.add_no_window_metrics(TaskMetricsType::SinkerSinkedRecords, 123);
        task.flush().await;
        let mut output = String::new();
        TextEncoder::new()
            .encode_utf8(&prometheus.registry.gather(), &mut output)
            .unwrap();
        assert!(output.contains("pipeline_sink_parallel_utilization_latest 0.325\n"));
        assert!(output.contains("pipeline_sink_parallel_utilization_avg 0.325\n"));
        assert!(output.contains("pipeline_sink_parallel_utilization_min 0.325\n"));
        assert!(output.contains("pipeline_sink_parallel_utilization_max 0.325\n"));
        for suffix in ["latest", "avg", "min", "max"] {
            assert!(output.contains(&format!("pipeline_sink_duration_seconds_{suffix} 0.1\n")));
        }
        assert!(output.contains("pipeline_sink_operations_total 1\n"));
        for (suffix, expected) in [
            ("latest", 0.0009),
            ("avg", 0.0004),
            ("min", 0.0001),
            ("max", 0.0009),
        ] {
            let prefix = format!("partitioner_duration_seconds_{suffix} ");
            let actual = output
                .lines()
                .find_map(|line| line.strip_prefix(&prefix))
                .unwrap()
                .parse::<f64>()
                .unwrap();
            assert!((actual - expected).abs() < 1e-12);
        }
        assert!(output.contains("sinker_sinked_records 123\n"));
        let counters: Vec<_> = metrics
            .time_window_counters
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        for counter in counters {
            for sample in counter.counters.write().await.iter_mut() {
                sample.timestamp = std::time::Instant::now() - Duration::from_secs(2);
            }
        }
        task.flush().await;
        let mut after_flush = String::new();
        TextEncoder::new()
            .encode_utf8(&prometheus.registry.gather(), &mut after_flush)
            .unwrap();
        for prefix in [
            "pipeline_sink_parallel_utilization",
            "pipeline_sink_duration_seconds",
            "partitioner_duration_seconds",
        ] {
            for suffix in ["latest", "avg", "min", "max"] {
                assert!(
                    after_flush.contains(&format!("{prefix}_{suffix} 0\n")),
                    "{after_flush}"
                );
            }
        }
        assert!(after_flush.contains("pipeline_sink_operations_total 1\n"));
        assert!(after_flush.contains("sinker_sinked_records 123\n"));
        task.unregister("pipeline", vec![MonitorType::Pipeline]);
    }

    #[cfg(feature = "tracing")]
    #[test]
    fn validates_runtime_trace_labels_during_initialization() {
        for label in ["bad-label", "marker", "wait_point"] {
            let prometheus = PrometheusMetrics::new(
                None,
                MetricsConfig {
                    http_host: "127.0.0.1".to_owned(),
                    http_port: 0,
                    workers: 1,
                    metrics_labels: HashMap::from([(label.to_owned(), "value".to_owned())]),
                },
            );

            assert!(prometheus.initialization().is_err(), "label: {label}");
        }
    }
}
