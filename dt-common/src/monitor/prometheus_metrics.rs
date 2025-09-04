#[cfg(feature = "metrics")]
use std::{collections::BTreeMap, sync::Arc};

use actix_web::{middleware::Logger, web, App, HttpResponse, HttpServer, Responder, Result};
use dashmap::DashMap;
use prometheus::{Gauge, Opts, Registry, TextEncoder};

use crate::config::config_enums::TaskType;
use crate::config::metrics_config::MetricsConfig;
use crate::monitor::task_metrics::TaskMetricsType;

pub struct PrometheusMetrics {
    registry: Arc<Registry>,
    metrics: DashMap<TaskMetricsType, Gauge>,
    task_type: Option<TaskType>,
    config: MetricsConfig,
}

impl PrometheusMetrics {
    pub fn new(task_type: Option<TaskType>, config: MetricsConfig) -> Self {
        Self {
            registry: Arc::new(Registry::new()),
            metrics: DashMap::new(),
            task_type,
            config,
        }
    }

    pub fn initialization(&self) -> &Self {
        let register_handler =
            |metrics_name: &str, metrics_desc: &str, metrics_type: TaskMetricsType| {
                let metrics = Gauge::with_opts(
                    Opts::new(metrics_name, metrics_desc)
                        .const_labels(self.config.metrics_labels.to_owned()),
                )
                .unwrap();

                self.registry.register(Box::new(metrics.clone())).unwrap();
                self.metrics.insert(metrics_type, metrics);
            };

        // TODO: support these metrics:
        // register_handler(
        //     "extractor_rps_max",
        //     "the max records per second of extractor",
        //     TaskMetricsType::ExtractorRpsMax,
        // );
        // register_handler(
        //     "extractor_rps_min",
        //     "the min records per second of extractor",
        //     TaskMetricsType::ExtractorRpsMin,
        // );
        // register_handler(
        //     "extractor_rps_avg",
        //     "the average records per second of extractor",
        //     TaskMetricsType::ExtractorRpsAvg,
        // );
        // register_handler(
        //     "extractor_bps_max",
        //     "the max bytes per second of extractor",
        //     TaskMetricsType::ExtractorBpsMax,
        // );
        // register_handler(
        //     "extractor_bps_min",
        //     "the min bytes per second of extractor",
        //     TaskMetricsType::ExtractorBpsMin,
        // );
        // register_handler(
        //     "extractor_bps_avg",
        //     "the average bytes per second of extractor",
        //     TaskMetricsType::ExtractorBpsAvg,
        // );

        register_handler(
            "extractor_pushed_rps_max",
            "the max pushed records per second of extractor",
            TaskMetricsType::ExtractorPushedRpsMax,
        );
        register_handler(
            "extractor_pushed_rps_min",
            "the min pushed records per second of extractor",
            TaskMetricsType::ExtractorPushedRpsMin,
        );
        register_handler(
            "extractor_pushed_rps_avg",
            "the average pushed records per second of extractor",
            TaskMetricsType::ExtractorPushedRpsAvg,
        );
        register_handler(
            "extractor_pushed_bps_max",
            "the max pushed bytes per second of extractor",
            TaskMetricsType::ExtractorPushedBpsMax,
        );
        register_handler(
            "extractor_pushed_bps_min",
            "the min pushed bytes per second of extractor",
            TaskMetricsType::ExtractorPushedBpsMin,
        );
        register_handler(
            "extractor_pushed_bps_avg",
            "the average pushed bytes per second of extractor",
            TaskMetricsType::ExtractorPushedBpsAvg,
        );

        register_handler(
            "pipeline_queue_size",
            "the records size of pipeline queue",
            TaskMetricsType::PipelineQueueSize,
        );
        register_handler(
            "pipeline_queue_bytes",
            "the bytes in pipeline queue",
            TaskMetricsType::PipelineQueueBytes,
        );

        register_handler(
            "sinker_rt_max",
            "the max response time of sinker, the unit is millisecond",
            TaskMetricsType::SinkerRtMax,
        );
        register_handler(
            "sinker_rt_min",
            "the min response time of sinker, the unit is millisecond",
            TaskMetricsType::SinkerRtMin,
        );
        register_handler(
            "sinker_rt_avg",
            "the average response time of sinker, the unit is millisecond",
            TaskMetricsType::SinkerRtAvg,
        );

        register_handler(
            "sinker_rps_max",
            "the max records per second of sinker",
            TaskMetricsType::SinkerRpsMax,
        );
        register_handler(
            "sinker_rps_min",
            "the min records per second of sinker",
            TaskMetricsType::SinkerRpsMin,
        );
        register_handler(
            "sinker_rps_avg",
            "the average records per second of sinker",
            TaskMetricsType::SinkerRpsAvg,
        );
        register_handler(
            "sinker_bps_max",
            "the max bytes per second of sinker",
            TaskMetricsType::SinkerBpsMax,
        );
        register_handler(
            "sinker_bps_min",
            "the min bytes per second of sinker",
            TaskMetricsType::SinkerBpsMin,
        );
        register_handler(
            "sinker_bps_avg",
            "the average bytes per second of sinker",
            TaskMetricsType::SinkerBpsAvg,
        );

        register_handler(
            "sinker_sinked_records",
            "the number of records sinked",
            TaskMetricsType::SinkerSinkedRecords,
        );
        register_handler(
            "sinker_sinked_bytes",
            "the bytes of records sinked",
            TaskMetricsType::SinkerSinkedBytes,
        );

        if let Some(task_type) = &self.task_type {
            match task_type {
                TaskType::Snapshot => {
                    register_handler(
                        "extractor_plan_records",
                        "the records estimated by extractor plan",
                        TaskMetricsType::ExtractorPlanRecords,
                    );
                }
                TaskType::Cdc => {
                    register_handler(
                        "timestamp",
                        "the timestamp of task",
                        TaskMetricsType::Timestamp,
                    );
                    register_handler(
                        "sinker_ddl_count",
                        "the count of DDL operations",
                        TaskMetricsType::SinkerDdlCount,
                    );
                }
                TaskType::Struct | TaskType::Check => {}
            }
        }
        self
    }

    pub fn set_metrics(&self, metrics: &BTreeMap<TaskMetricsType, u64>) {
        for (metrics_type, value) in metrics.iter() {
            if let Some(metrics) = self.metrics.get_mut(metrics_type) {
                metrics.set(*value as f64);
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
        .shutdown_timeout(10)
        .bind(&addr)
        .unwrap()
        .run();

        tokio::spawn(server)
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
