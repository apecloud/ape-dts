use std::{
    collections::HashMap,
    panic,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{bail, Context};
use chrono::Local;
use log4rs::config::{Config, Deserializers, RawConfig};
use tokio::{
    fs::{metadata, File},
    io::AsyncReadExt,
    runtime::Handle,
    sync::{Mutex, RwLock},
    task::JoinSet,
};

use super::{
    extractor_util::ExtractorUtil, parallelizer_util::ParallelizerUtil, sinker_util::SinkerUtil,
};
use crate::{
    create_router,
    task_util::{ConnClient, TaskUtil},
};
use async_mutex::Mutex as AsyncMutex;
use std::sync::Mutex as StdMutex;

static LOG_HANDLE: StdMutex<Option<log4rs::Handle>> = StdMutex::new(None);
use dt_common::log_filter::{parse_size_limit, SizeLimitFilterDeserializer};
use dt_common::{
    config::{
        checker_config::CheckerConfig,
        config_enums::{DbType, ExtractType, PipelineType, SinkType, TaskKind, TaskType},
        config_token_parser::{ConfigTokenParser, TokenEscapePair},
        extractor_config::ExtractorConfig,
        sinker_config::SinkerConfig,
        task_config::{TaskConfig, DEFAULT_CHECK_LOG_FILE_SIZE},
    },
    error::Error,
    limiter::buffer_limiter::BufferLimiter,
    log_error, log_finished, log_info, log_warn,
    meta::{
        avro::avro_converter::AvroConverter, dt_ctl_queue::DtCtlQueue, dt_queue::DtQueue,
        position::Position, row_type::RowType, syncer::Syncer,
    },
    monitor::{
        monitor::Monitor,
        task_metrics::TaskMetricsType,
        task_monitor::{MonitorType, TaskMonitor, TaskMonitorHandle},
        FlushableMonitor,
    },
    rdb_filter::RdbFilter,
    utils::sql_util::SqlUtil,
};
use dt_connector::{
    checker::base_checker::CheckContext,
    checker::check_log::CheckSummaryLog,
    checker::{
        Checker, CheckerHandle, CheckerStateStore, DataCheckerHandle, MongoChecker, MysqlChecker,
        PgChecker, StructCheckerHandle,
    },
    data_marker::DataMarker,
    extractor::resumer::{recorder::Recorder, recovery::Recovery},
    rdb_router::RdbRouter,
    sinker::base_sinker::BaseSinker,
    Extractor, Sinker,
};
use dt_pipeline::{
    base_pipeline::BasePipeline, http_server_pipeline::HttpServerPipeline,
    lua_processor::LuaProcessor, Pipeline,
};

#[cfg(feature = "metrics")]
use dt_common::monitor::prometheus_metrics::PrometheusMetrics;

#[derive(Clone)]
pub struct TaskInfo {
    pub extractor_config: ExtractorConfig,
    pub no_data: bool,
}

#[derive(Clone)]
pub struct TaskRunner {
    task_type: Option<TaskType>,
    config: TaskConfig,
    filter: RdbFilter,
    task_monitor: Arc<TaskMonitor>,
    #[cfg(feature = "metrics")]
    prometheus_metrics: Arc<PrometheusMetrics>,
}

const CHECK_LOG_DIR_PLACEHOLDER: &str = "CHECK_LOG_DIR_PLACEHOLDER";
const STATISTIC_LOG_DIR_PLACEHOLDER: &str = "STATISTIC_LOG_DIR_PLACEHOLDER";
const LOG_LEVEL_PLACEHOLDER: &str = "LOG_LEVEL_PLACEHOLDER";
const LOG_DIR_PLACEHOLDER: &str = "LOG_DIR_PLACEHOLDER";
const CHECK_LOG_FILE_SIZE_PLACEHOLDER: &str = "CHECK_LOG_FILE_SIZE_PLACEHOLDER";
const DEFAULT_CHECK_LOG_DIR_PLACEHOLDER: &str = "LOG_DIR_PLACEHOLDER/check";
const DEFAULT_STATISTIC_LOG_DIR_PLACEHOLDER: &str = "LOG_DIR_PLACEHOLDER/statistic";

fn init_task_check_summary() -> CheckSummaryLog {
    CheckSummaryLog {
        start_time: Local::now().to_rfc3339(),
        is_consistent: true,
        ..Default::default()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SingleTaskWorker {
    Extractor,
    Pipeline,
    Monitor,
}

impl SingleTaskWorker {
    fn as_str(self) -> &'static str {
        match self {
            Self::Extractor => "extractor",
            Self::Pipeline => "pipeline",
            Self::Monitor => "monitor",
        }
    }
}

impl TaskRunner {
    pub fn new(task_config_file: &str) -> anyhow::Result<Self> {
        let config = TaskConfig::new(task_config_file)
            .with_context(|| format!("invalid configs in [{}]", task_config_file))?;
        let task_type = config.task_type();
        #[cfg(not(feature = "metrics"))]
        let task_monitor = Arc::new(TaskMonitor::new(task_type));

        #[cfg(feature = "metrics")]
        let prometheus_metrics =
            Arc::new(PrometheusMetrics::new(task_type, config.metrics.clone()));

        #[cfg(feature = "metrics")]
        let task_monitor = Arc::new(TaskMonitor::new(task_type, prometheus_metrics.clone()));

        Ok(Self {
            filter: RdbFilter::from_config(&config.filter, &config.extractor_basic.db_type)?,
            config,
            task_monitor,
            #[cfg(feature = "metrics")]
            prometheus_metrics,
            task_type,
        })
    }

    pub async fn start_task(&self) -> anyhow::Result<()> {
        self.init_log4rs().await?;

        let worker_thread_cnt = Handle::current().metrics().num_workers();
        log_info!(
            "ape-dts started with {} worker thread(s)",
            worker_thread_cnt
        );

        panic::set_hook(Box::new(|panic_info| {
            let backtrace = std::backtrace::Backtrace::capture();
            log_error!("panic: {}\nbacktrace:\n{}", panic_info, backtrace);
        }));

        log_info!(
            "start task: [taskID: {}, taskType: {:?}]",
            &self.config.global.task_id,
            &self.task_type
        );

        let db_type = &self.config.extractor_basic.db_type;
        let router = Arc::new(RdbRouter::from_config(&self.config.router, db_type)?);
        let (recorder, recovery, checker_state_store) = match &self.task_type {
            Some(task_type) => {
                TaskUtil::build_resumer(
                    task_type.to_owned(),
                    &self.config.global,
                    &self.config.resumer,
                )
                .await?
            }
            None => (None, None, None),
        };
        if self
            .task_type
            .is_some_and(|task_type| task_type.is_cdc_inline_check())
            && checker_state_store.is_none()
        {
            bail!(Error::ConfigError(
                "config [checker] with CDC tasks requires [resumer] resume_type=from_target or from_db to persist checker state"
                    .into(),
            ));
        }
        let (extractor_client, sinker_client) = ConnClient::from_config(&self.config).await?;

        let check_summary = self
            .config
            .checker
            .as_ref()
            .map(|_| Arc::new(AsyncMutex::new(init_task_check_summary())));

        #[cfg(feature = "metrics")]
        self.prometheus_metrics
            .initialization()
            .start_metrics()
            .await;

        let task_info = self
            .get_task_info(extractor_client.clone(), recovery.clone())
            .await?;
        if !task_info.no_data {
            self.clone()
                .create_task(
                    task_info.extractor_config,
                    extractor_client.clone(),
                    sinker_client.clone(),
                    router,
                    recorder,
                    recovery,
                    check_summary.clone(),
                    checker_state_store.clone(),
                )
                .await?;
        }

        // close connections
        extractor_client.close().await?;
        sinker_client.close().await?;

        if let Some(check_summary) = check_summary {
            if self.config.checker.is_none()
                || !self
                    .task_type
                    .as_ref()
                    .is_some_and(|task_type| task_type.is_cdc_inline_check())
            {
                let mut summary = check_summary.lock().await;
                summary.end_time = Local::now().to_rfc3339();
                dt_common::log_summary!("{}", summary);
            }
        }

        log_finished!("task finished");
        log::logger().flush();
        Ok(())
    }

    async fn create_task(
        self,
        extractor_config: ExtractorConfig,
        extractor_client: ConnClient,
        sinker_client: ConnClient,
        router: Arc<RdbRouter>,
        recorder: Option<Arc<dyn Recorder + Send + Sync>>,
        recovery: Option<Arc<dyn Recovery + Send + Sync>>,
        check_summary: Option<Arc<AsyncMutex<CheckSummaryLog>>>,
        checker_state_store: Option<Arc<CheckerStateStore>>,
    ) -> anyhow::Result<()> {
        let enqueue_limiter = BufferLimiter::from_config(
            Some(&self.config.extractor_basic.rate_limiter),
            Some(&self.config.pipeline.capacity_limiter),
        )
        .map(Arc::new);
        let dequeue_limiter =
            BufferLimiter::from_config(Some(&self.config.sinker_basic.rate_limiter), None)
                .map(Arc::new);
        let max_bytes = self.config.pipeline.capacity_limiter.buffer_memory_mb * 1024 * 1024;
        let buffer = Arc::new(DtQueue::new(
            self.config.pipeline.capacity_limiter.buffer_size,
            max_bytes as u64,
            enqueue_limiter,
            dequeue_limiter,
        ));
        let ctl_buffer = Arc::new(DtCtlQueue::new());

        let shut_down = Arc::new(AtomicBool::new(false));
        let syncer = Arc::new(Mutex::new(Syncer {
            received_position: Position::None,
            committed_position: Position::None,
        }));

        let (extractor_data_marker, sinker_data_marker) = if let Some(data_marker_config) =
            &self.config.data_marker
        {
            let sinker_db_type = self
                .config
                .destination_target()
                .map(|target| target.db_type)
                .unwrap_or(self.config.sinker_basic.db_type.clone());
            let extractor_data_marker =
                DataMarker::from_config(data_marker_config, &self.config.extractor_basic.db_type)?;
            let sinker_data_marker = DataMarker::from_config(data_marker_config, &sinker_db_type)?;
            (Some(extractor_data_marker), Some(sinker_data_marker))
        } else {
            (None, None)
        };
        let rw_sinker_data_marker = sinker_data_marker
            .clone()
            .map(|data_marker| Arc::new(RwLock::new(data_marker)));
        let single_task_id = self.config.global.task_id.clone();
        let is_snapshot_task = self
            .task_type
            .as_ref()
            .is_some_and(|task_type| task_type.kind == TaskKind::Snapshot);

        let monitor_time_window_secs = self.config.pipeline.counter_time_window_secs;
        let monitor_max_sub_count = self.config.pipeline.counter_max_sub_count;
        let monitor_count_window = self.config.pipeline.capacity_limiter.buffer_size as u64;
        let build_monitor = |name| {
            Arc::new(Monitor::new(
                name,
                &single_task_id,
                monitor_time_window_secs,
                monitor_max_sub_count,
                monitor_count_window,
            ))
        };

        let extractor_monitor = build_monitor("extractor");
        let extractor_monitor_handle = self.task_monitor.handle(
            MonitorType::Extractor,
            monitor_time_window_secs,
            monitor_max_sub_count,
            monitor_count_window,
        );
        let extractor = ExtractorUtil::create_extractor(
            &self.config,
            &extractor_config,
            extractor_client.clone(),
            buffer.clone(),
            ctl_buffer.clone(),
            shut_down.clone(),
            syncer.clone(),
            extractor_monitor_handle,
            single_task_id.clone(),
            extractor_data_marker,
            (*router).clone(),
            recovery.clone(),
        )
        .await?;
        let extractor = Arc::new(Mutex::new(extractor));

        let checker_monitor = build_monitor("checker");
        let checker_monitor_handle = self.task_monitor.handle(
            MonitorType::Checker,
            monitor_time_window_secs,
            monitor_max_sub_count,
            monitor_count_window,
        );
        let checker = self
            .create_checker(
                self.config.checker.as_ref(),
                &single_task_id,
                checker_monitor_handle,
                check_summary.clone(),
                recovery.as_ref(),
                checker_state_store.clone(),
            )
            .await?;

        let sinker_monitor = build_monitor("sinker");
        let sinker_monitor_handle = self.task_monitor.handle(
            MonitorType::Sinker,
            monitor_time_window_secs,
            monitor_max_sub_count,
            monitor_count_window,
        );
        let sinkers = SinkerUtil::create_sinkers(
            &self.config,
            &extractor_config,
            sinker_client.clone(),
            sinker_monitor_handle,
            single_task_id.clone(),
            rw_sinker_data_marker.clone(),
            checker.as_ref().and_then(|handle| match handle {
                CheckerHandle::Data(handle) => Some(handle.clone()),
                CheckerHandle::Struct(_) => None,
            }),
        )
        .await?;

        let pipeline_monitor_handle = self.task_monitor.handle(
            MonitorType::Pipeline,
            monitor_time_window_secs,
            monitor_max_sub_count,
            monitor_count_window,
        );
        let pipeline = self
            .create_pipeline(
                buffer,
                shut_down.clone(),
                syncer,
                sinkers,
                pipeline_monitor_handle.clone(),
                self.config.global.task_id.clone(), // pipline metrics are shared
                ctl_buffer.clone(),
                rw_sinker_data_marker.clone(),
                recorder.clone(),
                checker,
            )
            .await?;
        let pipeline = Arc::new(Mutex::new(pipeline));

        let mut monitors = vec![(
            MonitorType::Pipeline,
            pipeline_monitor_handle.build_monitor("pipeline", &self.config.global.task_id),
        )];
        if !is_snapshot_task {
            monitors.push((MonitorType::Extractor, extractor_monitor.clone()));
            monitors.push((MonitorType::Sinker, sinker_monitor.clone()));
            monitors.push((MonitorType::Checker, checker_monitor.clone()));
        }
        self.task_monitor.register(&single_task_id, monitors);

        // do pre operations before task starts
        self.create_task_tables(
            extractor_client.clone(),
            sinker_client.clone(),
            sinker_data_marker,
        )
        .await?;

        let interval_secs = self.config.pipeline.checkpoint_interval_secs;
        let single_task_flush_monitors: Vec<Arc<dyn FlushableMonitor + Send + Sync>> =
            vec![self.task_monitor.clone()];
        let monitor_shut_down = shut_down.clone();
        let monitor_task = tokio::spawn(async move {
            TaskUtil::flush_monitors(
                interval_secs,
                monitor_shut_down,
                &single_task_flush_monitors,
            )
            .await;
            Ok(())
        });

        let worker_result = Self::run_single_task_workers(
            extractor.clone(),
            pipeline.clone(),
            monitor_task,
            shut_down.clone(),
        )
        .await;

        let mut monitor_types = vec![MonitorType::Pipeline];
        if !is_snapshot_task {
            monitor_types.push(MonitorType::Extractor);
            monitor_types.push(MonitorType::Sinker);
            monitor_types.push(MonitorType::Checker);
        }
        self.task_monitor.unregister(&single_task_id, monitor_types);
        worker_result
    }

    async fn run_single_task_workers(
        extractor: Arc<Mutex<Box<dyn Extractor + Send>>>,
        pipeline: Arc<Mutex<Box<dyn Pipeline + Send>>>,
        monitor_task: tokio::task::JoinHandle<anyhow::Result<()>>,
        shut_down: Arc<AtomicBool>,
    ) -> anyhow::Result<()> {
        let mut join_set = JoinSet::new();

        let extractor_worker = extractor.clone();
        join_set.spawn(async move {
            (
                SingleTaskWorker::Extractor,
                Self::run_extractor_worker(extractor_worker).await,
            )
        });

        let pipeline_worker = pipeline.clone();
        join_set.spawn(async move {
            (
                SingleTaskWorker::Pipeline,
                Self::run_pipeline_worker(pipeline_worker).await,
            )
        });

        join_set.spawn(async move {
            (
                SingleTaskWorker::Monitor,
                monitor_task
                    .await
                    .context("monitor task join error")
                    .and_then(|result| result),
            )
        });

        let mut extractor_done = false;
        let mut pipeline_done = false;
        let mut failure = None;
        let mut mark_done = |kind| match kind {
            SingleTaskWorker::Extractor => extractor_done = true,
            SingleTaskWorker::Pipeline => pipeline_done = true,
            SingleTaskWorker::Monitor => {}
        };

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((kind, Ok(()))) => mark_done(kind),
                Ok((kind, Err(err))) => {
                    failure = Some((Some(kind), err));
                    break;
                }
                Err(err) => {
                    failure = Some((
                        None,
                        anyhow::anyhow!("single task worker join error: {}", err),
                    ));
                    break;
                }
            }
        }

        if let Some((failed_worker, err)) = failure {
            shut_down.store(true, Ordering::Release);
            join_set.abort_all();

            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok((kind, Ok(()))) => mark_done(kind),
                    Ok((kind, Err(shutdown_err))) => {
                        log_error!(
                            "single task worker [{}] also failed during shutdown, error: {:#}",
                            kind.as_str(),
                            shutdown_err
                        );
                    }
                    Err(join_err) if !join_err.is_cancelled() => {
                        log_error!(
                            "single task worker join error during shutdown: {}",
                            join_err
                        );
                    }
                    Err(_) => {}
                }
            }

            if !extractor_done && failed_worker != Some(SingleTaskWorker::Extractor) {
                if let Err(clean_err) = Self::close_extractor_after_abort(extractor).await {
                    log_error!(
                        "failed to close extractor after task error: {:#}",
                        clean_err
                    );
                }
            }
            if !pipeline_done && failed_worker != Some(SingleTaskWorker::Pipeline) {
                if let Err(clean_err) = Self::stop_pipeline_after_abort(pipeline).await {
                    log_error!("failed to stop pipeline after task error: {:#}", clean_err);
                }
            }

            return Err(err);
        }

        Ok(())
    }

    async fn run_extractor_worker(
        extractor: Arc<Mutex<Box<dyn Extractor + Send>>>,
    ) -> anyhow::Result<()> {
        let extract_result = {
            let mut extractor = extractor.lock().await;
            extractor.extract().await
        };
        let close_result = {
            let mut extractor = extractor.lock().await;
            extractor.close().await
        };

        extract_result.context("extractor.extract failed")?;
        close_result.context("extractor.close failed")?;
        Ok(())
    }

    async fn run_pipeline_worker(
        pipeline: Arc<Mutex<Box<dyn Pipeline + Send>>>,
    ) -> anyhow::Result<()> {
        let start_result = {
            let mut pipeline = pipeline.lock().await;
            pipeline.start().await
        };
        let stop_result = {
            let mut pipeline = pipeline.lock().await;
            pipeline.stop().await
        };

        start_result.context("pipeline.start failed")?;
        stop_result.context("pipeline.stop failed")?;
        Ok(())
    }

    async fn close_extractor_after_abort(
        extractor: Arc<Mutex<Box<dyn Extractor + Send>>>,
    ) -> anyhow::Result<()> {
        let mut extractor = extractor.lock().await;
        extractor
            .close()
            .await
            .context("extractor.close after abort failed")
    }

    async fn stop_pipeline_after_abort(
        pipeline: Arc<Mutex<Box<dyn Pipeline + Send>>>,
    ) -> anyhow::Result<()> {
        let mut pipeline = pipeline.lock().await;
        pipeline
            .stop()
            .await
            .context("pipeline.stop after abort failed")
    }

    async fn create_pipeline(
        &self,
        buffer: Arc<DtQueue>,
        shut_down: Arc<AtomicBool>,
        syncer: Arc<Mutex<Syncer>>,
        sinkers: Vec<Arc<AsyncMutex<Box<dyn Sinker + Send>>>>,
        monitor: TaskMonitorHandle,
        monitor_task_id: String,
        ctl_buffer: Arc<DtCtlQueue>,
        data_marker: Option<Arc<RwLock<DataMarker>>>,
        recorder: Option<Arc<dyn Recorder + Send + Sync>>,
        checker: Option<CheckerHandle>,
    ) -> anyhow::Result<Box<dyn Pipeline + Send>> {
        match self.config.pipeline.pipeline_type {
            PipelineType::Basic => {
                let lua_processor =
                    self.config
                        .processor
                        .as_ref()
                        .map(|processor_config| LuaProcessor {
                            lua_code: processor_config.lua_code.clone(),
                        });

                let parallelizer = ParallelizerUtil::create_parallelizer(
                    &self.config,
                    monitor.clone(),
                    monitor_task_id.clone(),
                )
                .await?;

                let pipeline = BasePipeline {
                    buffer,
                    parallelizer,
                    sinker_config: self.config.sinker.clone(),
                    sinkers,
                    shut_down,
                    checkpoint_interval_secs: self.config.pipeline.checkpoint_interval_secs,
                    batch_sink_interval_secs: self.config.pipeline.batch_sink_interval_secs,
                    syncer,
                    monitor,
                    monitor_task_id,
                    ctl_buffer,
                    pending_dtctls: HashMap::new(),
                    data_marker,
                    lua_processor,
                    recorder,
                    checker,
                };
                Ok(Box::new(pipeline) as Box<dyn Pipeline + Send>)
            }

            PipelineType::HttpServer => {
                let meta_manager = ExtractorUtil::get_extractor_meta_manager(&self.config).await?;
                let avro_converter =
                    AvroConverter::new(meta_manager, self.config.pipeline.with_field_defs);
                let pipeline = HttpServerPipeline::new(
                    buffer,
                    syncer,
                    monitor,
                    monitor_task_id,
                    ctl_buffer,
                    avro_converter,
                    self.config.pipeline.checkpoint_interval_secs,
                    self.config.pipeline.batch_sink_interval_secs,
                    &self.config.pipeline.http_host,
                    self.config.pipeline.http_port,
                );
                Ok(Box::new(pipeline) as Box<dyn Pipeline + Send>)
            }
        }
    }

    async fn create_checker(
        &self,
        checker_config: Option<&CheckerConfig>,
        single_task_id: &str,
        monitor: TaskMonitorHandle,
        check_summary: Option<Arc<AsyncMutex<CheckSummaryLog>>>,
        recovery: Option<&Arc<dyn Recovery + Send + Sync>>,
        checker_state_store: Option<Arc<CheckerStateStore>>,
    ) -> anyhow::Result<Option<CheckerHandle>> {
        if !matches!(self.config.pipeline.pipeline_type, PipelineType::Basic) {
            return Ok(None);
        }

        let cfg = match checker_config {
            Some(cfg) => cfg,
            None => return Ok(None),
        };
        let max_connections = cfg.max_connections.max(1);
        let queue_size = cfg.queue_size.max(1);
        let log_level = &self.config.runtime.log_level;
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let is_cdc_task = matches!(self.config.extractor_basic.extract_type, ExtractType::Cdc)
            && matches!(self.config.sinker_basic.sink_type, SinkType::Write);
        let expected_resume_position = if is_cdc_task {
            if let Some(recovery_handler) = recovery {
                recovery_handler.get_cdc_resume_position().await
            } else {
                None
            }
        } else {
            None
        };
        let checker_batch_size = cfg.batch_size.max(1);
        if cfg.batch_size == 0 {
            log_warn!("checker.batch_size=0 is invalid. Using 1.");
        }
        let check_log_dir_base = if cfg.check_log_dir.is_empty() {
            format!("{}/check", self.config.runtime.log_dir)
        } else {
            cfg.check_log_dir.clone()
        };
        let checker_task_id = if single_task_id.is_empty() {
            self.config.global.task_id.clone()
        } else {
            format!("{}::{}", self.config.global.task_id, single_task_id)
        };
        let cdc_check_log_max_file_size =
            parse_size_limit(&cfg.check_log_file_size).map_err(|e| {
                Error::ConfigError(format!(
                    "invalid config [checker].check_log_file_size: {}, error: {}",
                    cfg.check_log_file_size, e
                ))
            })?;
        let cdc_check_log_max_rows = if cfg.check_log_max_rows == 0 {
            log_warn!("checker.check_log_max_rows=0 is invalid. Using 1.");
            1
        } else {
            cfg.check_log_max_rows
        };
        let (max_retries, retry_interval_secs) = if is_cdc_task {
            if cfg.max_retries > 0 || cfg.retry_interval_secs > 0 {
                log_warn!(
                    "CDC+check mode does not support retries. Ignoring max_retries={} and retry_interval_secs={} from config.",
                    cfg.max_retries,
                    cfg.retry_interval_secs
                );
            }
            (0, 0)
        } else {
            (cfg.max_retries, cfg.retry_interval_secs)
        };
        let checker_target = self
            .config
            .checker_target()
            .ok_or_else(|| Error::ConfigError("config [checker] target is missing".into()))?;
        let checker_db_type = checker_target.db_type.clone();
        let checker_url = checker_target.url.clone();
        let checker_auth = checker_target.connection_auth.clone();
        let inline_check = self
            .config
            .task_type()
            .is_some_and(|task_type| task_type.is_inline_check());

        let is_struct_task = matches!(
            self.config.extractor,
            ExtractorConfig::MysqlStruct { .. } | ExtractorConfig::PgStruct { .. }
        );

        if is_struct_task {
            let filter = RdbFilter::from_config(&self.config.filter, &checker_db_type)?;
            let router = RdbRouter::from_config(&self.config.router, &checker_db_type)?;
            let checker = match checker_db_type {
                DbType::Mysql => {
                    let conn_pool = TaskUtil::create_mysql_conn_pool(
                        &checker_url,
                        &DbType::Mysql,
                        &checker_auth,
                        max_connections,
                        enable_sqlx_log,
                        None,
                    )
                    .await?;
                    StructCheckerHandle::new(
                        checker_db_type,
                        Some(conn_pool),
                        None,
                        filter,
                        router,
                        cfg.output_revise_sql,
                        retry_interval_secs,
                        max_retries,
                        check_summary,
                        monitor.clone(),
                        single_task_id.to_string(),
                    )
                }
                DbType::Pg => {
                    let conn_pool = TaskUtil::create_pg_conn_pool(
                        &checker_url,
                        &checker_auth,
                        max_connections,
                        enable_sqlx_log,
                        false,
                    )
                    .await?;
                    StructCheckerHandle::new(
                        checker_db_type,
                        None,
                        Some(conn_pool),
                        filter,
                        router,
                        cfg.output_revise_sql,
                        retry_interval_secs,
                        max_retries,
                        check_summary,
                        monitor.clone(),
                        single_task_id.to_string(),
                    )
                }
                _ => bail!(
                    "struct check not supported for db_type: {}",
                    checker_db_type
                ),
            };
            return Ok(Some(CheckerHandle::Struct(checker)));
        }

        let s3_output = if cfg.cdc_check_log_s3 {
            let s3_cfg = cfg.s3_config.as_ref().ok_or_else(|| {
                Error::ConfigError(
                    "cdc_check_log_s3=true but checker s3 config is missing in [checker]".into(),
                )
            })?;
            let s3_client = TaskUtil::create_s3_client(s3_cfg)?;
            let key_prefix = if single_task_id.is_empty() {
                if cfg.s3_key_prefix.is_empty() {
                    format!("{}/check", self.config.global.task_id)
                } else {
                    cfg.s3_key_prefix.clone()
                }
            } else {
                let base = if cfg.s3_key_prefix.is_empty() {
                    format!("{}/check", self.config.global.task_id)
                } else {
                    cfg.s3_key_prefix.clone()
                };
                let scope = single_task_id
                    .chars()
                    .map(|ch| {
                        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                            ch
                        } else {
                            '_'
                        }
                    })
                    .collect::<String>();
                let scope = if scope.is_empty() {
                    "default".to_string()
                } else {
                    scope
                };
                format!("{}/{}", base.trim_end_matches('/'), scope)
            };
            Some((s3_client, key_prefix))
        } else {
            None
        };
        let state_store = checker_state_store.clone();

        let build_check_context =
            |extractor_meta_manager,
             reverse_router,
             source_checker: Option<Arc<AsyncMutex<Box<dyn Checker>>>>,
             revise_match_full_row| CheckContext {
                extractor_meta_manager,
                reverse_router,
                batch_size: checker_batch_size,
                monitor: monitor.clone(),
                monitor_task_id: single_task_id.to_string(),
                base_sinker: BaseSinker::new(
                    monitor.clone(),
                    single_task_id.to_string(),
                    self.config.pipeline.checkpoint_interval_secs,
                ),
                output_full_row: cfg.output_full_row,
                output_revise_sql: cfg.output_revise_sql,
                revise_match_full_row,
                retry_interval_secs,
                max_retries,
                is_cdc: is_cdc_task,
                summary: CheckSummaryLog {
                    start_time: Local::now().to_rfc3339(),
                    ..Default::default()
                },
                global_summary: check_summary.clone(),
                check_log_dir: check_log_dir_base.clone(),
                cdc_check_log_max_file_size,
                cdc_check_log_max_rows,
                s3_output: s3_output.clone(),
                cdc_check_log_interval_secs: cfg.cdc_check_log_interval_secs,
                state_store: state_store.clone(),
                source_checker,
                expected_resume_position: expected_resume_position.clone(),
            };

        match checker_db_type {
            DbType::Mysql => {
                let reverse_router = create_router!(self.config, Mysql).reverse();
                let extractor_meta_manager =
                    ExtractorUtil::get_extractor_meta_manager(&self.config).await?;
                let source_checker = self
                    .create_source_checker(is_cdc_task, enable_sqlx_log)
                    .await?;
                let conn_pool = TaskUtil::create_mysql_conn_pool(
                    &checker_url,
                    &DbType::Mysql,
                    &checker_auth,
                    max_connections,
                    enable_sqlx_log,
                    None,
                )
                .await?;
                let meta_manager =
                    dt_common::meta::mysql::mysql_meta_manager::MysqlMetaManager::new(
                        conn_pool.clone(),
                    )
                    .await?;
                let checker = DataCheckerHandle::spawn(
                    MysqlChecker::new(conn_pool, meta_manager),
                    checker_task_id.clone(),
                    build_check_context(
                        extractor_meta_manager,
                        reverse_router,
                        source_checker,
                        cfg.revise_match_full_row,
                    ),
                    queue_size,
                    "MysqlChecker",
                );
                Ok(Some(CheckerHandle::Data(checker)))
            }
            DbType::Pg => {
                let reverse_router = create_router!(self.config, Pg).reverse();
                let extractor_meta_manager =
                    ExtractorUtil::get_extractor_meta_manager(&self.config).await?;
                let source_checker = self
                    .create_source_checker(is_cdc_task, enable_sqlx_log)
                    .await?;
                let conn_pool = TaskUtil::create_pg_conn_pool(
                    &checker_url,
                    &checker_auth,
                    max_connections,
                    enable_sqlx_log,
                    false,
                )
                .await?;
                let meta_manager =
                    dt_common::meta::pg::pg_meta_manager::PgMetaManager::new(conn_pool.clone())
                        .await?;
                let checker = DataCheckerHandle::spawn(
                    PgChecker::new(conn_pool, meta_manager),
                    checker_task_id.clone(),
                    build_check_context(
                        extractor_meta_manager,
                        reverse_router,
                        source_checker,
                        cfg.revise_match_full_row,
                    ),
                    queue_size,
                    "PgChecker",
                );
                Ok(Some(CheckerHandle::Data(checker)))
            }
            DbType::Mongo => {
                let reverse_router = create_router!(self.config, Mongo).reverse();
                let source_checker = self
                    .create_source_checker(is_cdc_task, enable_sqlx_log)
                    .await?;
                let app_name = match (&self.config.sinker, inline_check) {
                    (SinkerConfig::Mongo { app_name, .. }, true) => app_name.as_str(),
                    _ => "checker",
                };
                let mongo_client = TaskUtil::create_mongo_client(
                    &checker_url,
                    &checker_auth,
                    app_name,
                    Some(max_connections),
                )
                .await?;
                let checker = DataCheckerHandle::spawn(
                    MongoChecker::new(mongo_client),
                    checker_task_id.clone(),
                    build_check_context(None, reverse_router, source_checker, false),
                    queue_size,
                    "MongoChecker",
                );
                Ok(Some(CheckerHandle::Data(checker)))
            }
            _ => bail!("checker not supported for db_type: {}", checker_db_type),
        }
    }

    async fn create_source_checker(
        &self,
        is_cdc_task: bool,
        enable_sqlx_log: bool,
    ) -> anyhow::Result<Option<Arc<AsyncMutex<Box<dyn Checker>>>>> {
        if !is_cdc_task {
            return Ok(None);
        }

        let checker: Box<dyn Checker> = match self.config.extractor_basic.db_type {
            DbType::Mysql => {
                let pool = TaskUtil::create_mysql_conn_pool(
                    &self.config.extractor_basic.url,
                    &DbType::Mysql,
                    &self.config.extractor_basic.connection_auth,
                    1,
                    enable_sqlx_log,
                    None,
                )
                .await?;
                let meta_manager =
                    dt_common::meta::mysql::mysql_meta_manager::MysqlMetaManager::new(pool.clone())
                        .await?;
                Box::new(MysqlChecker::new(pool, meta_manager))
            }
            DbType::Pg => {
                let pool = TaskUtil::create_pg_conn_pool(
                    &self.config.extractor_basic.url,
                    &self.config.extractor_basic.connection_auth,
                    1,
                    enable_sqlx_log,
                    false,
                )
                .await?;
                let meta_manager =
                    dt_common::meta::pg::pg_meta_manager::PgMetaManager::new(pool.clone()).await?;
                Box::new(PgChecker::new(pool, meta_manager))
            }
            DbType::Mongo => {
                let client = TaskUtil::create_mongo_client(
                    &self.config.extractor_basic.url,
                    &self.config.extractor_basic.connection_auth,
                    "checker-source",
                    Some(1),
                )
                .await?;
                Box::new(MongoChecker::new(client))
            }
            _ => return Ok(None),
        };

        Ok(Some(Arc::new(AsyncMutex::new(checker))))
    }

    async fn init_log4rs(&self) -> anyhow::Result<()> {
        let log4rs_file = &self.config.runtime.log4rs_file;
        if metadata(log4rs_file).await.is_err() {
            return Ok(());
        }

        let mut config_str = String::new();
        let mut file = File::open(log4rs_file).await?;
        file.read_to_string(&mut config_str).await?;

        match &self.config.sinker {
            SinkerConfig::RedisStatistic {
                statistic_log_dir, ..
            } => {
                if !statistic_log_dir.is_empty() {
                    config_str =
                        config_str.replace(STATISTIC_LOG_DIR_PLACEHOLDER, statistic_log_dir);
                }
            }

            _ => {
                if let Some(cfg) = self.config.checker.as_ref() {
                    let check_log_dir = &cfg.check_log_dir;
                    let check_log_file_size = &cfg.check_log_file_size;
                    if !check_log_dir.is_empty() {
                        config_str = config_str.replace(CHECK_LOG_DIR_PLACEHOLDER, check_log_dir);
                    }
                    config_str =
                        config_str.replace(CHECK_LOG_FILE_SIZE_PLACEHOLDER, check_log_file_size);
                }
            }
        }

        config_str = config_str
            .replace(CHECK_LOG_DIR_PLACEHOLDER, DEFAULT_CHECK_LOG_DIR_PLACEHOLDER)
            .replace(
                STATISTIC_LOG_DIR_PLACEHOLDER,
                DEFAULT_STATISTIC_LOG_DIR_PLACEHOLDER,
            )
            .replace(CHECK_LOG_FILE_SIZE_PLACEHOLDER, DEFAULT_CHECK_LOG_FILE_SIZE)
            .replace(LOG_DIR_PLACEHOLDER, &self.config.runtime.log_dir)
            .replace(LOG_LEVEL_PLACEHOLDER, &self.config.runtime.log_level);

        let raw: RawConfig = serde_yaml::from_str(&config_str)?;
        let mut deserializers = Deserializers::default();
        deserializers.insert("size_limit", SizeLimitFilterDeserializer);
        let (appenders, errors) = raw.appenders_lossy(&deserializers);
        if !errors.is_empty() {
            bail!("errors deserializing appenders: {:?}", errors);
        }

        let config = Config::builder()
            .appenders(appenders)
            .loggers(raw.loggers())
            .build(raw.root())?;
        let mut handle_guard = LOG_HANDLE.lock().unwrap();
        if let Some(handle) = handle_guard.as_ref() {
            // refresh log4rs config in one process
            handle.set_config(config);
        } else {
            let handle = log4rs::init_config(config)?;
            *handle_guard = Some(handle);
        }
        Ok(())
    }

    async fn create_task_tables(
        &self,
        extractor_client: ConnClient,
        sinker_client: ConnClient,
        sinker_data_marker: Option<DataMarker>,
    ) -> anyhow::Result<()> {
        // create heartbeat table
        let heartbeat_schema_tb = match &self.config.extractor {
            ExtractorConfig::MysqlCdc { heartbeat_tb, .. }
            | ExtractorConfig::PgCdc { heartbeat_tb, .. } => ConfigTokenParser::parse(
                heartbeat_tb,
                &['.'],
                &TokenEscapePair::from_char_pairs(SqlUtil::get_escape_pairs(
                    &self.config.extractor_basic.db_type,
                )),
            ),
            _ => vec![],
        };

        if heartbeat_schema_tb.len() == 2 {
            match &self.config.extractor {
                ExtractorConfig::MysqlCdc { .. } => {
                    let db_sql =
                        format!("CREATE DATABASE IF NOT EXISTS `{}`", heartbeat_schema_tb[0]);
                    let tb_sql = format!(
                        "CREATE TABLE IF NOT EXISTS `{}`.`{}`(
                        server_id INT UNSIGNED,
                        update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        received_binlog_filename VARCHAR(255),
                        received_next_event_position INT UNSIGNED,
                        received_timestamp VARCHAR(255),
                        flushed_binlog_filename VARCHAR(255),
                        flushed_next_event_position INT UNSIGNED,
                        flushed_timestamp VARCHAR(255),
                        PRIMARY KEY(server_id)
                    )",
                        heartbeat_schema_tb[0], heartbeat_schema_tb[1]
                    );

                    TaskUtil::check_and_create_tb(
                        &extractor_client.clone(),
                        &heartbeat_schema_tb[0],
                        &heartbeat_schema_tb[1],
                        &db_sql,
                        &tb_sql,
                        &DbType::Mysql,
                    )
                    .await?
                }

                ExtractorConfig::PgCdc { .. } => {
                    let schema_sql = format!(
                        r#"CREATE SCHEMA IF NOT EXISTS "{}""#,
                        heartbeat_schema_tb[0]
                    );
                    let tb_sql = format!(
                        r#"CREATE TABLE IF NOT EXISTS "{}"."{}"(
                        slot_name character varying(64) not null,
                        update_timestamp timestamp without time zone default (now() at time zone 'utc'),
                        received_lsn character varying(64),
                        received_timestamp character varying(64),
                        flushed_lsn character varying(64),
                        flushed_timestamp character varying(64),
                        primary key(slot_name)
                    )"#,
                        heartbeat_schema_tb[0], heartbeat_schema_tb[1]
                    );

                    TaskUtil::check_and_create_tb(
                        &extractor_client.clone(),
                        &heartbeat_schema_tb[0],
                        &heartbeat_schema_tb[1],
                        &schema_sql,
                        &tb_sql,
                        &DbType::Pg,
                    )
                    .await?
                }

                _ => {}
            }
        }

        // create data marker table
        if let Some(data_marker) = sinker_data_marker {
            match &self.config.sinker {
                SinkerConfig::Mysql { .. } => {
                    let db_sql = format!(
                        "CREATE DATABASE IF NOT EXISTS `{}`",
                        data_marker.marker_schema
                    );
                    let tb_sql = format!(
                        "CREATE TABLE IF NOT EXISTS `{}`.`{}` (
                            data_origin_node varchar(255) NOT NULL,
                            src_node varchar(255) NOT NULL,
                            dst_node varchar(255) NOT NULL,
                            n bigint DEFAULT NULL,
                            PRIMARY KEY (data_origin_node, src_node, dst_node)
                        )",
                        data_marker.marker_schema, data_marker.marker_tb
                    );

                    TaskUtil::check_and_create_tb(
                        &sinker_client.clone(),
                        &data_marker.marker_schema,
                        &data_marker.marker_tb,
                        &db_sql,
                        &tb_sql,
                        &DbType::Mysql,
                    )
                    .await?
                }

                SinkerConfig::Pg { .. } => {
                    let schema_sql = format!(
                        r#"CREATE SCHEMA IF NOT EXISTS "{}""#,
                        data_marker.marker_schema
                    );
                    let tb_sql = format!(
                        r#"CREATE TABLE IF NOT EXISTS "{}"."{}" (
                            data_origin_node varchar(255) NOT NULL,
                            src_node varchar(255) NOT NULL,
                            dst_node varchar(255) NOT NULL,
                            n bigint DEFAULT NULL,
                            PRIMARY KEY (data_origin_node, src_node, dst_node)
                        )"#,
                        data_marker.marker_schema, data_marker.marker_tb
                    );

                    TaskUtil::check_and_create_tb(
                        &sinker_client.clone(),
                        &data_marker.marker_schema,
                        &data_marker.marker_tb,
                        &schema_sql,
                        &tb_sql,
                        &DbType::Pg,
                    )
                    .await?
                }

                _ => {}
            }
        }
        Ok(())
    }

    async fn get_task_info(
        &self,
        extractor_client: ConnClient,
        recovery: Option<Arc<dyn Recovery + Send + Sync>>,
    ) -> anyhow::Result<TaskInfo> {
        let db_type = &self.config.extractor_basic.db_type;
        let filter = &self.filter;

        let mut schema_tbs = HashMap::new();
        let schemas = TaskUtil::list_schemas(&extractor_client, db_type)
            .await?
            .iter()
            .filter(|schema| !filter.filter_schema(schema))
            .map(|s| s.to_owned())
            .collect::<Vec<_>>();
        if schemas.is_empty() {
            log_warn!("no schemas to extract");
            return Ok(TaskInfo {
                extractor_config: self.config.extractor.clone(),
                no_data: true,
            });
        }

        if matches!(
            self.config.extractor,
            ExtractorConfig::MysqlSnapshot { .. }
                | ExtractorConfig::PgSnapshot { .. }
                | ExtractorConfig::MongoSnapshot { .. }
                | ExtractorConfig::FoxlakeS3 { .. }
        ) {
            if let Some(task_type) = &self.task_type {
                log_info!("begin to estimate record count");
                let record_count = TaskUtil::estimate_record_count(
                    task_type,
                    &extractor_client,
                    db_type,
                    &schemas,
                    filter,
                )
                .await?;
                log_info!("estimate record count: {}", record_count);

                self.task_monitor
                    .add_no_window_metrics(TaskMetricsType::ExtractorPlanRecords, record_count);
            }
        }

        match &self.config.extractor {
            ExtractorConfig::MysqlStruct {
                url,
                connection_auth,
                db,
                db_batch_size,
                ..
            } => {
                return Ok(TaskInfo {
                    extractor_config: ExtractorConfig::MysqlStruct {
                        url: url.clone(),
                        connection_auth: connection_auth.clone(),
                        db: db.clone(),
                        dbs: schemas,
                        db_batch_size: *db_batch_size,
                    },
                    no_data: false,
                });
            }
            ExtractorConfig::PgStruct {
                url,
                connection_auth,
                schema,
                db_batch_size,
                ..
            } => {
                return Ok(TaskInfo {
                    extractor_config: ExtractorConfig::PgStruct {
                        url: url.clone(),
                        connection_auth: connection_auth.clone(),
                        schema: schema.clone(),
                        schemas,
                        do_global_structs: true,
                        db_batch_size: *db_batch_size,
                    },
                    no_data: false,
                })
            }
            _ => {}
        };
        for schema in schemas.iter() {
            // find pending tables
            let tbs = TaskUtil::list_tbs(&extractor_client, schema, db_type).await?;

            self.task_monitor
                .add_no_window_metrics(TaskMetricsType::TotalProgressCount, tbs.len() as u64);
            let mut finished_tbs = 0;

            let mut tables = Vec::new();
            for tb in tbs.iter() {
                if let Some(recovery_handler) = recovery.as_ref() {
                    if recovery_handler.check_snapshot_finished(schema, tb).await {
                        log_info!("schema: {}, tb: {}, already finished", schema, tb);
                        finished_tbs += 1;
                        continue;
                    }
                }

                if filter.filter_event(schema, tb, &RowType::Insert) {
                    log_info!("schema: {}, tb: {}, insert events filtered", schema, tb);
                    continue;
                }
                tables.push(tb.to_owned());
            }
            schema_tbs.insert(schema.clone(), tables);

            self.task_monitor
                .add_no_window_metrics(TaskMetricsType::FinishedProgressCount, finished_tbs as u64);
        }
        let extractor_config = match &self.config.extractor {
            ExtractorConfig::MysqlSnapshot {
                url,
                connection_auth,
                sample_interval,
                parallel_size,
                parallel_type,
                batch_size,
                ..
            } => ExtractorConfig::MysqlSnapshot {
                url: url.clone(),
                connection_auth: connection_auth.clone(),
                db: String::new(),
                tb: String::new(),
                db_tbs: schema_tbs,
                sample_interval: *sample_interval,
                parallel_size: *parallel_size,
                parallel_type: parallel_type.clone(),
                batch_size: *batch_size,
                partition_cols: String::new(),
            },

            ExtractorConfig::PgSnapshot {
                url,
                connection_auth,
                sample_interval,
                parallel_size,
                parallel_type,
                batch_size,
                ..
            } => ExtractorConfig::PgSnapshot {
                url: url.clone(),
                connection_auth: connection_auth.clone(),
                schema: String::new(),
                tb: String::new(),
                schema_tbs,
                sample_interval: *sample_interval,
                parallel_size: *parallel_size,
                parallel_type: parallel_type.clone(),
                batch_size: *batch_size,
                partition_cols: String::new(),
            },

            ExtractorConfig::MongoSnapshot {
                url,
                connection_auth,
                app_name,
                parallel_size,
                parallel_type,
                ..
            } => ExtractorConfig::MongoSnapshot {
                url: url.clone(),
                connection_auth: connection_auth.clone(),
                app_name: app_name.clone(),
                db: String::new(),
                tb: String::new(),
                db_tbs: schema_tbs,
                parallel_size: *parallel_size,
                parallel_type: parallel_type.clone(),
            },

            ExtractorConfig::FoxlakeS3 {
                url,
                s3_config,
                parallel_size,
                parallel_type,
                batch_size,
                ..
            } => ExtractorConfig::FoxlakeS3 {
                url: url.clone(),
                schema: String::new(),
                tb: String::new(),
                schema_tbs,
                parallel_size: *parallel_size,
                parallel_type: parallel_type.clone(),
                s3_config: s3_config.clone(),
                batch_size: *batch_size,
            },

            _ => self.config.extractor.clone(),
        };
        Ok(TaskInfo {
            extractor_config,
            no_data: false,
        })
    }
}
