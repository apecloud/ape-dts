use std::{
    collections::VecDeque,
    panic,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{bail, Context};
use chrono::Local;
use log4rs::config::{Config, Deserializers, RawConfig};
use ratelimit::Ratelimiter;
use tokio::{
    fs::{metadata, File},
    io::AsyncReadExt,
    select,
    sync::{Mutex, RwLock},
    task::JoinSet,
    time::Duration,
    try_join,
};

use super::{
    extractor_util::{ExtractorUtil, PartitionCols},
    parallelizer_util::ParallelizerUtil,
    sinker_util::SinkerUtil,
};
use crate::create_router;
use crate::task_util::{ConnClient, TaskUtil};
use async_mutex::Mutex as AsyncMutex;
use std::sync::Mutex as StdMutex;

static LOG_HANDLE: StdMutex<Option<log4rs::Handle>> = StdMutex::new(None);
use dt_common::log_filter::SizeLimitFilterDeserializer;
use dt_common::{
    config::{
        checker_config::CheckerConfig,
        config_enums::{build_task_type, DbType, ExtractType, PipelineType, SinkType, TaskType},
        config_token_parser::{ConfigTokenParser, TokenEscapePair},
        extractor_config::ExtractorConfig,
        sinker_config::SinkerConfig,
        task_config::{TaskConfig, DEFAULT_CHECK_LOG_FILE_SIZE},
    },
    error::Error,
    log_error, log_finished, log_info, log_warn,
    meta::{
        avro::avro_converter::AvroConverter, dt_queue::DtQueue, position::Position,
        row_type::RowType, syncer::Syncer,
    },
    monitor::{
        group_monitor::GroupMonitor,
        monitor::Monitor,
        task_metrics::TaskMetricsType,
        task_monitor::{MonitorType, TaskMonitor},
        FlushableMonitor,
    },
    rdb_filter::RdbFilter,
    utils::{sql_util::SqlUtil, time_util::TimeUtil},
};
use dt_connector::{
    checker::base_checker::CheckContext,
    checker::check_log::CheckSummaryLog,
    checker::{
        CheckerHandle, MongoCheckerHandle, MysqlCheckerHandle, PgCheckerHandle, StructCheckerHandle,
    },
    data_marker::DataMarker,
    extractor::resumer::{recorder::Recorder, recovery::Recovery},
    rdb_router::RdbRouter,
    Sinker,
};
use dt_pipeline::{
    base_pipeline::BasePipeline, http_server_pipeline::HttpServerPipeline,
    lua_processor::LuaProcessor, Pipeline,
};

#[cfg(feature = "metrics")]
use dt_common::monitor::prometheus_metrics::PrometheusMetrics;

#[derive(Clone)]
pub struct TaskContext {
    pub id: String,
    pub extractor_config: ExtractorConfig,
    pub extractor_client: ConnClient,
    pub partition_cols: Option<Arc<PartitionCols>>,
    pub sinker_client: ConnClient,
    pub router: Arc<RdbRouter>,
    pub recorder: Option<Arc<dyn Recorder + Send + Sync>>,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
    pub check_summary: Option<Arc<AsyncMutex<CheckSummaryLog>>>,
}

#[derive(Clone)]
pub struct TaskRunner {
    task_type: Option<TaskType>,
    config: TaskConfig,
    extractor_monitor: Arc<GroupMonitor>,
    pipeline_monitor: Arc<GroupMonitor>,
    sinker_monitor: Arc<GroupMonitor>,
    checker_monitor: Arc<GroupMonitor>,
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

impl TaskRunner {
    pub fn new(task_config_file: &str) -> anyhow::Result<Self> {
        let config = TaskConfig::new(task_config_file)
            .with_context(|| format!("invalid configs in [{}]", task_config_file))?;
        let task_type = build_task_type(
            &config.extractor_basic.extract_type,
            &config.sinker_basic.sink_type,
            config.checker.is_some(),
        );
        #[cfg(not(feature = "metrics"))]
        let task_monitor = Arc::new(TaskMonitor::new(task_type.clone()));

        #[cfg(feature = "metrics")]
        let prometheus_metrics = Arc::new(PrometheusMetrics::new(
            task_type.clone(),
            config.metrics.clone(),
        ));

        #[cfg(feature = "metrics")]
        let task_monitor = Arc::new(TaskMonitor::new(
            task_type.clone(),
            prometheus_metrics.clone(),
        ));

        Ok(Self {
            config,
            extractor_monitor: Arc::new(GroupMonitor::new("extractor", "global")),
            pipeline_monitor: Arc::new(GroupMonitor::new("pipeline", "global")),
            sinker_monitor: Arc::new(GroupMonitor::new("sinker", "global")),
            checker_monitor: Arc::new(GroupMonitor::new("checker", "global")),
            task_monitor,
            #[cfg(feature = "metrics")]
            prometheus_metrics,
            task_type,
        })
    }

    pub async fn start_task(&self) -> anyhow::Result<()> {
        self.init_log4rs().await?;

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
        let (recorder, recovery) = match &self.task_type {
            Some(task_type) => {
                TaskUtil::build_resumer(
                    task_type.to_owned(),
                    &self.config.global,
                    &self.config.resumer,
                )
                .await?
            }
            None => (None, None),
        };
        let (extractor_client, sinker_client) = ConnClient::from_config(&self.config).await?;

        let check_summary = self.config.checker.as_ref().map(|_| {
            Arc::new(AsyncMutex::new(CheckSummaryLog {
                start_time: Local::now().to_rfc3339(),
                ..Default::default()
            }))
        });

        let partition_cols = match &self.config.extractor {
            ExtractorConfig::MysqlSnapshot { partition_cols, .. }
            | ExtractorConfig::PgSnapshot { partition_cols, .. } => Some(Arc::new(
                ExtractorUtil::parse_partition_cols(partition_cols)?,
            )),
            _ => None,
        };

        let task_context = TaskContext {
            id: String::new(),
            extractor_config: self.config.extractor.clone(),
            extractor_client: extractor_client.clone(),
            partition_cols,
            sinker_client: sinker_client.clone(),
            router,
            recorder,
            recovery,
            check_summary: check_summary.clone(),
        };

        #[cfg(feature = "metrics")]
        self.prometheus_metrics
            .initialization()
            .start_metrics()
            .await;

        match &self.config.extractor {
            ExtractorConfig::MysqlStruct { .. } | ExtractorConfig::PgStruct { .. } => {
                let mut pending_tasks = self.build_pending_tasks(task_context, false).await?;
                if let Some(task_context) = pending_tasks.pop_front() {
                    self.clone().start_single_task(task_context, false).await?
                }
            }

            ExtractorConfig::MysqlSnapshot { .. }
            | ExtractorConfig::PgSnapshot { .. }
            | ExtractorConfig::MongoSnapshot { .. }
            | ExtractorConfig::FoxlakeS3 { .. } => self.start_multi_task(task_context).await?,

            _ => self.clone().start_single_task(task_context, false).await?,
        };

        // close connections
        extractor_client.close().await?;
        sinker_client.close().await?;

        if let Some(check_summary) = check_summary {
            let mut summary = check_summary.lock().await;
            summary.end_time = Local::now().to_rfc3339();
            summary.is_consistent = summary.miss_count == 0 && summary.diff_count == 0;
            dt_common::log_summary!("{}", summary);
        }

        log_finished!("task finished");
        log::logger().flush();
        Ok(())
    }

    async fn start_multi_task(&self, task_context: TaskContext) -> anyhow::Result<()> {
        let mut pending_tasks = self.build_pending_tasks(task_context, true).await?;

        // start a thread to flush global monitors
        let global_shut_down = Arc::new(AtomicBool::new(false));
        let global_shut_down_clone = global_shut_down.clone();
        let interval_secs = self.config.pipeline.checkpoint_interval_secs;
        let extractor_monitor = self.extractor_monitor.clone();
        let pipeline_monitor = self.pipeline_monitor.clone();
        let sinker_monitor = self.sinker_monitor.clone();
        let checker_monitor = self.checker_monitor.clone();
        let task_monitor = self.task_monitor.clone();
        let global_monitor_task = tokio::spawn(async move {
            Self::flush_monitors_generic::<GroupMonitor, TaskMonitor>(
                interval_secs,
                global_shut_down_clone,
                &[
                    extractor_monitor,
                    pipeline_monitor,
                    sinker_monitor,
                    checker_monitor,
                ],
                &[task_monitor],
            )
            .await
        });

        let task_parallel_size = self.get_task_parallel_size();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(task_parallel_size));
        let mut join_set: JoinSet<(String, anyhow::Result<()>)> = JoinSet::new();

        // initialize the task pool to its maximum capacity
        while join_set.len() < task_parallel_size && !pending_tasks.is_empty() {
            if let Some(task_context) = pending_tasks.pop_front() {
                self.clone()
                    .spawn_single_task(task_context, &mut join_set, &semaphore)
                    .await?;
            }
        }

        // when a task is completed, if there are still pending tables, add a new task
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((_, Ok(()))) => {
                    if let Some(task_context) = pending_tasks.pop_front() {
                        self.clone()
                            .spawn_single_task(task_context, &mut join_set, &semaphore)
                            .await?;
                    }
                }
                Ok((single_task_id, Err(e))) => {
                    bail!("single task: [{}] failed, error: {}", single_task_id, e)
                }
                Err(e) => {
                    bail!("join error: {}", e)
                }
            }
        }

        global_shut_down.store(true, Ordering::Release);
        global_monitor_task.await?;
        Ok(())
    }

    async fn spawn_single_task(
        self,
        task_context: TaskContext,
        join_set: &mut JoinSet<(String, anyhow::Result<()>)>,
        semaphore: &Arc<tokio::sync::Semaphore>,
    ) -> anyhow::Result<()> {
        let single_task_id = task_context.id;
        let semaphore = Arc::clone(semaphore);
        let task_context = TaskContext {
            id: single_task_id.clone(),
            extractor_config: task_context.extractor_config,
            extractor_client: task_context.extractor_client,
            sinker_client: task_context.sinker_client,
            router: task_context.router,
            recorder: task_context.recorder,
            recovery: task_context.recovery,
            check_summary: task_context.check_summary,
            partition_cols: task_context.partition_cols,
        };
        let me = self.clone();
        join_set.spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let res = me.start_single_task(task_context, true).await;
            (single_task_id, res)
        });
        Ok(())
    }

    async fn start_single_task(
        self,
        task_context: TaskContext,
        is_multi_task: bool,
    ) -> anyhow::Result<()> {
        let extractor_config = task_context.extractor_config;
        let extractor_client = task_context.extractor_client;
        let sinker_client = task_context.sinker_client;
        let router = (*task_context.router).clone();
        let recorder = task_context.recorder.clone();
        let recovery = task_context.recovery.clone();

        let max_bytes = self.config.pipeline.buffer_memory_mb * 1024 * 1024;
        let buffer = Arc::new(DtQueue::new(
            self.config.pipeline.buffer_size,
            max_bytes as u64,
        ));

        let shut_down = Arc::new(AtomicBool::new(false));
        let syncer = Arc::new(Mutex::new(Syncer {
            received_position: Position::None,
            committed_position: Position::None,
        }));

        let (extractor_data_marker, sinker_data_marker) = if let Some(data_marker_config) =
            &self.config.data_marker
        {
            let extractor_data_marker =
                DataMarker::from_config(data_marker_config, &self.config.extractor_basic.db_type)?;
            let sinker_data_marker =
                DataMarker::from_config(data_marker_config, &self.config.sinker_basic.db_type)?;
            (Some(extractor_data_marker), Some(sinker_data_marker))
        } else {
            (None, None)
        };
        let rw_sinker_data_marker = sinker_data_marker
            .clone()
            .map(|data_marker| Arc::new(RwLock::new(data_marker)));

        let single_task_id = match &extractor_config {
            ExtractorConfig::MysqlSnapshot { db, tb, .. } => format!("{}.{}", db, tb),
            ExtractorConfig::PgSnapshot { schema, tb, .. } => format!("{}.{}", schema, tb),
            ExtractorConfig::MongoSnapshot { db, tb, .. } => format!("{}.{}", db, tb),
            _ => String::new(),
        };

        // extractor
        let monitor_time_window_secs = self.config.pipeline.counter_time_window_secs;
        let monitor_max_sub_count = self.config.pipeline.counter_max_sub_count;
        let monitor_count_window = self.config.pipeline.buffer_size as u64;
        let extractor_monitor = Arc::new(Monitor::new(
            "extractor",
            &single_task_id,
            monitor_time_window_secs,
            monitor_max_sub_count,
            monitor_count_window,
        ));
        let mut extractor = ExtractorUtil::create_extractor(
            &self.config,
            &extractor_config,
            extractor_client.clone(),
            task_context.partition_cols,
            buffer.clone(),
            shut_down.clone(),
            syncer.clone(),
            extractor_monitor.clone(),
            extractor_data_marker,
            router,
            recovery,
        )
        .await?;

        // sinkers
        let sinker_monitor = Arc::new(Monitor::new(
            "sinker",
            &single_task_id,
            monitor_time_window_secs,
            monitor_max_sub_count,
            monitor_count_window,
        ));
        let sinkers = SinkerUtil::create_sinkers(
            &self.config,
            &extractor_config,
            sinker_client.clone(),
            sinker_monitor.clone(),
            rw_sinker_data_marker.clone(),
        )
        .await?;

        // checker
        let checker_monitor = Arc::new(Monitor::new(
            "checker",
            &single_task_id,
            monitor_time_window_secs,
            monitor_max_sub_count,
            monitor_count_window,
        ));
        let checker = self
            .create_checker(
                self.config.checker.as_ref(),
                checker_monitor.clone(),
                task_context.check_summary.clone(),
            )
            .await?;

        // pipeline
        let pipeline_monitor = Arc::new(Monitor::new(
            "pipeline",
            &single_task_id,
            monitor_time_window_secs,
            monitor_max_sub_count,
            monitor_count_window,
        ));

        let mut pipeline = self
            .create_pipeline(
                buffer,
                shut_down.clone(),
                syncer,
                sinkers,
                pipeline_monitor.clone(),
                rw_sinker_data_marker.clone(),
                recorder.clone(),
                checker,
            )
            .await?;

        // add monitors to global monitors
        tokio::join!(
            async {
                self.extractor_monitor
                    .add_monitor(&single_task_id, extractor_monitor.clone());
            },
            async {
                self.pipeline_monitor
                    .add_monitor(&single_task_id, pipeline_monitor.clone());
            },
            async {
                self.sinker_monitor
                    .add_monitor(&single_task_id, sinker_monitor.clone());
            },
            async {
                self.checker_monitor
                    .add_monitor(&single_task_id, checker_monitor.clone());
            },
            async {
                self.task_monitor.register(
                    &single_task_id,
                    vec![
                        (MonitorType::Extractor, extractor_monitor.clone()),
                        (MonitorType::Pipeline, pipeline_monitor.clone()),
                        (MonitorType::Sinker, sinker_monitor.clone()),
                        (MonitorType::Checker, checker_monitor.clone()),
                    ],
                );
            }
        );

        // do pre operations before task starts
        self.pre_single_task(
            extractor_client.clone(),
            sinker_client.clone(),
            sinker_data_marker,
        )
        .await?;

        // snapshot check_summary before this subtask so we can compute per-subtask deltas
        let check_summary_snapshot = if let Some(check_summary) = &task_context.check_summary {
            let s = check_summary.lock().await;
            Some((s.miss_count, s.diff_count, s.sql_count.unwrap_or(0)))
        } else {
            None
        };

        // start threads
        let f1 = tokio::spawn(async move {
            extractor.extract().await.unwrap();
            extractor.close().await.unwrap();
        });

        let f2 = tokio::spawn(async move {
            pipeline.start().await.unwrap();
            pipeline.stop().await.unwrap();
        });

        let interval_secs = self.config.pipeline.checkpoint_interval_secs;
        let tasks: Vec<Arc<TaskMonitor>> = if is_multi_task {
            vec![]
        } else {
            vec![self.task_monitor.clone()]
        };
        let f3 = tokio::spawn(async move {
            Self::flush_monitors_generic::<Monitor, TaskMonitor>(
                interval_secs,
                shut_down,
                &[
                    extractor_monitor,
                    pipeline_monitor,
                    sinker_monitor,
                    checker_monitor,
                ],
                &tasks,
            )
            .await
        });
        try_join!(f1, f2, f3)?;

        if let (Some(check_summary), Some((prev_miss, prev_diff, _prev_sql))) =
            (&task_context.check_summary, check_summary_snapshot)
        {
            let summary = check_summary.lock().await;
            let delta_miss = summary.miss_count.saturating_sub(prev_miss);
            let delta_diff = summary.diff_count.saturating_sub(prev_diff);
            self.task_monitor
                .add_no_window_metrics(TaskMetricsType::CheckerMissCount, delta_miss as u64);
            self.task_monitor
                .add_no_window_metrics(TaskMetricsType::CheckerDiffCount, delta_diff as u64);
        }

        // finished log
        let (schema, tb) = match &extractor_config {
            ExtractorConfig::MysqlSnapshot { db, tb, .. }
            | ExtractorConfig::MongoSnapshot { db, tb, .. } => (db.to_owned(), tb.to_owned()),
            ExtractorConfig::PgSnapshot { schema, tb, .. }
            | ExtractorConfig::FoxlakeS3 { schema, tb, .. } => (schema.to_owned(), tb.to_owned()),
            _ => (String::new(), String::new()),
        };
        if !tb.is_empty() {
            let finish_position = Position::RdbSnapshotFinished {
                db_type: self.config.extractor_basic.db_type.to_string(),
                schema,
                tb,
            };
            log_finished!("{}", finish_position.to_string());
            self.task_monitor
                .add_no_window_metrics(TaskMetricsType::FinishedProgressCount, 1);

            if let Some(handler) = &recorder {
                if let Err(e) = handler.record_position(&finish_position).await {
                    log_error!("failed to record position: {}, err: {}", finish_position, e);
                }
            }
        }

        // remove monitors from global monitors
        tokio::join!(
            async {
                self.extractor_monitor.remove_monitor(&single_task_id);
            },
            async {
                self.pipeline_monitor.remove_monitor(&single_task_id);
            },
            async {
                self.sinker_monitor.remove_monitor(&single_task_id);
            },
            async {
                self.checker_monitor.remove_monitor(&single_task_id);
            },
            async {
                self.task_monitor.unregister(
                    &single_task_id,
                    vec![
                        MonitorType::Extractor,
                        MonitorType::Pipeline,
                        MonitorType::Sinker,
                        MonitorType::Checker,
                    ],
                );
            }
        );

        Ok(())
    }

    async fn create_pipeline(
        &self,
        buffer: Arc<DtQueue>,
        shut_down: Arc<AtomicBool>,
        syncer: Arc<Mutex<Syncer>>,
        sinkers: Vec<Arc<AsyncMutex<Box<dyn Sinker + Send>>>>,
        monitor: Arc<Monitor>,
        data_marker: Option<Arc<RwLock<DataMarker>>>,
        recorder: Option<Arc<dyn Recorder + Send + Sync>>,
        checker: Option<CheckerHandle>,
    ) -> anyhow::Result<Box<dyn Pipeline + Send>> {
        match self.config.pipeline.pipeline_type {
            PipelineType::Basic => {
                let rps_limiter = if self.config.pipeline.max_rps > 0 {
                    Some(
                        Ratelimiter::builder(self.config.pipeline.max_rps, Duration::from_secs(1))
                            .max_tokens(self.config.pipeline.max_rps)
                            .initial_available(self.config.pipeline.max_rps)
                            .build()?,
                    )
                } else {
                    None
                };

                let lua_processor =
                    self.config
                        .processor
                        .as_ref()
                        .map(|processor_config| LuaProcessor {
                            lua_code: processor_config.lua_code.clone(),
                        });

                let (parallelizer, remaining_checker) = ParallelizerUtil::create_parallelizer(
                    &self.config,
                    monitor.clone(),
                    rps_limiter,
                    checker,
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
                    data_marker,
                    lua_processor,
                    recorder,
                    checker: remaining_checker,
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
        monitor: Arc<Monitor>,
        check_summary: Option<Arc<AsyncMutex<CheckSummaryLog>>>,
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
        let (max_retries, retry_interval_secs) = if is_cdc_task {
            // CDC tasks: data arrives as a stream, checker verifies immediately
            // after sinker commits. Retries are harmful because subsequent
            // DELETE events may remove data that was correctly written,
            // causing false misses in the retry queue.
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
        let missing = || Error::ConfigError("config [checker] target is required".into());
        let fallback_target = if matches!(self.config.sinker_basic.sink_type, SinkType::Dummy) {
            None
        } else {
            Some((
                &self.config.sinker_basic.db_type,
                &self.config.sinker_basic.url,
                &self.config.sinker_basic.connection_auth,
            ))
        };
        let checker_db_type = cfg
            .db_type
            .clone()
            .or_else(|| fallback_target.map(|(db_type, _, _)| db_type.clone()))
            .ok_or_else(missing)?;
        let checker_url = cfg
            .url
            .clone()
            .or_else(|| fallback_target.map(|(_, url, _)| url.clone()))
            .ok_or_else(missing)?;
        let checker_auth = cfg
            .connection_auth
            .clone()
            .or_else(|| fallback_target.map(|(_, _, auth)| auth.clone()))
            .ok_or_else(missing)?;

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
                    )
                }
                _ => bail!(
                    "struct check not supported for db_type: {}",
                    checker_db_type
                ),
            };
            return Ok(Some(CheckerHandle::Struct(checker)));
        }

        match checker_db_type {
            DbType::Mysql => {
                let reverse_router = create_router!(self.config, Mysql).reverse();
                let extractor_meta_manager =
                    ExtractorUtil::get_extractor_meta_manager(&self.config).await?;
                let conn_pool = TaskUtil::create_mysql_conn_pool(
                    &checker_url,
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
                let checker = MysqlCheckerHandle::spawn(
                    conn_pool,
                    meta_manager,
                    CheckContext {
                        extractor_meta_manager,
                        reverse_router,
                        batch_size: cfg.batch_size,
                        monitor,
                        output_full_row: cfg.output_full_row,
                        output_revise_sql: cfg.output_revise_sql,
                        revise_match_full_row: cfg.revise_match_full_row,
                        retry_interval_secs,
                        max_retries,
                        summary: CheckSummaryLog {
                            start_time: Local::now().to_rfc3339(),
                            ..Default::default()
                        },
                        global_summary: check_summary,
                    },
                    queue_size,
                );
                Ok(Some(CheckerHandle::Data(checker)))
            }
            DbType::Pg => {
                let reverse_router = create_router!(self.config, Pg).reverse();
                let extractor_meta_manager =
                    ExtractorUtil::get_extractor_meta_manager(&self.config).await?;
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
                let checker = PgCheckerHandle::spawn(
                    conn_pool,
                    meta_manager,
                    CheckContext {
                        extractor_meta_manager,
                        reverse_router,
                        batch_size: cfg.batch_size,
                        monitor,
                        output_full_row: cfg.output_full_row,
                        output_revise_sql: cfg.output_revise_sql,
                        revise_match_full_row: cfg.revise_match_full_row,
                        retry_interval_secs,
                        max_retries,
                        summary: CheckSummaryLog {
                            start_time: Local::now().to_rfc3339(),
                            ..Default::default()
                        },
                        global_summary: check_summary,
                    },
                    queue_size,
                );
                Ok(Some(CheckerHandle::Data(checker)))
            }
            DbType::Mongo => {
                let reverse_router = create_router!(self.config, Mongo).reverse();
                let app_name = if cfg.db_type.is_some() && !is_cdc_task {
                    "checker"
                } else {
                    match &self.config.sinker {
                        SinkerConfig::Mongo { app_name, .. } => app_name.as_str(),
                        _ => "checker",
                    }
                };
                let mongo_client = TaskUtil::create_mongo_client(
                    &checker_url,
                    &checker_auth,
                    app_name,
                    Some(max_connections),
                )
                .await?;
                let checker = MongoCheckerHandle::spawn(
                    mongo_client,
                    CheckContext {
                        extractor_meta_manager: None,
                        reverse_router,
                        batch_size: cfg.batch_size,
                        monitor,
                        output_full_row: cfg.output_full_row,
                        output_revise_sql: cfg.output_revise_sql,
                        revise_match_full_row: false,
                        retry_interval_secs,
                        max_retries,
                        summary: CheckSummaryLog {
                            start_time: Local::now().to_rfc3339(),
                            ..Default::default()
                        },
                        global_summary: check_summary,
                    },
                    queue_size,
                );
                Ok(Some(CheckerHandle::Data(checker)))
            }
            _ => bail!("checker not supported for db_type: {}", checker_db_type),
        }
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

    async fn flush_monitors_generic<T1, T2>(
        interval_secs: u64,
        shut_down: Arc<AtomicBool>,
        t1_monitors: &[Arc<T1>],
        t2_monitors: &[Arc<T2>],
    ) where
        T1: FlushableMonitor + Send + Sync + 'static,
        T2: FlushableMonitor + Send + Sync + 'static,
    {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        interval.tick().await;

        loop {
            if shut_down.load(Ordering::Acquire) {
                Self::do_flush_monitors(t1_monitors, t2_monitors).await;
                break;
            }

            select! {
                _ = interval.tick() => {
                    Self::do_flush_monitors(t1_monitors, t2_monitors).await;
                }
                _ = Self::wait_for_shutdown(shut_down.clone()) => {
                    log_info!("task shutdown detected, do final flush");
                    Self::do_flush_monitors(t1_monitors, t2_monitors).await;
                    break;
                }
            }
        }
    }

    async fn wait_for_shutdown(shut_down: Arc<AtomicBool>) {
        loop {
            if shut_down.load(Ordering::Acquire) {
                break;
            }
            TimeUtil::sleep_millis(100).await;
        }
    }

    async fn do_flush_monitors<T1, T2>(t1_monitors: &[Arc<T1>], t2_monitors: &[Arc<T2>])
    where
        T1: FlushableMonitor + Send + Sync + 'static,
        T2: FlushableMonitor + Send + Sync + 'static,
    {
        let t1_futures = t1_monitors
            .iter()
            .map(|monitor| {
                let monitor = monitor.clone();
                async move { monitor.flush().await }
            })
            .collect::<Vec<_>>();

        let t2_futures = t2_monitors
            .iter()
            .map(|monitor| {
                let monitor = monitor.clone();
                async move { monitor.flush().await }
            })
            .collect::<Vec<_>>();

        tokio::join!(
            futures::future::join_all(t1_futures),
            futures::future::join_all(t2_futures)
        );
    }

    async fn pre_single_task(
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

    async fn build_pending_tasks(
        &self,
        original_task_context: TaskContext,
        is_multi_task: bool,
    ) -> anyhow::Result<VecDeque<TaskContext>> {
        let db_type = &self.config.extractor_basic.db_type;
        let filter = RdbFilter::from_config(&self.config.filter, db_type)?;

        let mut pending_tasks = VecDeque::new();

        let schemas =
            TaskUtil::list_schemas(&original_task_context.extractor_client.clone(), db_type)
                .await?
                .iter()
                .filter(|schema| !filter.filter_schema(schema))
                .map(|s| s.to_owned())
                .collect::<Vec<_>>();
        if schemas.is_empty() {
            log_warn!("no schemas to extract");
            return Ok(pending_tasks);
        }

        if is_multi_task {
            if let Some(task_type) = &self.task_type {
                log_info!("begin to estimate record count");
                let record_count = TaskUtil::estimate_record_count(
                    task_type,
                    &original_task_context.extractor_client.clone(),
                    db_type,
                    &schemas,
                    &filter,
                )
                .await?;
                log_info!("estimate record count: {}", record_count);

                self.task_monitor
                    .add_no_window_metrics(TaskMetricsType::ExtractorPlanRecords, record_count);
            }
        }

        let router = original_task_context.router.clone();
        let extractor_client = original_task_context.extractor_client.clone();
        let sinker_client = original_task_context.sinker_client.clone();

        let is_db_extractor_config = matches!(
            &self.config.extractor,
            ExtractorConfig::MysqlStruct { .. } | ExtractorConfig::PgStruct { .. }
        );
        if is_db_extractor_config {
            let db_extractor_config = match &self.config.extractor {
                ExtractorConfig::MysqlStruct {
                    url,
                    connection_auth,
                    db,
                    db_batch_size,
                    ..
                } => ExtractorConfig::MysqlStruct {
                    url: url.clone(),
                    connection_auth: connection_auth.clone(),
                    db: db.clone(),
                    dbs: schemas,
                    db_batch_size: *db_batch_size,
                },
                ExtractorConfig::PgStruct {
                    url,
                    connection_auth,
                    schema,
                    db_batch_size,
                    ..
                } => ExtractorConfig::PgStruct {
                    url: url.clone(),
                    connection_auth: connection_auth.clone(),
                    schema: schema.clone(),
                    schemas,
                    do_global_structs: true,
                    db_batch_size: *db_batch_size,
                },
                _ => {
                    bail! {Error::ConfigError("unsupported extractor config type".into())}
                }
            };
            pending_tasks.push_back(TaskContext {
                extractor_config: db_extractor_config,
                router,
                id: "".to_string(),
                extractor_client,
                sinker_client,
                recorder: original_task_context.recorder.clone(),
                recovery: original_task_context.recovery.clone(),
                check_summary: original_task_context.check_summary.clone(),
                partition_cols: original_task_context.partition_cols.clone(),
            });
        } else {
            for schema in schemas.iter() {
                // find pending tables
                let tbs = TaskUtil::list_tbs(
                    &original_task_context.extractor_client.clone(),
                    schema,
                    db_type,
                )
                .await?;

                self.task_monitor
                    .add_no_window_metrics(TaskMetricsType::TotalProgressCount, tbs.len() as u64);
                let mut finished_tbs = 0;

                for tb in tbs.iter() {
                    if let Some(recovery_handler) = original_task_context.recovery.as_ref() {
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
                    let tb_extractor_config = match &self.config.extractor {
                        ExtractorConfig::MysqlSnapshot {
                            url,
                            connection_auth,
                            sample_interval,
                            parallel_size,
                            batch_size,
                            ..
                        } => ExtractorConfig::MysqlSnapshot {
                            url: url.clone(),
                            connection_auth: connection_auth.clone(),
                            db: schema.clone(),
                            tb: tb.clone(),
                            sample_interval: *sample_interval,
                            parallel_size: *parallel_size,
                            batch_size: *batch_size,
                            partition_cols: String::new(),
                        },

                        ExtractorConfig::PgSnapshot {
                            url,
                            connection_auth,
                            sample_interval,
                            parallel_size,
                            batch_size,
                            ..
                        } => ExtractorConfig::PgSnapshot {
                            url: url.clone(),
                            connection_auth: connection_auth.clone(),
                            schema: schema.clone(),
                            tb: tb.clone(),
                            sample_interval: *sample_interval,
                            parallel_size: *parallel_size,
                            batch_size: *batch_size,
                            partition_cols: String::new(),
                        },

                        ExtractorConfig::MongoSnapshot {
                            url,
                            connection_auth,
                            app_name,
                            ..
                        } => ExtractorConfig::MongoSnapshot {
                            url: url.clone(),
                            connection_auth: connection_auth.clone(),
                            app_name: app_name.clone(),
                            db: schema.clone(),
                            tb: tb.clone(),
                        },

                        ExtractorConfig::FoxlakeS3 {
                            url,
                            s3_config,
                            batch_size,
                            ..
                        } => ExtractorConfig::FoxlakeS3 {
                            url: url.clone(),
                            schema: schema.clone(),
                            tb: tb.clone(),
                            s3_config: s3_config.clone(),
                            batch_size: *batch_size,
                        },

                        _ => {
                            bail! {Error::ConfigError("unsupported extractor config for `runtime.tb_parallel_size`".into())};
                        }
                    };
                    pending_tasks.push_back(TaskContext {
                        extractor_config: tb_extractor_config,
                        router: router.clone(),
                        id: format!("{}.{}", schema, tb),
                        extractor_client: extractor_client.clone(),
                        sinker_client: sinker_client.clone(),
                        recorder: original_task_context.recorder.clone(),
                        recovery: original_task_context.recovery.clone(),
                        check_summary: original_task_context.check_summary.clone(),
                        partition_cols: original_task_context.partition_cols.clone(),
                    });
                }

                self.task_monitor.add_no_window_metrics(
                    TaskMetricsType::FinishedProgressCount,
                    finished_tbs as u64,
                );
            }
        }
        Ok(pending_tasks)
    }

    fn get_task_parallel_size(&self) -> usize {
        match &self.config.extractor {
            ExtractorConfig::MysqlSnapshot { .. }
            | ExtractorConfig::PgSnapshot { .. }
            | ExtractorConfig::FoxlakeS3 { .. }
            | ExtractorConfig::MongoSnapshot { .. } => self.config.runtime.tb_parallel_size,
            _ => 1,
        }
    }
}
