use std::{
    collections::VecDeque,
    panic,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{bail, Context};
use log4rs::config::RawConfig;
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
    extractor_util::ExtractorUtil, parallelizer_util::ParallelizerUtil, sinker_util::SinkerUtil,
};
use crate::task_util::TaskUtil;
use dt_common::{
    config::{
        config_enums::{build_task_type, DbType, PipelineType},
        config_token_parser::{ConfigTokenParser, TokenEscapePair},
        extractor_config::ExtractorConfig,
        sinker_config::SinkerConfig,
        task_config::TaskConfig,
    },
    error::Error,
    log_error, log_finished, log_info,
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
    data_marker::DataMarker,
    extractor::resumer::{cdc_resumer::CdcResumer, snapshot_resumer::SnapshotResumer},
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
pub struct TaskContext<'a> {
    pub id: String,
    pub extractor_config: ExtractorConfig,
    pub router: &'a RdbRouter,
    pub snapshot_resumer: &'a SnapshotResumer,
    pub cdc_resumer: &'a CdcResumer,
}

#[derive(Clone)]
pub struct TaskRunner {
    config: TaskConfig,
    extractor_monitor: Arc<GroupMonitor>,
    pipeline_monitor: Arc<GroupMonitor>,
    sinker_monitor: Arc<GroupMonitor>,
    task_monitor: Arc<TaskMonitor>,
    #[cfg(feature = "metrics")]
    prometheus_metrics: Arc<PrometheusMetrics>,
}

const CHECK_LOG_DIR_PLACEHOLDER: &str = "CHECK_LOG_DIR_PLACEHOLDER";
const STATISTIC_LOG_DIR_PLACEHOLDER: &str = "STATISTIC_LOG_DIR_PLACEHOLDER";
const LOG_LEVEL_PLACEHOLDER: &str = "LOG_LEVEL_PLACEHOLDER";
const LOG_DIR_PLACEHOLDER: &str = "LOG_DIR_PLACEHOLDER";
const DEFAULT_CHECK_LOG_DIR_PLACEHOLDER: &str = "LOG_DIR_PLACEHOLDER/check";
const DEFAULT_STATISTIC_LOG_DIR_PLACEHOLDER: &str = "LOG_DIR_PLACEHOLDER/statistic";

impl TaskRunner {
    pub fn new(task_config_file: &str) -> anyhow::Result<Self> {
        let config = TaskConfig::new(task_config_file)
            .with_context(|| format!("invalid configs in [{}]", task_config_file))?;
        let task_type = build_task_type(
            &config.extractor_basic.extract_type,
            &config.sinker_basic.sink_type,
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
            task_monitor,
            #[cfg(feature = "metrics")]
            prometheus_metrics,
        })
    }

    pub async fn start_task(&self, enable_log4rs: bool) -> anyhow::Result<()> {
        if enable_log4rs {
            self.init_log4rs().await?;
        }

        panic::set_hook(Box::new(|panic_info| {
            let backtrace = std::backtrace::Backtrace::capture();
            log_error!("panic: {}\nbacktrace:\n{}", panic_info, backtrace);
        }));

        let db_type = &self.config.extractor_basic.db_type;
        let router = RdbRouter::from_config(&self.config.router, db_type)?;
        let snapshot_resumer = SnapshotResumer::from_config(&self.config)?;
        let cdc_resumer = CdcResumer::from_config(&self.config)?;

        #[cfg(feature = "metrics")]
        self.prometheus_metrics
            .initialization()
            .start_metrics()
            .await;

        match &self.config.extractor {
            ExtractorConfig::MysqlStruct { url, .. } | ExtractorConfig::PgStruct { url, .. } => {
                let mut pending_task = self
                    .build_pending_tasks(url, &router, &snapshot_resumer, &cdc_resumer, false)
                    .await?;
                if !pending_task.is_empty() {
                    if let Some(task_context) = pending_task.pop_front() {
                        self.clone()
                            .start_single_task(
                                &task_context.extractor_config,
                                &router,
                                &snapshot_resumer,
                                &cdc_resumer,
                                false,
                            )
                            .await?
                    }
                }
            }

            ExtractorConfig::MysqlSnapshot { url, .. }
            | ExtractorConfig::PgSnapshot { url, .. }
            | ExtractorConfig::MongoSnapshot { url, .. }
            | ExtractorConfig::FoxlakeS3 { url, .. } => {
                self.start_multi_task(url, &router, &snapshot_resumer, &cdc_resumer)
                    .await?
            }

            _ => {
                self.clone()
                    .start_single_task(
                        &self.config.extractor,
                        &router,
                        &snapshot_resumer,
                        &cdc_resumer,
                        false,
                    )
                    .await?
            }
        };

        log_finished!("task finished");
        Ok(())
    }

    async fn start_multi_task(
        &self,
        url: &str,
        router: &RdbRouter,
        snapshot_resumer: &SnapshotResumer,
        cdc_resumer: &CdcResumer,
    ) -> anyhow::Result<()> {
        let mut pending_tasks = self
            .build_pending_tasks(url, router, snapshot_resumer, cdc_resumer, true)
            .await?;

        // start a thread to flush global monitors
        let global_shut_down = Arc::new(AtomicBool::new(false));
        let global_shut_down_clone = global_shut_down.clone();
        let interval_secs = self.config.pipeline.checkpoint_interval_secs;
        let extractor_monitor = self.extractor_monitor.clone();
        let pipeline_monitor = self.pipeline_monitor.clone();
        let sinker_monitor = self.sinker_monitor.clone();
        let task_monitor = self.task_monitor.clone();
        let global_monitor_task = tokio::spawn(async move {
            Self::flush_monitors_generic::<GroupMonitor, TaskMonitor>(
                interval_secs,
                global_shut_down_clone,
                &[extractor_monitor, pipeline_monitor, sinker_monitor],
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
        task_context: TaskContext<'_>,
        join_set: &mut JoinSet<(String, anyhow::Result<()>)>,
        semaphore: &Arc<tokio::sync::Semaphore>,
    ) -> anyhow::Result<()> {
        let single_task_id = task_context.id;
        let extractor_config = task_context.extractor_config;
        let router = task_context.router.clone();
        let snapshot_resumer = task_context.snapshot_resumer.clone();
        let cdc_resumer = task_context.cdc_resumer.clone();
        let semaphore = Arc::clone(semaphore);
        let me = self.clone();
        join_set.spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let res = me
                .start_single_task(
                    &extractor_config,
                    &router,
                    &snapshot_resumer,
                    &cdc_resumer,
                    true,
                )
                .await;
            (single_task_id, res)
        });
        Ok(())
    }

    async fn start_single_task(
        self,
        extractor_config: &ExtractorConfig,
        router: &RdbRouter,
        snapshot_resumer: &SnapshotResumer,
        cdc_resumer: &CdcResumer,
        is_multi_task: bool,
    ) -> anyhow::Result<()> {
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

        let single_task_id = match extractor_config {
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
            extractor_config,
            buffer.clone(),
            shut_down.clone(),
            syncer.clone(),
            extractor_monitor.clone(),
            extractor_data_marker,
            router.clone(),
            snapshot_resumer.clone(),
            cdc_resumer.clone(),
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
            extractor_config,
            sinker_monitor.clone(),
            rw_sinker_data_marker.clone(),
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
                self.task_monitor.register(
                    &single_task_id,
                    vec![
                        (MonitorType::Extractor, extractor_monitor.clone()),
                        (MonitorType::Pipeline, pipeline_monitor.clone()),
                        (MonitorType::Sinker, sinker_monitor.clone()),
                    ],
                );
            }
        );

        // do pre operations before task starts
        self.pre_single_task(sinker_data_marker).await?;

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
                &[extractor_monitor, pipeline_monitor, sinker_monitor],
                &tasks,
            )
            .await
        });
        try_join!(f1, f2, f3)?;

        // finished log
        let (schema, tb) = match extractor_config {
            ExtractorConfig::MysqlSnapshot { db, tb, .. }
            | ExtractorConfig::MongoSnapshot { db, tb, .. } => (db.to_owned(), tb.to_owned()),
            ExtractorConfig::PgSnapshot { schema, tb, .. }
            | ExtractorConfig::FoxlakeS3 { schema, tb, .. } => (schema.to_owned(), tb.to_owned()),
            _ => (String::new(), String::new()),
        };
        if !tb.is_empty() {
            log_finished!(
                "{}",
                Position::RdbSnapshotFinished {
                    db_type: self.config.extractor_basic.db_type.to_string(),
                    schema,
                    tb,
                }
                .to_string()
            );
            self.task_monitor
                .add_no_window_metrics(TaskMetricsType::FinishedProgressCount, 1);
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
                self.task_monitor.unregister(
                    &single_task_id,
                    vec![
                        MonitorType::Extractor,
                        MonitorType::Pipeline,
                        MonitorType::Sinker,
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
        sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
        monitor: Arc<Monitor>,
        data_marker: Option<Arc<RwLock<DataMarker>>>,
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

                let parallelizer = ParallelizerUtil::create_parallelizer(
                    &self.config,
                    monitor.clone(),
                    rps_limiter,
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
                };
                Ok(Box::new(pipeline))
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
                Ok(Box::new(pipeline))
            }
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
            SinkerConfig::MysqlCheck { check_log_dir, .. }
            | SinkerConfig::PgCheck { check_log_dir, .. } => {
                if !check_log_dir.is_empty() {
                    config_str = config_str.replace(CHECK_LOG_DIR_PLACEHOLDER, check_log_dir);
                }
            }

            SinkerConfig::RedisStatistic {
                statistic_log_dir, ..
            } => {
                if !statistic_log_dir.is_empty() {
                    config_str =
                        config_str.replace(STATISTIC_LOG_DIR_PLACEHOLDER, statistic_log_dir);
                }
            }

            _ => {}
        }

        config_str = config_str
            .replace(CHECK_LOG_DIR_PLACEHOLDER, DEFAULT_CHECK_LOG_DIR_PLACEHOLDER)
            .replace(
                STATISTIC_LOG_DIR_PLACEHOLDER,
                DEFAULT_STATISTIC_LOG_DIR_PLACEHOLDER,
            )
            .replace(LOG_DIR_PLACEHOLDER, &self.config.runtime.log_dir)
            .replace(LOG_LEVEL_PLACEHOLDER, &self.config.runtime.log_level);

        let config: RawConfig = serde_yaml::from_str(&config_str)?;
        log4rs::init_raw_config(config)?;
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

    async fn pre_single_task(&self, sinker_data_marker: Option<DataMarker>) -> anyhow::Result<()> {
        // create heartbeat table
        let schema_tb = match &self.config.extractor {
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

        if schema_tb.len() == 2 {
            match &self.config.extractor {
                ExtractorConfig::MysqlCdc { url, .. } => {
                    let db_sql = format!("CREATE DATABASE IF NOT EXISTS `{}`", schema_tb[0]);
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
                        schema_tb[0], schema_tb[1]
                    );

                    TaskUtil::check_and_create_tb(
                        url,
                        &schema_tb[0],
                        &schema_tb[1],
                        &db_sql,
                        &tb_sql,
                        &DbType::Mysql,
                    )
                    .await?
                }

                ExtractorConfig::PgCdc { url, .. } => {
                    let schema_sql = format!(r#"CREATE SCHEMA IF NOT EXISTS "{}""#, schema_tb[0]);
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
                        schema_tb[0], schema_tb[1]
                    );

                    TaskUtil::check_and_create_tb(
                        url,
                        &schema_tb[0],
                        &schema_tb[1],
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
                SinkerConfig::Mysql { url, .. } => {
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
                        url,
                        &data_marker.marker_schema,
                        &data_marker.marker_tb,
                        &db_sql,
                        &tb_sql,
                        &DbType::Mysql,
                    )
                    .await?
                }

                SinkerConfig::Pg { url, .. } => {
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
                        url,
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

    async fn build_pending_tasks<'a>(
        &self,
        url: &str,
        router: &'a RdbRouter,
        snapshot_resumer: &'a SnapshotResumer,
        cdc_resumer: &'a CdcResumer,
        is_multi_task: bool,
    ) -> anyhow::Result<VecDeque<TaskContext<'a>>> {
        let db_type = &self.config.extractor_basic.db_type;
        let filter = RdbFilter::from_config(&self.config.filter, db_type)?;

        let schemas = TaskUtil::list_schemas(url, db_type)
            .await?
            .iter()
            .filter(|schema| !filter.filter_schema(schema))
            .map(|s| s.to_owned())
            .collect::<Vec<_>>();

        if is_multi_task {
            let task_type_option = build_task_type(
                &self.config.extractor_basic.extract_type,
                &self.config.sinker_basic.sink_type,
            );
            if let Some(task_type) = task_type_option {
                log_info!("begin to estimate record count");
                let record_count =
                    TaskUtil::estimate_record_count(&task_type, url, db_type, &schemas, &filter)
                        .await?;
                log_info!("estimate record count: {}", record_count);

                self.task_monitor
                    .add_no_window_metrics(TaskMetricsType::ExtractorPlanRecords, record_count);
            }
        }

        let mut pending_tasks = VecDeque::new();
        let is_db_extractor_config = match &self.config.extractor {
            ExtractorConfig::MysqlStruct { .. } | ExtractorConfig::PgStruct { .. } => true,
            _ => false,
        };
        if is_db_extractor_config {
            let db_extractor_config = match &self.config.extractor {
                ExtractorConfig::MysqlStruct {
                    url,
                    db,
                    db_batch_size,
                    ..
                } => ExtractorConfig::MysqlStruct {
                    url: url.clone(),
                    db: db.clone(),
                    dbs: schemas.clone(),
                    db_batch_size: db_batch_size.clone(),
                },
                ExtractorConfig::PgStruct {
                    url,
                    schema,
                    do_global_structs,
                    db_batch_size,
                    ..
                } => ExtractorConfig::PgStruct {
                    url: url.clone(),
                    schema: schema.clone(),
                    schemas: schemas,
                    do_global_structs: do_global_structs.clone(),
                    db_batch_size: db_batch_size.clone(),
                },
                _ => {
                    bail! {Error::ConfigError("unsupported extractor config type".into())}
                }
            };
            pending_tasks.push_back(TaskContext {
                extractor_config: db_extractor_config,
                router: router,
                snapshot_resumer: snapshot_resumer,
                cdc_resumer: cdc_resumer,
                id: "".to_string(),
            });
        } else {
            for schema in schemas.iter() {
                // find pending tables
                let tbs = TaskUtil::list_tbs(url, schema, db_type).await?;

                self.task_monitor
                    .add_no_window_metrics(TaskMetricsType::TotalProgressCount, tbs.len() as u64);
                let mut finished_tbs = 0;
                
                for tb in tbs.iter() {
                    if snapshot_resumer.check_finished(schema, tb) {
                        log_info!("schema: {}, tb: {}, already finished", schema, tb);
                        finished_tbs += 1;
                        continue;
                    }
                    if filter.filter_event(schema, tb, &RowType::Insert) {
                        log_info!("schema: {}, tb: {}, insert events filtered", schema, tb);
                        continue;
                    }
                    let tb_extractor_config = match &self.config.extractor {
                        ExtractorConfig::MysqlSnapshot {
                            url,
                            sample_interval,
                            parallel_size,
                            batch_size,
                            ..
                        } => ExtractorConfig::MysqlSnapshot {
                            url: url.clone(),
                            db: schema.clone(),
                            tb: tb.clone(),
                            sample_interval: *sample_interval,
                            parallel_size: *parallel_size,
                            batch_size: *batch_size,
                        },

                        ExtractorConfig::PgSnapshot {
                            url,
                            sample_interval,
                            batch_size,
                            ..
                        } => ExtractorConfig::PgSnapshot {
                            url: url.clone(),
                            schema: schema.clone(),
                            tb: tb.clone(),
                            sample_interval: *sample_interval,
                            batch_size: *batch_size,
                        },

                        ExtractorConfig::MongoSnapshot { url, app_name, .. } => {
                            ExtractorConfig::MongoSnapshot {
                                url: url.clone(),
                                app_name: app_name.clone(),
                                db: schema.clone(),
                                tb: tb.clone(),
                            }
                        }

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
                        router: router,
                        snapshot_resumer: snapshot_resumer,
                        cdc_resumer: cdc_resumer,
                        id: format!("{}.{}", schema, tb),
                    });
                }
                
                self.task_monitor
                .add_no_window_metrics(TaskMetricsType::FinishedProgressCount, finished_tbs as u64);
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
