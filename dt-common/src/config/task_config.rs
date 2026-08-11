use std::{collections::HashMap, fs, io::ErrorKind};

use anyhow::{bail, Error, Ok};

#[cfg(feature = "metrics")]
use crate::config::metrics_config::MetricsConfig;
use crate::{
    config::{
        config_enums::{RdbParallelType, ResumeType},
        connection_auth_config::ConnectionAuthConfig,
        global_config::GlobalConfig,
        limiter_config::{CapacityLimiterConfig, RateLimiterConfig},
    },
    error::{DtError, DtOptionExt, DtResultExt},
    meta::{
        mongo::mongo_cdc_source::MongoCdcSource, mssql::mssql_connection_pool::MssqlConnectionPool,
    },
    utils::task_util::TaskUtil,
};

use super::{
    checker_config::{
        CheckerConfig, CheckerOutputConfig, CheckerOutputType, InlineCheckConfig,
        DEFAULT_CHECK_LOG_FILE_SIZE,
    },
    config_enums::{
        CheckMode, ConflictPolicyEnum, DbType, ExtractType, MetaCenterType, ParallelType, SinkType,
        TaskKind, TaskType,
    },
    data_marker_config::DataMarkerConfig,
    extractor_config::{BasicExtractorConfig, ExtractorConfig},
    filter_config::FilterConfig,
    ini_loader::IniLoader,
    meta_center_config::MetaCenterConfig,
    parallelizer_config::{
        ChunkPartitionerRebalanceConfig, ChunkPartitionerRebalanceCost,
        ChunkPartitionerRebalanceStrategy, ParallelizerConfig,
    },
    pipeline_config::PipelineConfig,
    processor_config::ProcessorConfig,
    resumer_config::ResumerConfig,
    router_config::RouterConfig,
    runtime_config::RuntimeConfig,
    sinker_config::{BasicSinkerConfig, SinkerConfig},
    tracing_config::TracingConfig,
};

#[derive(Clone)]
pub struct TaskConfig {
    pub global: GlobalConfig,
    pub extractor_basic: BasicExtractorConfig,
    pub extractor: ExtractorConfig,
    pub sinker_basic: BasicSinkerConfig,
    pub sinker: SinkerConfig,
    pub runtime: RuntimeConfig,
    pub parallelizer: ParallelizerConfig,
    pub pipeline: PipelineConfig,
    pub filter: FilterConfig,
    pub router: RouterConfig,
    pub resumer: ResumerConfig,
    pub checker: Option<CheckerConfig>,
    pub meta_center: Option<MetaCenterConfig>,
    pub data_marker: Option<DataMarkerConfig>,
    pub processor: Option<ProcessorConfig>,
    pub tracing: TracingConfig,
    #[cfg(feature = "metrics")]
    pub metrics: MetricsConfig,
}

pub const DEFAULT_DB_BATCH_SIZE: usize = 100;
pub const DEFAULT_MAX_CONNECTIONS: u32 = 10;

// sections
const GLOBAL: &str = "global";
const EXTRACTOR: &str = "extractor";
const SINKER: &str = "sinker";
const PIPELINE: &str = "pipeline";
const PARALLELIZER: &str = "parallelizer";
const RUNTIME: &str = "runtime";
const FILTER: &str = "filter";
const ROUTER: &str = "router";
const RESUMER: &str = "resumer";
const DATA_MARKER: &str = "data_marker";
const PROCESSOR: &str = "processor";
const CHECKER: &str = "checker";
const CHECKER_OUTPUT: &str = "checker_output";
const CHECKER_CDC: &str = "checker_cdc";
const META_CENTER: &str = "metacenter";
const TRACING: &str = "tracing";
// keys
const DB_TYPE: &str = "db_type";
const URL: &str = "url";
const BATCH_SIZE: &str = "batch_size";
const MAX_CONNECTIONS: &str = "max_connections";
const CONNECTION_TIMEOUT_SECS: &str = "connection_timeout_secs";
const PARTITION_COLS: &str = "partition_cols";
const HEARTBEAT_INTERVAL_SECS: &str = "heartbeat_interval_secs";
const KEEPALIVE_INTERVAL_SECS: &str = "keepalive_interval_secs";
const HEARTBEAT_TB: &str = "heartbeat_tb";
const APP_NAME: &str = "app_name";
const REVERSE: &str = "reverse";
const REPL_PORT: &str = "repl_port";
const PARALLEL_SIZE: &str = "parallel_size";
const REBALANCE_STRATEGY: &str = "rebalance_strategy";
const REBALANCE_COST: &str = "rebalance_cost";
const REBALANCE_MAX_PARTITIONS_PER_SINKER: &str = "rebalance_max_partitions_per_sinker";
const REBALANCE_MIN_PARTITION_ROWS: &str = "rebalance_min_partition_rows";
const REBALANCE_SPLIT_SKEW_RATIO: &str = "rebalance_split_skew_ratio";
const LEGACY_TB_PARALLEL_SIZE: &str = "tb_parallel_size";
const DDL_CONFLICT_POLICY: &str = "ddl_conflict_policy";
const REPLACE: &str = "replace";
const DISABLE_FOREIGN_KEY_CHECKS: &str = "disable_foreign_key_checks";
const RESUME_TYPE: &str = "resume_type";
const CHECKER_QUEUE_SIZE: &str = "queue_size";
const CHECK_LOG_INTERVAL_SECS: &str = "check_log_interval_secs";
const SAMPLE_PERCENT: &str = "sample_percent";
const IS_DIRECT_CONNECTION: &str = "is_direct_connection";
const MONGO_REQUIRE_SHARD_KEY_FILTER: &str = "mongo_require_shard_key_filter";
const TASK_SUMMARY_MODE: &str = "task_summary_mode";
const OUTPUT_FORMAT: &str = "output_format";
// keys, checker
const CHECK_LOG_DIR: &str = "check_log_dir";
const CHECK_LOG_FILE_SIZE: &str = "check_log_file_size";
const CHECK_LOG_MAX_ROWS: &str = "check_log_max_rows";
const OUTPUT_FULL_ROW: &str = "output_full_row";
const OUTPUT_REVISE_SQL: &str = "output_revise_sql";
const REVISE_MATCH_FULL_ROW: &str = "revise_match_full_row";
const RECHECK_INTERVAL_SECS: &str = "recheck_interval_secs";
const RECHECK_COUNT: &str = "recheck_count";
const RECHECK_QUEUE_SIZE: &str = "recheck_queue_size";
const RECHECK_QUEUE_MEMORY_MB: &str = "recheck_queue_memory_mb";
const IS_ENABLED: &str = "is_enabled";
const LEGACY_CHECKER_ENABLE: &str = "enable";

// default values
pub const APE_DTS: &str = "APE_DTS";
const ASTRISK: &str = "*";
const RESUMER_CONNECTION_LIMIT_DEFAULT: usize = 5;

impl TaskConfig {
    pub fn new(task_config_file: &str) -> anyhow::Result<Self> {
        let loader = IniLoader::new(task_config_file)?;
        Self::from_loader(&loader)
    }

    pub fn from_loader(loader: &IniLoader) -> anyhow::Result<Self> {
        let pipeline = Self::load_pipeline_config(loader)?;
        let runtime = Self::load_runtime_config(loader)?;
        let (sinker_basic, sinker) = Self::load_sinker_config(loader)?;
        let (extractor_basic, extractor) = Self::load_extractor_config(loader, &pipeline)?;
        let filter = Self::load_filter_config(loader)?;
        let router = Self::load_router_config(loader)?;
        let parallelizer = Self::load_parallelizer_config(loader, &sinker_basic, &pipeline)?;
        let checker =
            Self::load_checker_config(loader, &extractor_basic.extract_type, &sinker_basic)?;
        if let Some(checker_cfg) = checker.as_ref() {
            let task_type = if matches!(extractor_basic.extract_type, ExtractType::CheckLog)
                && matches!(sinker_basic.sink_type, SinkType::Check)
                && matches!(
                    sinker_basic.db_type,
                    DbType::Mysql | DbType::Pg | DbType::Mongo
                ) {
                None
            } else {
                Some(
                    Self::build_task_type(
                        &extractor_basic.extract_type,
                        &sinker_basic.sink_type,
                        &sinker_basic.db_type,
                        true,
                    )
                    .or_dt_error(DtError::invalid_config(format!(
                        "checker is not supported for [sinker] db_type={} with [extractor] extract_type={} and [sinker] sink_type={}",
                        sinker_basic.db_type, extractor_basic.extract_type, sinker_basic.sink_type
                    )))?,
                )
            };

            if checker_cfg.sample_percent.is_some()
                && !matches!(
                    task_type,
                    Some(TaskType {
                        kind: TaskKind::Snapshot,
                        check: Some(_),
                    }) | Some(TaskType {
                        kind: TaskKind::Cdc,
                        check: Some(CheckMode::Inline),
                    })
                )
            {
                bail!(DtError::invalid_config(format!(
                    "config [checker].{} only supports snapshot check or inline cdc check",
                    SAMPLE_PERCENT
                )));
            }

            if task_type.is_some_and(|task_type| task_type.is_cdc_inline_check()) {
                if !matches!(parallelizer.parallel_type(), ParallelType::RdbMerge) {
                    bail!(DtError::invalid_config(
                        "inline cdc check currently supports only [parallelizer] parallel_type=rdb_merge",
                    ));
                }
                if checker_cfg.inline_check.is_none() {
                    bail!(DtError::invalid_config(
                        "inline cdc check requires [checker_cdc].is_enabled=true",
                    ));
                }
            }
        }
        let resumer = Self::load_resumer_config(loader, &runtime, &sinker_basic)?;
        Ok(Self {
            global: Self::load_global_config(
                loader,
                &extractor_basic,
                &sinker_basic,
                &filter,
                &router,
            )?,
            extractor_basic,
            extractor,
            parallelizer,
            pipeline,
            sinker_basic,
            sinker,
            runtime,
            filter,
            router,
            resumer,
            checker,
            data_marker: Self::load_data_marker_config(loader)?,
            processor: Self::load_processor_config(loader)?,
            meta_center: Self::load_meta_center_config(loader)?,
            tracing: Self::load_tracing_config(loader)?,
            #[cfg(feature = "metrics")]
            metrics: Self::load_metrics_config(loader)?,
        })
    }

    pub fn sink_target(&self) -> Option<BasicSinkerConfig> {
        (!matches!(self.sinker_basic.sink_type, SinkType::Dummy)).then(|| self.sinker_basic.clone())
    }

    pub fn checker_target(&self) -> Option<BasicSinkerConfig> {
        self.checker.as_ref()?;
        self.sink_target()
    }

    pub fn destination_target(&self) -> Option<BasicSinkerConfig> {
        if let Some(target) = self.sink_target() {
            return Some(target);
        }
        self.checker_target()
    }

    pub fn task_type(&self) -> Option<TaskType> {
        if matches!(self.extractor_basic.extract_type, ExtractType::CheckLog) {
            // check_log replays existing check logs, so it stays outside TaskType to skip
            // recorder/recovery/checker-store initialization.
            return None;
        }
        Self::build_task_type(
            &self.extractor_basic.extract_type,
            &self.sinker_basic.sink_type,
            &self.sinker_basic.db_type,
            self.checker.is_some(),
        )
    }

    fn write_sink_supports_inline_checker(target_db_type: &DbType) -> bool {
        matches!(target_db_type, DbType::Mysql | DbType::Pg | DbType::Mongo)
    }

    fn task_kind_from_extract_type(extract_type: &ExtractType) -> Option<TaskKind> {
        match extract_type {
            ExtractType::Struct => Some(TaskKind::Struct),
            ExtractType::Snapshot => Some(TaskKind::Snapshot),
            ExtractType::Cdc => Some(TaskKind::Cdc),
            _ => None,
        }
    }

    fn build_task_type(
        extract_type: &ExtractType,
        sink_type: &SinkType,
        target_db_type: &DbType,
        checker_enabled: bool,
    ) -> Option<TaskType> {
        let kind = Self::task_kind_from_extract_type(extract_type)?;
        let check = if !checker_enabled {
            match (kind, sink_type) {
                (TaskKind::Struct, SinkType::Struct)
                | (TaskKind::Snapshot, SinkType::Write)
                | (TaskKind::Cdc, SinkType::Write) => None,
                _ => return None,
            }
        } else {
            match (kind, sink_type, target_db_type) {
                (TaskKind::Struct, SinkType::Check, DbType::Mysql | DbType::Pg) => {
                    Some(CheckMode::Standalone)
                }
                (
                    TaskKind::Snapshot,
                    SinkType::Check,
                    DbType::Mysql | DbType::Pg | DbType::Mongo,
                ) => Some(CheckMode::Standalone),
                (TaskKind::Snapshot, SinkType::Write, db_type)
                    if Self::write_sink_supports_inline_checker(db_type) =>
                {
                    Some(CheckMode::Inline)
                }
                (TaskKind::Cdc, SinkType::Write, DbType::Mysql | DbType::Pg) => {
                    Some(CheckMode::Inline)
                }
                _ => return None,
            }
        };

        Some(TaskType::new(kind, check))
    }

    fn load_global_config(
        loader: &IniLoader,
        extractor_basic: &BasicExtractorConfig,
        sinker_basic: &BasicSinkerConfig,
        filter: &FilterConfig,
        router: &RouterConfig,
    ) -> anyhow::Result<GlobalConfig> {
        Ok(GlobalConfig {
            task_id: loader.get_with_default(
                GLOBAL,
                "task_id",
                TaskUtil::generate_task_id(extractor_basic, sinker_basic, filter, router),
            )?,
        })
    }

    fn load_extractor_config(
        loader: &IniLoader,
        pipeline: &PipelineConfig,
    ) -> anyhow::Result<(BasicExtractorConfig, ExtractorConfig)> {
        let db_type: DbType = loader.get_required(EXTRACTOR, DB_TYPE)?;
        let extract_type: ExtractType = loader.get_required(EXTRACTOR, "extract_type")?;
        let url: String = loader.get_optional(EXTRACTOR, URL)?;
        let heartbeat_interval_secs: u64 =
            loader.get_with_default(EXTRACTOR, HEARTBEAT_INTERVAL_SECS, 10)?;
        let keepalive_interval_secs: u64 =
            loader.get_with_default(EXTRACTOR, KEEPALIVE_INTERVAL_SECS, 10)?;
        let heartbeat_tb = loader.get_optional(EXTRACTOR, HEARTBEAT_TB)?;
        let mut default_batch_size =
            pipeline.capacity_limiter.buffer_size / Self::load_snapshot_parallel_size(loader)?;
        if default_batch_size == 0 {
            default_batch_size = pipeline.capacity_limiter.buffer_size;
        }
        let batch_size = loader.get_with_default(EXTRACTOR, BATCH_SIZE, default_batch_size)?;
        if batch_size == 0 {
            bail!(DtError::invalid_config(format!(
                "config [extractor].{} must be greater than 0",
                BATCH_SIZE
            )));
        }
        let max_connections =
            loader.get_with_default(EXTRACTOR, MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS)?;
        let connection_timeout_secs = Self::load_connection_timeout_secs(loader, EXTRACTOR)?;

        let connection_auth = ConnectionAuthConfig::from(loader, EXTRACTOR)?;
        let app_name: String = loader.get_with_default(EXTRACTOR, APP_NAME, APE_DTS.to_string())?;
        let app_name_override = loader.contains(EXTRACTOR, APP_NAME);
        let is_direct_connection = if loader.contains(EXTRACTOR, IS_DIRECT_CONNECTION) {
            Some(loader.get_optional(EXTRACTOR, IS_DIRECT_CONNECTION)?)
        } else {
            None
        };

        let rate_limiter = RateLimiterConfig {
            max_rps: loader.get_optional(EXTRACTOR, "max_rps")?,
            max_mbps: loader.get_optional(EXTRACTOR, "max_mbps")?,
        };
        let basic = BasicExtractorConfig {
            db_type: db_type.clone(),
            extract_type: extract_type.clone(),
            url: url.clone(),
            connection_auth: connection_auth.clone(),
            max_connections,
            connection_timeout_secs,
            rate_limiter,
            app_name: if matches!(db_type, DbType::Mssql) && !app_name_override {
                None
            } else {
                Some(app_name.to_owned())
            },
            is_direct_connection,
        };

        let not_supported_err =
            DtError::invalid_config(format!("extract type: {} not supported", extract_type));

        let extractor = match db_type {
            DbType::Mysql => match extract_type {
                ExtractType::Snapshot => ExtractorConfig::MysqlSnapshot {
                    url,
                    connection_auth,
                    db: String::new(),
                    tb: String::new(),
                    db_tbs: HashMap::new(),
                    sample_rate: None,
                    parallel_size: Self::load_snapshot_parallel_size(loader)?,
                    parallel_type: loader.get_with_default(
                        EXTRACTOR,
                        "parallel_type",
                        RdbParallelType::Table,
                    )?,
                    batch_size,
                    partition_cols: loader.get_optional(EXTRACTOR, PARTITION_COLS)?,
                },

                ExtractType::Cdc => ExtractorConfig::MysqlCdc {
                    url,
                    connection_auth,
                    binlog_filename: loader.get_optional(EXTRACTOR, "binlog_filename")?,
                    binlog_position: loader.get_optional(EXTRACTOR, "binlog_position")?,
                    server_id: loader.get_required(EXTRACTOR, "server_id")?,
                    gtid_enabled: loader.get_optional(EXTRACTOR, "gtid_enabled")?,
                    gtid_set: loader.get_optional(EXTRACTOR, "gtid_set")?,
                    binlog_heartbeat_interval_secs: loader.get_with_default(
                        EXTRACTOR,
                        "binlog_heartbeat_interval_secs",
                        10,
                    )?,
                    binlog_timeout_secs: loader.get_with_default(
                        EXTRACTOR,
                        "binlog_timeout_secs",
                        60,
                    )?,
                    heartbeat_interval_secs,
                    heartbeat_tb,
                    keepalive_idle_secs: loader.get_with_default(
                        EXTRACTOR,
                        "keepalive_idle_secs",
                        60,
                    )?,
                    keepalive_interval_secs: loader.get_with_default(
                        EXTRACTOR,
                        "keepalive_interval_secs",
                        10,
                    )?,
                    start_time_utc: loader.get_optional(EXTRACTOR, "start_time_utc")?,
                    end_time_utc: loader.get_optional(EXTRACTOR, "end_time_utc")?,
                },

                ExtractType::CheckLog => ExtractorConfig::MysqlCheck {
                    url,
                    connection_auth,
                    check_log_dir: loader.get_required(EXTRACTOR, CHECK_LOG_DIR)?,
                    batch_size: loader.get_with_default(EXTRACTOR, BATCH_SIZE, 200)?,
                },

                ExtractType::Struct => ExtractorConfig::MysqlStruct {
                    url,
                    connection_auth,
                    db: String::new(),
                    dbs: Vec::new(),
                    db_batch_size: loader.get_with_default(
                        EXTRACTOR,
                        "db_batch_size",
                        DEFAULT_DB_BATCH_SIZE,
                    )?,
                },
                _ => bail! {not_supported_err},
            },

            DbType::Pg => match extract_type {
                ExtractType::Snapshot => ExtractorConfig::PgSnapshot {
                    url,
                    connection_auth,
                    schema: String::new(),
                    tb: String::new(),
                    schema_tbs: HashMap::new(),
                    sample_rate: None,
                    parallel_size: Self::load_snapshot_parallel_size(loader)?,
                    parallel_type: loader.get_with_default(
                        EXTRACTOR,
                        "parallel_type",
                        RdbParallelType::Table,
                    )?,
                    batch_size,
                    partition_cols: loader.get_optional(EXTRACTOR, PARTITION_COLS)?,
                },

                ExtractType::Cdc => ExtractorConfig::PgCdc {
                    url,
                    connection_auth,
                    slot_name: loader.get_required(EXTRACTOR, "slot_name")?,
                    pub_name: loader.get_optional(EXTRACTOR, "pub_name")?,
                    start_lsn: loader.get_optional(EXTRACTOR, "start_lsn")?,
                    recreate_slot_if_exists: loader
                        .get_optional(EXTRACTOR, "recreate_slot_if_exists")?,
                    keepalive_interval_secs,
                    heartbeat_interval_secs,
                    heartbeat_tb,
                    ddl_meta_tb: loader.get_optional(EXTRACTOR, "ddl_meta_tb")?,
                    start_time_utc: loader.get_optional(EXTRACTOR, "start_time_utc")?,
                    end_time_utc: loader.get_optional(EXTRACTOR, "end_time_utc")?,
                },

                ExtractType::CheckLog => ExtractorConfig::PgCheck {
                    url,
                    connection_auth,
                    check_log_dir: loader.get_required(EXTRACTOR, CHECK_LOG_DIR)?,
                    batch_size: loader.get_with_default(EXTRACTOR, BATCH_SIZE, 200)?,
                },

                ExtractType::Struct => ExtractorConfig::PgStruct {
                    url,
                    connection_auth,
                    schema: String::new(),
                    schemas: Vec::new(),
                    do_global_structs: false,
                    db_batch_size: loader.get_with_default(
                        EXTRACTOR,
                        "db_batch_size",
                        DEFAULT_DB_BATCH_SIZE,
                    )?,
                },

                _ => bail! { not_supported_err },
            },

            DbType::Mssql => match extract_type {
                ExtractType::Snapshot => {
                    Self::validate_mssql_connection(
                        EXTRACTOR,
                        &url,
                        &connection_auth,
                        basic.app_name.as_deref(),
                        max_connections,
                    )?;
                    ExtractorConfig::MssqlSnapshot {
                        url,
                        connection_auth,
                        schema: String::new(),
                        tb: String::new(),
                        schema_tbs: HashMap::new(),
                        parallel_size: Self::load_snapshot_parallel_size(loader)?,
                        parallel_type: loader.get_with_default(
                            EXTRACTOR,
                            "parallel_type",
                            RdbParallelType::Table,
                        )?,
                        batch_size,
                        partition_cols: loader.get_optional(EXTRACTOR, PARTITION_COLS)?,
                    }
                }
                _ => bail! { not_supported_err },
            },

            DbType::Mongo => match extract_type {
                ExtractType::Snapshot => {
                    let batch_size = match u32::try_from(batch_size) {
                        std::result::Result::Ok(batch_size) => batch_size,
                        Err(_) => bail! { DtError::invalid_config(format!(
                            "config [{}].{} default value exceeds u32::MAX",
                            EXTRACTOR, BATCH_SIZE,
                        ))},
                    };

                    ExtractorConfig::MongoSnapshot {
                        url,
                        connection_auth,
                        is_direct_connection,
                        app_name,
                        db: String::new(),
                        tb: String::new(),
                        db_tbs: HashMap::new(),
                        parallel_size: Self::load_snapshot_parallel_size(loader)?,
                        parallel_type: loader.get_with_default(
                            EXTRACTOR,
                            "parallel_type",
                            RdbParallelType::Table,
                        )?,
                        batch_size,
                    }
                }

                ExtractType::Cdc => {
                    let source: String = loader.get_with_default(
                        EXTRACTOR,
                        "source",
                        "change_stream".to_string(),
                    )?;
                    ExtractorConfig::MongoCdc {
                        url,
                        connection_auth,
                        is_direct_connection,
                        app_name,
                        resume_token: loader.get_optional(EXTRACTOR, "resume_token")?,
                        start_timestamp: loader.get_optional(EXTRACTOR, "start_timestamp")?,
                        source: MongoCdcSource::parse(&source)?,
                        heartbeat_interval_secs,
                        heartbeat_tb,
                    }
                }

                ExtractType::CheckLog => ExtractorConfig::MongoCheck {
                    url,
                    connection_auth,
                    is_direct_connection,
                    app_name,
                    check_log_dir: loader.get_required(EXTRACTOR, CHECK_LOG_DIR)?,
                    batch_size: loader.get_with_default(EXTRACTOR, BATCH_SIZE, 200)?,
                },

                ExtractType::Struct => ExtractorConfig::MongoStruct {
                    url,
                    connection_auth,
                    is_direct_connection,
                    app_name,
                    db: String::new(),
                    dbs: Vec::new(),
                    db_batch_size: loader.get_with_default(
                        EXTRACTOR,
                        "db_batch_size",
                        DEFAULT_DB_BATCH_SIZE,
                    )?,
                },

                _ => bail! { not_supported_err },
            },

            DbType::Redis => match extract_type {
                ExtractType::Snapshot => {
                    let repl_port = loader.get_with_default(EXTRACTOR, REPL_PORT, 10008)?;
                    ExtractorConfig::RedisSnapshot {
                        url,
                        connection_auth,
                        repl_port,
                        is_cluster: Self::get_is_cluster_config(loader, EXTRACTOR)?,
                    }
                }

                ExtractType::SnapshotFile => ExtractorConfig::RedisSnapshotFile {
                    file_path: loader.get_required(EXTRACTOR, "file_path")?,
                },

                ExtractType::Scan => ExtractorConfig::RedisScan {
                    url,
                    connection_auth,
                    statistic_type: loader.get_required(EXTRACTOR, "statistic_type")?,
                    scan_count: loader.get_with_default(EXTRACTOR, "scan_count", 1000)?,
                },

                ExtractType::Cdc => {
                    let repl_port = loader.get_with_default(EXTRACTOR, REPL_PORT, 10008)?;
                    ExtractorConfig::RedisCdc {
                        url,
                        connection_auth,
                        repl_port,
                        repl_id: loader.get_optional(EXTRACTOR, "repl_id")?,
                        repl_offset: loader.get_optional(EXTRACTOR, "repl_offset")?,
                        keepalive_interval_secs,
                        heartbeat_interval_secs,
                        heartbeat_key: loader.get_optional(EXTRACTOR, "heartbeat_key")?,
                        now_db_id: loader.get_optional(EXTRACTOR, "now_db_id")?,
                        is_cluster: Self::get_is_cluster_config(loader, EXTRACTOR)?,
                    }
                }

                ExtractType::SnapshotAndCdc => {
                    let repl_port = loader.get_with_default(EXTRACTOR, REPL_PORT, 10008)?;
                    ExtractorConfig::RedisSnapshotAndCdc {
                        url,
                        connection_auth,
                        repl_port,
                        repl_id: loader.get_optional(EXTRACTOR, "repl_id")?,
                        keepalive_interval_secs,
                        heartbeat_interval_secs,
                        heartbeat_key: loader.get_optional(EXTRACTOR, "heartbeat_key")?,
                        is_cluster: Self::get_is_cluster_config(loader, EXTRACTOR)?,
                    }
                }

                ExtractType::Reshard => ExtractorConfig::RedisReshard {
                    url,
                    connection_auth,
                },

                _ => bail! { not_supported_err },
            },

            DbType::Kafka => ExtractorConfig::Kafka {
                url,
                group: loader.get_required(EXTRACTOR, "group")?,
                topic: loader.get_required(EXTRACTOR, "topic")?,
                partition: loader.get_optional(EXTRACTOR, "partition")?,
                offset: loader.get_optional(EXTRACTOR, "offset")?,
                ack_interval_secs: loader.get_optional(EXTRACTOR, "ack_interval_secs")?,
            },

            db_type => {
                bail! {DtError::invalid_config(format!(
                    "extractor db type: {} not supported",
                    db_type
                ))}
            }
        };
        Ok((basic, extractor))
    }

    fn load_sinker_config(loader: &IniLoader) -> anyhow::Result<(BasicSinkerConfig, SinkerConfig)> {
        let has_sinker = loader.ini.sections().contains(&SINKER.to_string());
        if !has_sinker {
            bail!(DtError::invalid_config("config [sinker] is required"));
        }

        let sink_type = loader.get_with_default(SINKER, "sink_type", SinkType::Write)?;

        if let SinkType::Dummy = sink_type {
            return Ok((BasicSinkerConfig::default(), SinkerConfig::Dummy));
        }

        let db_type: DbType = loader.get_required(SINKER, DB_TYPE)?;
        let url: String = loader.get_optional(SINKER, URL)?;
        let batch_size: usize = loader.get_with_default(SINKER, BATCH_SIZE, 200)?;
        if batch_size == 0 {
            bail!(DtError::invalid_config(
                "config [sinker].batch_size must be greater than 0"
            ));
        }
        let max_connections =
            loader.get_with_default(SINKER, MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS)?;
        let connection_timeout_secs = Self::load_connection_timeout_secs(loader, SINKER)?;
        let connection_auth = ConnectionAuthConfig::from(loader, SINKER)?;
        let app_name: String = loader.get_with_default(SINKER, APP_NAME, APE_DTS.to_string())?;
        let app_name_override = loader.contains(SINKER, APP_NAME);
        let is_direct_connection = if loader.contains(SINKER, IS_DIRECT_CONNECTION) {
            Some(loader.get_optional(SINKER, IS_DIRECT_CONNECTION)?)
        } else {
            None
        };
        let rate_limiter = RateLimiterConfig {
            max_rps: loader.get_optional(SINKER, "max_rps")?,
            max_mbps: loader.get_optional(SINKER, "max_mbps")?,
        };
        let is_cluster = Self::get_is_cluster_config(loader, SINKER)?;

        let basic = BasicSinkerConfig {
            sink_type: sink_type.clone(),
            db_type: db_type.clone(),
            url: url.clone(),
            connection_auth: connection_auth.clone(),
            batch_size,
            max_connections,
            connection_timeout_secs,
            rate_limiter,
            app_name: if matches!(db_type, DbType::Mssql) && !app_name_override {
                None
            } else {
                Some(app_name.to_owned())
            },
            is_direct_connection,
            is_cluster,
        };

        let conflict_policy: ConflictPolicyEnum =
            loader.get_with_default(SINKER, "conflict_policy", ConflictPolicyEnum::Interrupt)?;

        let not_supported_err =
            DtError::invalid_config(format!("sinker db type: {} not supported", db_type));

        let sinker = match db_type {
            DbType::Mysql | DbType::Tidb => match sink_type {
                SinkType::Write | SinkType::Check => SinkerConfig::Mysql {
                    url,
                    connection_auth,
                    batch_size,
                    replace: loader.get_with_default(SINKER, REPLACE, true)?,
                    disable_foreign_key_checks: loader.get_with_default(
                        SINKER,
                        DISABLE_FOREIGN_KEY_CHECKS,
                        true,
                    )?,
                    transaction_isolation: loader.get_optional(SINKER, "transaction_isolation")?,
                },

                SinkType::Struct => SinkerConfig::MysqlStruct {
                    url,
                    connection_auth,
                    conflict_policy,
                },

                SinkType::Sql => SinkerConfig::Sql {
                    reverse: loader.get_optional(SINKER, REVERSE)?,
                },

                _ => bail! { not_supported_err },
            },

            DbType::Pg => match sink_type {
                SinkType::Write | SinkType::Check => SinkerConfig::Pg {
                    url,
                    connection_auth,
                    batch_size,
                    replace: loader.get_with_default(SINKER, REPLACE, true)?,
                    disable_foreign_key_checks: loader.get_with_default(
                        SINKER,
                        DISABLE_FOREIGN_KEY_CHECKS,
                        true,
                    )?,
                },

                SinkType::Struct => SinkerConfig::PgStruct {
                    url,
                    connection_auth,
                    conflict_policy,
                },

                SinkType::Sql => SinkerConfig::Sql {
                    reverse: loader.get_optional(SINKER, REVERSE)?,
                },

                _ => bail! { not_supported_err },
            },

            DbType::Mssql => match sink_type {
                SinkType::Write => {
                    Self::validate_mssql_connection(
                        SINKER,
                        &url,
                        &connection_auth,
                        basic.app_name.as_deref(),
                        max_connections,
                    )?;
                    SinkerConfig::Mssql {
                        url,
                        connection_auth,
                        batch_size,
                    }
                }
                _ => bail! { not_supported_err },
            },

            DbType::Mongo => match sink_type {
                SinkType::Write | SinkType::Check => SinkerConfig::Mongo {
                    url,
                    connection_auth,
                    is_direct_connection,
                    app_name,
                    batch_size,
                    require_shard_key_filter: loader.get_with_default(
                        SINKER,
                        MONGO_REQUIRE_SHARD_KEY_FILTER,
                        true,
                    )?,
                },

                SinkType::Struct => SinkerConfig::MongoStruct {
                    url,
                    connection_auth,
                    is_direct_connection,
                    app_name,
                    conflict_policy,
                },

                _ => bail! { not_supported_err },
            },

            DbType::Kafka => SinkerConfig::Kafka {
                url,
                batch_size,
                ack_timeout_secs: loader.get_with_default(SINKER, "ack_timeout_secs", 5)?,
                required_acks: loader.get_with_default(
                    SINKER,
                    "required_acks",
                    "one".to_string(),
                )?,
                with_field_defs: loader.get_with_default(SINKER, "with_field_defs", true)?,
            },

            DbType::Redis => match sink_type {
                SinkType::Write => SinkerConfig::Redis {
                    url,
                    connection_auth,
                    batch_size,
                    method: loader.get_optional(SINKER, "method")?,
                    is_cluster,
                },

                SinkType::Statistic => SinkerConfig::RedisStatistic {
                    statistic_type: loader.get_required(SINKER, "statistic_type")?,
                    data_size_threshold: loader.get_optional(SINKER, "data_size_threshold")?,
                    freq_threshold: loader.get_optional(SINKER, "freq_threshold")?,
                    statistic_log_dir: loader.get_optional(SINKER, "statistic_log_dir")?,
                },

                _ => bail! { not_supported_err },
            },

            DbType::StarRocks => match sink_type {
                SinkType::Write => SinkerConfig::StarRocks {
                    url,
                    connection_auth,
                    batch_size,
                    stream_load_url: loader.get_optional(SINKER, "stream_load_url")?,
                    hard_delete: loader.get_optional(SINKER, "hard_delete")?,
                },

                SinkType::Struct => SinkerConfig::StarRocksStruct {
                    url,
                    connection_auth,
                    conflict_policy,
                },

                _ => bail! { not_supported_err },
            },

            DbType::Doris => match sink_type {
                SinkType::Write => SinkerConfig::Doris {
                    url,
                    connection_auth,
                    batch_size,
                    stream_load_url: loader.get_optional(SINKER, "stream_load_url")?,
                },

                SinkType::Struct => SinkerConfig::DorisStruct {
                    url,
                    connection_auth,
                    conflict_policy,
                },

                _ => bail! { not_supported_err },
            },

            DbType::ClickHouse => match sink_type {
                SinkType::Write => SinkerConfig::ClickHouse { url, batch_size },

                SinkType::Struct => SinkerConfig::ClickhouseStruct {
                    url,
                    conflict_policy,
                    engine: loader.get_with_default(
                        SINKER,
                        "engine",
                        "ReplacingMergeTree".to_string(),
                    )?,
                },

                _ => bail! { not_supported_err },
            },
        };
        Ok((basic, sinker))
    }

    fn load_parallelizer_config(
        loader: &IniLoader,
        sinker_basic: &BasicSinkerConfig,
        _pipeline: &PipelineConfig,
    ) -> anyhow::Result<ParallelizerConfig> {
        let parallel_size = loader.get_with_default(PARALLELIZER, PARALLEL_SIZE, 1)?;
        let parallel_type =
            loader.get_with_default(PARALLELIZER, "parallel_type", ParallelType::Serial)?;
        if !matches!(parallel_type, ParallelType::Snapshot) {
            return Ok(ParallelizerConfig::Basic {
                parallel_size,
                parallel_type,
            });
        }

        let default_rebalance = ChunkPartitionerRebalanceConfig::default();
        // Keep sink-side partitions large enough to preserve sinker batch efficiency by default.
        let min_partition_rows = loader.get_with_default(
            PARALLELIZER,
            REBALANCE_MIN_PARTITION_ROWS,
            if sinker_basic.batch_size > 0 {
                sinker_basic.batch_size
            } else {
                default_rebalance.min_partition_rows
            },
        )?;
        if min_partition_rows == 0 {
            bail!(DtError::invalid_config(format!(
                "config [parallelizer].{} must be greater than 0",
                REBALANCE_MIN_PARTITION_ROWS
            )));
        }

        let max_partitions_per_sinker = loader.get_with_default(
            PARALLELIZER,
            REBALANCE_MAX_PARTITIONS_PER_SINKER,
            default_rebalance.max_partitions_per_sinker,
        )?;
        if max_partitions_per_sinker == 0 {
            bail!(DtError::invalid_config(format!(
                "config [parallelizer].{} must be greater than 0",
                REBALANCE_MAX_PARTITIONS_PER_SINKER
            )));
        }

        let split_skew_ratio = loader.get_with_default(
            PARALLELIZER,
            REBALANCE_SPLIT_SKEW_RATIO,
            default_rebalance.split_skew_ratio,
        )?;
        if split_skew_ratio <= 0.0 {
            bail!(DtError::invalid_config(format!(
                "config [parallelizer].{} must be greater than 0",
                REBALANCE_SPLIT_SKEW_RATIO
            )));
        }

        Ok(ParallelizerConfig::Snapshot {
            parallel_size,
            chunk_partitioner_rebalance: ChunkPartitionerRebalanceConfig {
                strategy: loader.get_with_default(
                    PARALLELIZER,
                    REBALANCE_STRATEGY,
                    ChunkPartitionerRebalanceStrategy::None,
                )?,
                cost: loader.get_with_default(
                    PARALLELIZER,
                    REBALANCE_COST,
                    ChunkPartitionerRebalanceCost::Rows,
                )?,
                max_partitions_per_sinker,
                min_partition_rows,
                split_skew_ratio,
            },
        })
    }

    fn load_pipeline_config(loader: &IniLoader) -> anyhow::Result<PipelineConfig> {
        let capacity_limiter = CapacityLimiterConfig {
            buffer_size: loader.get_with_default(PIPELINE, "buffer_size", 16000)?,
            buffer_memory_mb: loader.get_optional(PIPELINE, "buffer_memory_mb")?,
        };
        let mut config = PipelineConfig {
            capacity_limiter,
            checkpoint_interval_secs: loader.get_with_default(
                PIPELINE,
                "checkpoint_interval_secs",
                10,
            )?,
            batch_sink_interval_secs: loader.get_optional(PIPELINE, "batch_sink_interval_secs")?,
            counter_time_window_secs: loader.get_optional(PIPELINE, "counter_time_window_secs")?,
            counter_max_sub_count: loader.get_with_default(
                PIPELINE,
                "counter_max_sub_count",
                1000,
            )?,
        };

        if config.counter_time_window_secs == 0 {
            config.counter_time_window_secs = config.checkpoint_interval_secs;
        }
        Ok(config)
    }

    fn load_checker_config(
        loader: &IniLoader,
        extract_type: &ExtractType,
        sinker_basic: &BasicSinkerConfig,
    ) -> anyhow::Result<Option<CheckerConfig>> {
        if loader.contains(CHECKER, LEGACY_CHECKER_ENABLE) {
            bail!(DtError::invalid_config(
                "config [checker].enable is no longer supported; use [sinker].sink_type=check for standalone check, [checker] for snapshot inline check, or [checker_cdc].is_enabled=true for CDC inline check",
            ));
        }
        let has_checker = loader.ini.sections().contains(&CHECKER.to_string());
        let cdc_enabled = loader.ini.sections().contains(&CHECKER_CDC.to_string())
            && loader.get_with_default(CHECKER_CDC, IS_ENABLED, false)?;
        let standalone = matches!(sinker_basic.sink_type, SinkType::Check);
        let snapshot_inline = has_checker
            && matches!(extract_type, ExtractType::Snapshot)
            && matches!(sinker_basic.sink_type, SinkType::Write);

        if !standalone && !snapshot_inline && !cdc_enabled {
            return Ok(None);
        }

        if cdc_enabled
            && (!matches!(extract_type, ExtractType::Cdc)
                || !matches!(sinker_basic.sink_type, SinkType::Write))
        {
            bail!(DtError::invalid_config(
                "config [checker_cdc].is_enabled=true requires [extractor] extract_type=cdc and [sinker] sink_type=write",
            ));
        }
        if matches!(extract_type, ExtractType::Cdc) && !cdc_enabled {
            return Ok(None);
        }
        if sinker_basic.url.is_empty() {
            bail!(DtError::invalid_config(
                "checker target requires non-empty [sinker].url",
            ));
        }

        let default = CheckerConfig::default();
        let sample_percent = match loader.ini.get(CHECKER, SAMPLE_PERCENT) {
            Some(raw) if !raw.is_empty() => {
                let sample_percent = raw.parse::<usize>().map_err(|_| {
                    DtError::invalid_config(format!(
                        "config [checker].{}={}, can not be parsed as usize",
                        SAMPLE_PERCENT, raw
                    ))
                })?;
                if !(1..=100).contains(&sample_percent) {
                    bail!(DtError::invalid_config(format!(
                        "config [checker].sample_percent must be between 1 and 100, got {}",
                        sample_percent
                    )));
                }
                Some(sample_percent as u8)
            }
            _ => None,
        };

        let output_default = CheckerOutputConfig::default();
        let recheck_queue_size =
            loader.get_with_default(CHECKER, RECHECK_QUEUE_SIZE, default.recheck_queue_size)?;
        if recheck_queue_size == 0 {
            bail!(DtError::invalid_config(
                "config [checker].recheck_queue_size must be greater than 0",
            ));
        }
        let recheck_queue_memory_mb = loader.get_with_default(
            CHECKER,
            RECHECK_QUEUE_MEMORY_MB,
            default.recheck_queue_memory_mb,
        )?;
        if recheck_queue_memory_mb == 0 {
            bail!(DtError::invalid_config(
                "config [checker].recheck_queue_memory_mb must be greater than 0",
            ));
        }

        let inline_check = if cdc_enabled {
            let inline_default = InlineCheckConfig::default();
            Some(InlineCheckConfig {
                queue_size: loader.get_with_default(
                    CHECKER_CDC,
                    CHECKER_QUEUE_SIZE,
                    inline_default.queue_size,
                )?,
                check_log_interval_secs: loader.get_with_default(
                    CHECKER_CDC,
                    CHECK_LOG_INTERVAL_SECS,
                    inline_default.check_log_interval_secs,
                )?,
            })
        } else {
            None
        };

        Ok(Some(CheckerConfig {
            batch_size: loader.get_with_default(CHECKER, BATCH_SIZE, default.batch_size)?,
            sample_percent,
            recheck_count: loader.get_with_default(
                CHECKER,
                RECHECK_COUNT,
                default.recheck_count,
            )?,
            recheck_interval_secs: loader.get_with_default(
                CHECKER,
                RECHECK_INTERVAL_SECS,
                default.recheck_interval_secs,
            )?,
            recheck_queue_size,
            recheck_queue_memory_mb,
            output: CheckerOutputConfig {
                output_full_row: loader.get_with_default(
                    CHECKER_OUTPUT,
                    OUTPUT_FULL_ROW,
                    output_default.output_full_row,
                )?,
                output_revise_sql: loader.get_with_default(
                    CHECKER_OUTPUT,
                    OUTPUT_REVISE_SQL,
                    output_default.output_revise_sql,
                )?,
                revise_match_full_row: loader.get_with_default(
                    CHECKER_OUTPUT,
                    REVISE_MATCH_FULL_ROW,
                    output_default.revise_match_full_row,
                )?,
                log_dir: loader.get_optional(CHECKER_OUTPUT, CHECK_LOG_DIR)?,
                log_file_size: loader.get_with_default(
                    CHECKER_OUTPUT,
                    CHECK_LOG_FILE_SIZE,
                    DEFAULT_CHECK_LOG_FILE_SIZE.to_string(),
                )?,
                log_max_rows: loader.get_with_default(
                    CHECKER_OUTPUT,
                    CHECK_LOG_MAX_ROWS,
                    super::checker_config::DEFAULT_CHECK_LOG_MAX_ROWS,
                )?,
                output_type: CheckerOutputType::Logs,
            },
            inline_check,
        }))
    }

    fn load_runtime_config(loader: &IniLoader) -> anyhow::Result<RuntimeConfig> {
        Ok(RuntimeConfig {
            log_level: loader.get_with_default(RUNTIME, "log_level", "info".to_string())?,
            log_dir: loader.get_with_default(RUNTIME, "log_dir", "./logs".to_string())?,
            log4rs_file: loader.get_with_default(
                RUNTIME,
                "log4rs_file",
                "./log4rs.yaml".to_string(),
            )?,
            check_result_stdout_only: loader.get_with_default(
                RUNTIME,
                "check_result_stdout_only",
                false,
            )?,
        })
    }

    fn load_tracing_config(loader: &IniLoader) -> anyhow::Result<TracingConfig> {
        let default = TracingConfig::default();
        Ok(TracingConfig {
            task_summary_mode: loader.get_with_default(
                TRACING,
                TASK_SUMMARY_MODE,
                default.task_summary_mode,
            )?,
            output_format: loader.get_with_default(
                TRACING,
                OUTPUT_FORMAT,
                default.output_format,
            )?,
        })
    }

    fn load_snapshot_parallel_size(loader: &IniLoader) -> anyhow::Result<usize> {
        Ok(if loader.contains(EXTRACTOR, PARALLEL_SIZE) {
            loader.get_with_default(EXTRACTOR, PARALLEL_SIZE, 1)?
        } else {
            loader.get_with_default(RUNTIME, LEGACY_TB_PARALLEL_SIZE, 1)?
        })
    }

    fn load_filter_config(loader: &IniLoader) -> anyhow::Result<FilterConfig> {
        Ok(FilterConfig {
            do_schemas: loader.get_optional(FILTER, "do_dbs")?,
            ignore_schemas: loader.get_optional(FILTER, "ignore_dbs")?,
            do_tbs: loader.get_optional(FILTER, "do_tbs")?,
            ignore_tbs: loader.get_optional(FILTER, "ignore_tbs")?,
            ignore_cols: loader.get_optional(FILTER, "ignore_cols")?,
            do_events: loader.get_with_default(FILTER, "do_events", ASTRISK.to_string())?,
            do_ddls: loader.get_optional(FILTER, "do_ddls")?,
            do_dcls: loader.get_optional(FILTER, "do_dcls")?,
            do_structures: loader.get_with_default(FILTER, "do_structures", ASTRISK.to_string())?,
            ignore_cmds: loader.get_optional(FILTER, "ignore_cmds")?,
            where_conditions: loader.get_optional(FILTER, "where_conditions")?,
        })
    }

    fn load_router_config(loader: &IniLoader) -> anyhow::Result<RouterConfig> {
        Ok(RouterConfig::Rdb {
            schema_map: loader.get_optional(ROUTER, "db_map")?,
            tb_map: loader.get_optional(ROUTER, "tb_map")?,
            col_map: loader.get_optional(ROUTER, "col_map")?,
            topic_map: loader.get_optional(ROUTER, "topic_map")?,
        })
    }

    fn load_resumer_config(
        loader: &IniLoader,
        runtime: &RuntimeConfig,
        sinker_basic: &BasicSinkerConfig,
    ) -> anyhow::Result<ResumerConfig> {
        let legacy_keys = ["resume_from_log", "resume_log_dir", "resume_config_file"]
            .into_iter()
            .filter(|key| loader.contains(RESUMER, key))
            .collect::<Vec<_>>();
        if !legacy_keys.is_empty() {
            bail!(DtError::invalid_config(format!(
                "legacy [resumer] configs {} are no longer supported; migrate to resume_type=from_log, log_dir, and config_file",
                legacy_keys.join(", ")
            )));
        }

        let resume_type = loader.get_with_default(RESUMER, RESUME_TYPE, ResumeType::Dummy)?;

        match resume_type {
            ResumeType::FromLog => Ok(ResumerConfig::FromLog {
                log_dir: loader.get_with_default(RESUMER, "log_dir", runtime.log_dir.clone())?,
                config_file: loader.get_optional(RESUMER, "config_file")?,
            }),
            ResumeType::FromTarget => Ok(ResumerConfig::FromDB {
                url: sinker_basic.url.clone(),
                connection_auth: sinker_basic.connection_auth.clone(),
                db_type: sinker_basic.db_type.clone(),
                table_full_name: loader.get_optional(RESUMER, "table_full_name")?,
                max_connections: loader.get_with_default(
                    RESUMER,
                    MAX_CONNECTIONS,
                    RESUMER_CONNECTION_LIMIT_DEFAULT,
                )?,
                is_direct_connection: sinker_basic.is_direct_connection,
            }),
            ResumeType::FromDB => {
                let is_direct_connection = if loader.contains(RESUMER, IS_DIRECT_CONNECTION) {
                    Some(loader.get_optional(RESUMER, IS_DIRECT_CONNECTION)?)
                } else {
                    None
                };
                Ok(ResumerConfig::FromDB {
                    url: loader.get_required(RESUMER, URL)?,
                    connection_auth: ConnectionAuthConfig::from(loader, RESUMER)?,
                    db_type: loader.get_required(RESUMER, DB_TYPE)?,
                    table_full_name: loader.get_optional(RESUMER, "table_full_name")?,
                    max_connections: loader.get_with_default(
                        RESUMER,
                        MAX_CONNECTIONS,
                        RESUMER_CONNECTION_LIMIT_DEFAULT,
                    )?,
                    is_direct_connection,
                })
            }
            _ => Ok(ResumerConfig::Dummy),
        }
    }

    fn load_data_marker_config(loader: &IniLoader) -> anyhow::Result<Option<DataMarkerConfig>> {
        if !loader.ini.sections().contains(&DATA_MARKER.to_string()) {
            return Ok(None);
        }

        Ok(Some(DataMarkerConfig {
            topo_name: loader.get_required(DATA_MARKER, "topo_name")?,
            topo_nodes: loader.get_optional(DATA_MARKER, "topo_nodes")?,
            src_node: loader.get_required(DATA_MARKER, "src_node")?,
            dst_node: loader.get_required(DATA_MARKER, "dst_node")?,
            do_nodes: loader.get_required(DATA_MARKER, "do_nodes")?,
            ignore_nodes: loader.get_optional(DATA_MARKER, "ignore_nodes")?,
            marker: loader.get_required(DATA_MARKER, "marker")?,
        }))
    }

    fn load_processor_config(loader: &IniLoader) -> anyhow::Result<Option<ProcessorConfig>> {
        if !loader.ini.sections().contains(&PROCESSOR.to_string()) {
            return Ok(None);
        }

        let lua_code_file: String = loader.get_optional(PROCESSOR, "lua_code_file")?;
        let mut lua_code = String::new();

        if !lua_code_file.is_empty() {
            lua_code = fs::read_to_string(&lua_code_file).map_err(|error| {
                let context = if error.kind() == ErrorKind::NotFound {
                    DtError::MissingConfig(format!("path: {lua_code_file}"))
                } else {
                    DtError::IoFailed(format!(
                        "failed to read processor Lua file: {lua_code_file}"
                    ))
                };
                Error::new(error).context(context)
            })?;
        }

        Ok(Some(ProcessorConfig {
            lua_code_file,
            lua_code,
        }))
    }

    fn load_meta_center_config(loader: &IniLoader) -> anyhow::Result<Option<MetaCenterConfig>> {
        let mut config = MetaCenterConfig::Basic;
        let db_type: DbType = loader.get_required(EXTRACTOR, DB_TYPE)?;
        let meta_type = loader.get_with_default(META_CENTER, "type", MetaCenterType::Basic)?;
        if meta_type == MetaCenterType::DbEngine && db_type == DbType::Mysql {
            let extractor_url: String = loader.get_required(EXTRACTOR, URL)?;
            let target_url: String = if loader.ini.sections().contains(&SINKER.to_string()) {
                let sink_type = loader.get_with_default(SINKER, "sink_type", SinkType::Write)?;
                if matches!(sink_type, SinkType::Dummy) {
                    loader.get_optional(CHECKER, URL)?
                } else {
                    loader.get_required(SINKER, URL)?
                }
            } else {
                loader.get_optional(CHECKER, URL)?
            };
            let meta_center_url: String = loader.get_required(META_CENTER, URL)?;
            if extractor_url == meta_center_url || target_url == meta_center_url {
                bail!(DtError::invalid_config(format!(
                    "config, [{}].{} should be different with [{}].{} and [{}].{}",
                    META_CENTER, URL, EXTRACTOR, URL, SINKER, URL
                )));
            }

            config = MetaCenterConfig::MySqlDbEngine {
                url: meta_center_url,
                connection_auth: ConnectionAuthConfig::from(loader, META_CENTER)?,
                ddl_conflict_policy: loader.get_with_default(
                    META_CENTER,
                    DDL_CONFLICT_POLICY,
                    ConflictPolicyEnum::Interrupt,
                )?,
            }
        }
        Ok(Some(config))
    }

    fn get_is_cluster_config(loader: &IniLoader, section: &str) -> anyhow::Result<Option<bool>> {
        let key = "is_cluster";
        Ok(match loader.ini.get(section, key) {
            Some(value) if !value.trim().is_empty() => Some(loader.get_optional(section, key)?),
            _ => None,
        })
    }

    fn validate_mssql_connection(
        section: &str,
        connection_string: &str,
        connection_auth: &ConnectionAuthConfig,
        application_name: Option<&str>,
        max_connections: u32,
    ) -> anyhow::Result<()> {
        if max_connections == 0 {
            bail!(DtError::invalid_config(format!(
                "config [{}].{} must be greater than 0",
                section, MAX_CONNECTIONS
            )));
        }
        MssqlConnectionPool::build_client_config(
            connection_string,
            connection_auth,
            application_name,
        )
        .dt_error(DtError::invalid_config(format!(
            "config [{}].{} is not a valid MSSQL connection configuration",
            section, URL
        )))?;
        Ok(())
    }

    fn load_connection_timeout_secs(loader: &IniLoader, section: &str) -> anyhow::Result<u64> {
        let connection_timeout_secs =
            loader.get_with_default(section, CONNECTION_TIMEOUT_SECS, 15)?;
        if connection_timeout_secs == 0 {
            bail!(DtError::invalid_config(format!(
                "config [{}].{} must be greater than 0",
                section, CONNECTION_TIMEOUT_SECS
            )));
        }
        Ok(connection_timeout_secs)
    }

    #[cfg(feature = "metrics")]
    fn load_metrics_config(loader: &IniLoader) -> anyhow::Result<MetricsConfig> {
        let metrics_section = "metrics";
        let labels_str: String = loader.get_optional(metrics_section, "labels")?;
        let mut metrics_labels = HashMap::new();
        if !labels_str.is_empty() {
            for label_pair in labels_str.split(',') {
                if let Some((key, value)) = label_pair.trim().split_once('=') {
                    metrics_labels.insert(key.trim().to_string(), value.trim().to_string());
                }
            }
        }
        Ok(MetricsConfig {
            http_host: loader.get_with_default(
                metrics_section,
                "http_host",
                "0.0.0.0".to_string(),
            )?,
            http_port: loader.get_with_default(metrics_section, "http_port", 9090)?,
            workers: loader.get_with_default(metrics_section, "workers", 2)?,
            metrics_labels,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env::temp_dir,
        fs,
        path::PathBuf,
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use crate::config::parallelizer_config::{
        ChunkPartitionerRebalanceCost, ChunkPartitionerRebalanceStrategy,
    };
    use crate::error::{ErrorCode, ErrorReport};
    use crate::runtime_trace::{TaskSummaryMode, TraceOutputFormat};

    use super::{
        CheckMode, DbType, ExtractorConfig, ParallelType, RdbParallelType, SinkerConfig,
        TaskConfig, TaskKind, TaskType,
    };

    static NEXT_CONFIG_ID: AtomicU64 = AtomicU64::new(0);

    fn cdc_inline_check_config(
        parallel_type: &str,
        extra_checker: &str,
        extra_cdc: &str,
        extra_output: &str,
    ) -> String {
        format!(
            r#"[extractor]
db_type=mysql
extract_type=cdc
url=mysql://127.0.0.1:3306
server_id=1

[sinker]
db_type=mysql
sink_type=write
url=mysql://127.0.0.1:3307

[checker]
batch_size=2
{extra_checker}

[checker_cdc]
is_enabled=true
{extra_cdc}

[checker_output]
{extra_output}

[parallelizer]
parallel_type={parallel_type}
"#
        )
    }

    fn write_temp_task_config(contents: &str) -> PathBuf {
        let unique_id = NEXT_CONFIG_ID.fetch_add(1, Ordering::Relaxed);
        let path = temp_dir().join(format!(
            "ape_dts_task_config_{}_{}.ini",
            process::id(),
            unique_id
        ));
        fs::write(&path, contents).unwrap();
        path
    }

    fn load_temp_task_config(contents: &str) -> anyhow::Result<TaskConfig> {
        let config_path = write_temp_task_config(contents);
        let result = TaskConfig::new(config_path.to_str().unwrap());
        fs::remove_file(config_path).unwrap();
        result
    }

    fn snapshot_check_config(extra_checker: &str) -> String {
        format!(
            r#"[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://127.0.0.1:3306

[sinker]
db_type=mysql
sink_type=check
url=mysql://127.0.0.1:3307
max_connections=3

[checker]
{extra_checker}

[parallelizer]
parallel_type=rdb_merge
"#
        )
    }

    fn basic_snapshot_config(extra_config: &str) -> String {
        format!(
            r#"[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://127.0.0.1:3306

[sinker]
db_type=mysql
sink_type=write
url=mysql://127.0.0.1:3307

[parallelizer]
parallel_type=rdb_merge

{extra_config}
"#
        )
    }

    #[test]
    fn tracing_config_defaults_to_marker_summary_mode() {
        let config = load_temp_task_config(&basic_snapshot_config(""))
            .expect("default tracing config should be valid");

        assert_eq!(config.tracing.task_summary_mode, TaskSummaryMode::Marker);
        assert_eq!(config.tracing.output_format, TraceOutputFormat::Plain);
    }

    #[test]
    fn tracing_config_loads_marker_summary_mode_and_json_output() {
        let config = load_temp_task_config(&basic_snapshot_config(
            r#"[tracing]
task_summary_mode=marker
output_format=json
"#,
        ))
        .expect("tracing config should be valid");

        assert_eq!(config.tracing.task_summary_mode, TaskSummaryMode::Marker);
        assert_eq!(config.tracing.output_format, TraceOutputFormat::Json);
    }

    #[test]
    fn redis_empty_is_cluster_keeps_auto_detection() {
        let config = load_temp_task_config(
            r#"[extractor]
db_type=redis
extract_type=cdc
url=redis://127.0.0.1:6379
is_cluster=

[sinker]
db_type=redis
sink_type=write
url=redis://127.0.0.1:6380
is_cluster=

[parallelizer]
parallel_type=redis
"#,
        )
        .unwrap();

        match config.extractor {
            ExtractorConfig::RedisCdc { is_cluster, .. } => assert_eq!(is_cluster, None),
            _ => panic!("expected redis cdc extractor"),
        }
        match config.sinker {
            SinkerConfig::Redis { is_cluster, .. } => assert_eq!(is_cluster, None),
            _ => panic!("expected redis sinker"),
        }
    }

    #[test]
    fn checker_accepts_supported_configs() {
        let config = load_temp_task_config(&cdc_inline_check_config(
            "rdb_merge",
            "",
            "queue_size=16",
            "",
        ))
        .expect("cdc inline checker config should be valid");
        assert_eq!(
            config.task_type(),
            Some(TaskType::new(TaskKind::Cdc, Some(CheckMode::Inline)))
        );
        assert_eq!(
            config
                .checker
                .as_ref()
                .and_then(|checker| checker.inline_check.as_ref())
                .expect("cdc inline config should exist")
                .queue_size,
            16
        );

        let config = load_temp_task_config(
            r#"[extractor]
db_type=mongo
extract_type=snapshot
url=mongodb://127.0.0.1:27017

[sinker]
db_type=mongo
sink_type=check
url=mongodb://127.0.0.1:27018

[checker]
batch_size=20

[parallelizer]
parallel_type=mongo
"#,
        )
        .expect("mongo standalone snapshot checker config should be valid");
        assert_eq!(
            config.task_type(),
            Some(TaskType::new(
                TaskKind::Snapshot,
                Some(CheckMode::Standalone)
            ))
        );
        assert_eq!(
            config.checker_target().expect("checker target").url,
            "mongodb://127.0.0.1:27018"
        );

        for (config, sample_percent) in [
            (snapshot_check_config("sample_percent=25"), 25),
            (
                cdc_inline_check_config("rdb_merge", "sample_percent=10", "", ""),
                10,
            ),
        ] {
            let checker = load_temp_task_config(&config)
                .expect("sampled checker config should be valid")
                .checker
                .expect("checker should exist");
            assert_eq!(checker.sample_percent, Some(sample_percent));
        }

        let checker = load_temp_task_config(
            r#"[extractor]
db_type=mysql
extract_type=struct
url=mysql://127.0.0.1:3306

[sinker]
db_type=mysql
sink_type=check
url=mysql://127.0.0.1:3307

[checker]
sample_percent=
"#,
        )
        .expect("empty sample_percent should be ignored")
        .checker
        .expect("checker should exist");
        assert_eq!(checker.sample_percent, None);

        let config = load_temp_task_config(&basic_snapshot_config(
            r#"[checker]
batch_size=10

[checker_output]
output_type=logs
check_log_dir=/tmp/ape-dts-check
output_full_row=true
"#,
        ))
        .expect("inline snapshot checker config should be valid");
        assert_eq!(
            config.task_type(),
            Some(TaskType::new(TaskKind::Snapshot, Some(CheckMode::Inline)))
        );
        let checker = config.checker.expect("checker should exist");
        assert_eq!(checker.log_dir(), "/tmp/ape-dts-check");
        assert!(checker.output.output_full_row);
    }

    #[test]
    fn checker_rejects_invalid_configs() {
        for (config, expected_err) in [
            (
                cdc_inline_check_config("serial", "", "", ""),
                "inline cdc check currently supports only [parallelizer] parallel_type=rdb_merge",
            ),
            (
                r#"[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://127.0.0.1:3306

[sinker]
db_type=mysql
sink_type=write
url=mysql://127.0.0.1:3307

[checker_cdc]
is_enabled=true
"#
                .to_string(),
                "config [checker_cdc].is_enabled=true requires [extractor] extract_type=cdc and [sinker] sink_type=write",
            ),
            (
                r#"[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://127.0.0.1:3306

[checker]
batch_size=2
"#
                .to_string(),
                "config [sinker] is required",
            ),
            (
                r#"[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://127.0.0.1:3306

[sinker]
db_type=mysql
sink_type=write
url=mysql://127.0.0.1:3307

[checker]
enable=true
"#
                .to_string(),
                "config [checker].enable is no longer supported; use [sinker].sink_type=check for standalone check, [checker] for snapshot inline check, or [checker_cdc].is_enabled=true for CDC inline check",
            ),
            (
                snapshot_check_config("sample_percent=0"),
                "config [checker].sample_percent must be between 1 and 100, got 0",
            ),
            (
                snapshot_check_config("recheck_queue_size=0"),
                "config [checker].recheck_queue_size must be greater than 0",
            ),
            (
                snapshot_check_config("recheck_queue_memory_mb=0"),
                "config [checker].recheck_queue_memory_mb must be greater than 0",
            ),
            (
                r#"[extractor]
db_type=mysql
extract_type=struct
url=mysql://127.0.0.1:3306

[sinker]
db_type=mysql
sink_type=check
url=mysql://127.0.0.1:3307

[checker]
sample_percent=10
"#
                .to_string(),
                "config [checker].sample_percent only supports snapshot check or inline cdc check",
            ),
        ] {
            let error = load_temp_task_config(&config).err().unwrap();
            assert_eq!(ErrorReport::from_anyhow(&error).code, ErrorCode::InvalidConfig);
            assert_eq!(error.root_cause().to_string(), expected_err);
        }
    }

    #[test]
    fn parallelizer_rebalance_config_uses_none_defaults() {
        let config_path = write_temp_task_config(
            r#"[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://127.0.0.1:3306

[sinker]
db_type=mysql
sink_type=write
url=mysql://127.0.0.1:3307
batch_size=128

[parallelizer]
parallel_type=snapshot
parallel_size=2
"#,
        );
        let config = TaskConfig::new(config_path.to_str().unwrap()).unwrap();
        fs::remove_file(config_path).unwrap();

        let rebalance = config
            .parallelizer
            .chunk_partitioner_rebalance()
            .expect("snapshot parallelizer should have rebalance config");
        assert_eq!(rebalance.strategy, ChunkPartitionerRebalanceStrategy::None);
        assert_eq!(rebalance.cost, ChunkPartitionerRebalanceCost::Rows);
        assert_eq!(rebalance.max_partitions_per_sinker, 2);
        assert_eq!(rebalance.min_partition_rows, 128);
        assert_eq!(rebalance.split_skew_ratio, 1.0);
    }

    #[test]
    fn snapshot_extractor_default_batch_size_is_at_least_one() {
        let config_path = write_temp_task_config(
            r#"[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://127.0.0.1:3306
parallel_size=32

[sinker]
db_type=mysql
sink_type=write
url=mysql://127.0.0.1:3307

[pipeline]
buffer_size=4
"#,
        );
        let config = TaskConfig::new(config_path.to_str().unwrap()).unwrap();
        fs::remove_file(config_path).unwrap();

        match config.extractor {
            ExtractorConfig::MysqlSnapshot { batch_size, .. } => {
                assert_eq!(batch_size, 4);
            }
            _ => panic!("expected mysql snapshot extractor config"),
        }
    }

    #[test]
    fn basic_configs_store_connection_timeout() {
        let config = load_temp_task_config(
            r#"[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://127.0.0.1:3306
connection_timeout_secs=7

[sinker]
db_type=mysql
sink_type=write
url=mysql://127.0.0.1:3307
connection_timeout_secs=9
"#,
        )
        .expect("basic connection timeout config should be accepted");

        assert_eq!(config.extractor_basic.connection_timeout_secs, 7);
        assert_eq!(config.sinker_basic.connection_timeout_secs, 9);
    }

    #[test]
    fn snapshot_extractor_batch_size_must_be_greater_than_zero() {
        let result = load_temp_task_config(
            r#"[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://127.0.0.1:3306
batch_size=0

[sinker]
db_type=mysql
sink_type=write
url=mysql://127.0.0.1:3307
"#,
        );

        let error = result.err().unwrap();
        assert_eq!(
            error.root_cause().to_string(),
            "config [extractor].batch_size must be greater than 0"
        );
    }

    #[test]
    fn parallelizer_rebalance_config_loads_explicit_values() {
        let config_path = write_temp_task_config(
            r#"[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://127.0.0.1:3306

[sinker]
db_type=mysql
sink_type=write
url=mysql://127.0.0.1:3307

[parallelizer]
parallel_type=snapshot
parallel_size=8
rebalance_strategy=auto_split
rebalance_cost=rows
rebalance_max_partitions_per_sinker=3
rebalance_min_partition_rows=64
rebalance_split_skew_ratio=1.5
"#,
        );
        let config = TaskConfig::new(config_path.to_str().unwrap()).unwrap();
        fs::remove_file(config_path).unwrap();

        let rebalance = config
            .parallelizer
            .chunk_partitioner_rebalance()
            .expect("snapshot parallelizer should have rebalance config");
        assert_eq!(
            rebalance.strategy,
            ChunkPartitionerRebalanceStrategy::AutoSplit
        );
        assert_eq!(rebalance.cost, ChunkPartitionerRebalanceCost::Rows);
        assert_eq!(rebalance.max_partitions_per_sinker, 3);
        assert_eq!(rebalance.min_partition_rows, 64);
        assert_eq!(rebalance.split_skew_ratio, 1.5);
    }

    #[test]
    fn parallelizer_basic_config_has_no_rebalance_config() {
        let config_path = write_temp_task_config(
            r#"[extractor]
db_type=mysql
extract_type=struct
url=mysql://127.0.0.1:3306

[sinker]
sink_type=dummy

[parallelizer]
parallel_type=rdb_merge
parallel_size=4
rebalance_strategy=auto_split
"#,
        );
        let config = TaskConfig::new(config_path.to_str().unwrap()).unwrap();
        fs::remove_file(config_path).unwrap();

        assert!(matches!(
            config.parallelizer.parallel_type(),
            ParallelType::RdbMerge
        ));
        assert_eq!(config.parallelizer.parallel_size(), 4);
        assert!(config.parallelizer.chunk_partitioner_rebalance().is_none());
    }

    #[test]
    fn parallelizer_rebalance_max_partitions_per_sinker_must_be_greater_than_zero() {
        let config_path = write_temp_task_config(
            r#"[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://127.0.0.1:3306

[sinker]
db_type=mysql
sink_type=write
url=mysql://127.0.0.1:3307

[parallelizer]
parallel_type=snapshot
rebalance_max_partitions_per_sinker=0
"#,
        );
        let err = TaskConfig::new(config_path.to_str().unwrap())
            .err()
            .unwrap();
        fs::remove_file(config_path).unwrap();

        assert_eq!(
            err.root_cause().to_string(),
            "config [parallelizer].rebalance_max_partitions_per_sinker must be greater than 0"
        );
    }

    #[test]
    fn sinker_batch_size_must_be_greater_than_zero() {
        let config_path = write_temp_task_config(
            r#"[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://127.0.0.1:3306

[sinker]
db_type=mysql
sink_type=write
url=mysql://127.0.0.1:3307
batch_size=0
"#,
        );
        let result = TaskConfig::new(config_path.to_str().unwrap());
        fs::remove_file(config_path).unwrap();

        match result {
            Err(err) => {
                assert_eq!(
                    err.root_cause().to_string(),
                    "config [sinker].batch_size must be greater than 0"
                );
            }
            Ok(_) => panic!("expected config validation error"),
        }
    }

    #[test]
    fn mssql_snapshot_config_accepts_chunk_and_partition_cols() {
        let config = load_temp_task_config(
            r#"[extractor]
db_type=mssql
extract_type=snapshot
url=server=tcp:127.0.0.1,1433;database=ape_dts;User ID=url_user;Password=url_password;Encrypt=true
username=sa
password=Password123!
ssl_mode=disable
app_name=task-config-extractor
parallel_type=chunk
partition_cols=json:[{"db":"dbo","tb":"basic_test","partition_col":"id"}]
max_connections=2
connection_timeout_secs=7
batch_size=16

[sinker]
db_type=mssql
sink_type=write
url=jdbc:sqlserver://127.0.0.1:1434;database=ape_dts;user=sa;password=Password123!;encrypt=DANGER_PLAINTEXT;ApplicationName=from-jdbc
max_connections=2
connection_timeout_secs=9
batch_size=8

[parallelizer]
parallel_type=snapshot
parallel_size=2
"#,
        )
        .expect("MSSQL chunk config should be accepted");

        assert_eq!(config.extractor_basic.db_type, DbType::Mssql);
        assert_eq!(config.sinker_basic.db_type, DbType::Mssql);
        assert_eq!(config.extractor_basic.connection_timeout_secs, 7);
        assert_eq!(config.sinker_basic.connection_timeout_secs, 9);
        assert_eq!(
            config.extractor_basic.app_name.as_deref(),
            Some("task-config-extractor")
        );
        assert_eq!(config.sinker_basic.app_name, None);
        assert_eq!(
            config.task_type(),
            Some(TaskType::new(TaskKind::Snapshot, None))
        );
        match config.extractor {
            ExtractorConfig::MssqlSnapshot {
                parallel_type,
                partition_cols,
                ..
            } => {
                assert_eq!(parallel_type, RdbParallelType::Chunk);
                assert!(partition_cols.contains("basic_test"));
            }
            _ => panic!("expected MSSQL snapshot extractor"),
        }
        assert!(matches!(config.sinker, SinkerConfig::Mssql { .. }));
    }

    #[test]
    fn mssql_snapshot_config_defaults_connection_timeout_to_15_seconds() {
        let config = load_temp_task_config(
            r#"[extractor]
db_type=mssql
extract_type=snapshot
url=server=tcp:127.0.0.1,1433;database=ape_dts
username=sa
password=Password123!
ssl_mode=disable

[sinker]
db_type=mssql
sink_type=write
url=server=tcp:127.0.0.1,1434;database=ape_dts
username=sa
password=Password123!
ssl_mode=disable

[parallelizer]
parallel_type=snapshot
parallel_size=1
"#,
        )
        .expect("MSSQL connection timeout defaults should be accepted");

        assert_eq!(config.extractor_basic.connection_timeout_secs, 15);
        assert_eq!(config.sinker_basic.connection_timeout_secs, 15);
        assert!(matches!(
            config.extractor,
            ExtractorConfig::MssqlSnapshot { .. }
        ));
        assert!(matches!(config.sinker, SinkerConfig::Mssql { .. }));
    }

    #[test]
    fn mssql_snapshot_config_rejects_invalid_connection_string() {
        let result = load_temp_task_config(
            r#"[extractor]
db_type=mssql
extract_type=snapshot
url=not-a-mssql-connection-string
username=sa
password=Password123!
ssl_mode=disable

[sinker]
db_type=mssql
sink_type=write
url=server=tcp:127.0.0.1,1434;database=ape_dts
username=sa
password=Password123!
ssl_mode=disable
"#,
        );
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("invalid MSSQL connection string should be rejected"),
        };

        assert!(error
            .to_string()
            .contains("valid MSSQL connection configuration"));
    }

    #[test]
    fn mssql_snapshot_config_rejects_zero_connection_timeout() {
        let result = load_temp_task_config(
            r#"[extractor]
db_type=mssql
extract_type=snapshot
url=server=tcp:127.0.0.1,1433;database=ape_dts
username=sa
password=Password123!
ssl_mode=disable
connection_timeout_secs=0

[sinker]
db_type=mssql
sink_type=write
url=server=tcp:127.0.0.1,1434;database=ape_dts
username=sa
password=Password123!
ssl_mode=disable
"#,
        );
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("zero MSSQL connection timeout should be rejected"),
        };

        assert_eq!(
            error.root_cause().to_string(),
            "config [extractor].connection_timeout_secs must be greater than 0"
        );
    }
}
