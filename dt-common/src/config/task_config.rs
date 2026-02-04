#[cfg(feature = "metrics")]
use std::collections::HashMap;
use std::{
    fs::{self, File},
    io::Read,
};

use anyhow::{bail, Ok};

#[cfg(feature = "metrics")]
use crate::config::metrics_config::MetricsConfig;
use crate::{
    config::{
        config_enums::ResumeType, connection_auth_config::ConnectionAuthConfig,
        global_config::GlobalConfig,
    },
    error::Error,
    utils::task_util::TaskUtil,
};

use super::{
    checker_config::CheckerConfig,
    config_enums::{
        ConflictPolicyEnum, DbType, ExtractType, MetaCenterType, ParallelType, PipelineType,
        SinkType,
    },
    data_marker_config::DataMarkerConfig,
    extractor_config::{BasicExtractorConfig, ExtractorConfig},
    filter_config::FilterConfig,
    ini_loader::IniLoader,
    meta_center_config::MetaCenterConfig,
    parallelizer_config::ParallelizerConfig,
    pipeline_config::PipelineConfig,
    processor_config::ProcessorConfig,
    resumer_config::ResumerConfig,
    router_config::RouterConfig,
    runtime_config::RuntimeConfig,
    s3_config::S3Config,
    sinker_config::{BasicSinkerConfig, SinkerConfig},
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
    #[cfg(feature = "metrics")]
    pub metrics: MetricsConfig,
}

pub const DEFAULT_DB_BATCH_SIZE: usize = 100;
pub const DEFAULT_MAX_CONNECTIONS: u32 = 10;
pub const DEFAULT_CHECK_LOG_FILE_SIZE: &str = "100mb";

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
const META_CENTER: &str = "metacenter";
// keys
const CHECK_LOG_DIR: &str = "check_log_dir";
const CHECK_LOG_FILE_SIZE: &str = "check_log_file_size";
const OUTPUT_FULL_ROW: &str = "output_full_row";
const OUTPUT_REVISE_SQL: &str = "output_revise_sql";
const REVISE_MATCH_FULL_ROW: &str = "revise_match_full_row";
const RETRY_INTERVAL_SECS: &str = "retry_interval_secs";
const MAX_RETRIES: &str = "max_retries";
const DB_TYPE: &str = "db_type";
const URL: &str = "url";
const BATCH_SIZE: &str = "batch_size";
const MAX_CONNECTIONS: &str = "max_connections";
const SAMPLE_INTERVAL: &str = "sample_interval";
const PARTITION_COLS: &str = "partition_cols";
const HEARTBEAT_INTERVAL_SECS: &str = "heartbeat_interval_secs";
const KEEPALIVE_INTERVAL_SECS: &str = "keepalive_interval_secs";
const HEARTBEAT_TB: &str = "heartbeat_tb";
const APP_NAME: &str = "app_name";
const REVERSE: &str = "reverse";
const REPL_PORT: &str = "repl_port";
const PARALLEL_SIZE: &str = "parallel_size";
const DDL_CONFLICT_POLICY: &str = "ddl_conflict_policy";
const REPLACE: &str = "replace";
const DISABLE_FOREIGN_KEY_CHECKS: &str = "disable_foreign_key_checks";
const RESUME_TYPE: &str = "resume_type";
const CHECKER_QUEUE_SIZE: &str = "queue_size";
const CHECKER_SAMPLE_RATE: &str = "sample_rate";
const USERNAME: &str = "username";
const PASSWORD: &str = "password";

// deprecated keys
const RESUME_FROM_LOG: &str = "resume_from_log";
const RESUME_LOG_DIR: &str = "resume_log_dir";
const RESUME_CONFIG_FILE: &str = "resume_config_file";
// default values
const APE_DTS: &str = "APE_DTS";
const ASTRISK: &str = "*";
const RESUMER_CONNECTION_LIMIT_DEFAULT: usize = 5;

impl TaskConfig {
    pub fn new(task_config_file: &str) -> anyhow::Result<Self> {
        let loader = IniLoader::new(task_config_file);

        let pipeline = Self::load_pipeline_config(&loader);
        let runtime = Self::load_runtime_config(&loader)?;
        let (sinker_basic, sinker) = Self::load_sinker_config(&loader)?;
        let resumer = Self::load_resumer_config(&loader, &runtime, &sinker_basic)?;
        let (extractor_basic, extractor) = Self::load_extractor_config(&loader, &pipeline)?;
        let filter = Self::load_filter_config(&loader)?;
        let router = Self::load_router_config(&loader)?;
        let checker = Self::load_checker_config(&loader, &sinker_basic)?;
        Ok(Self {
            global: Self::load_global_config(
                &loader,
                &extractor_basic,
                &sinker_basic,
                &filter,
                &router,
            )?,
            extractor_basic,
            extractor,
            parallelizer: Self::load_parallelizer_config(&loader)?,
            pipeline,
            sinker_basic,
            sinker,
            runtime,
            filter,
            router,
            resumer,
            checker,
            data_marker: Self::load_data_marker_config(&loader)?,
            processor: Self::load_processor_config(&loader)?,
            meta_center: Self::load_meta_center_config(&loader)?,
            #[cfg(feature = "metrics")]
            metrics: Self::load_metrics_config(&loader)?,
        })
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
            ),
        })
    }

    fn load_extractor_config(
        loader: &IniLoader,
        pipeline: &PipelineConfig,
    ) -> anyhow::Result<(BasicExtractorConfig, ExtractorConfig)> {
        let db_type: DbType = loader.get_required(EXTRACTOR, DB_TYPE);
        let extract_type: ExtractType = loader.get_required(EXTRACTOR, "extract_type");
        let url: String = loader.get_optional(EXTRACTOR, URL);
        let heartbeat_interval_secs: u64 =
            loader.get_with_default(EXTRACTOR, HEARTBEAT_INTERVAL_SECS, 10);
        let keepalive_interval_secs: u64 =
            loader.get_with_default(EXTRACTOR, KEEPALIVE_INTERVAL_SECS, 10);
        let heartbeat_tb = loader.get_optional(EXTRACTOR, HEARTBEAT_TB);
        let batch_size = loader.get_with_default(EXTRACTOR, BATCH_SIZE, pipeline.buffer_size);
        let max_connections =
            loader.get_with_default(EXTRACTOR, MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS);

        let connection_auth = ConnectionAuthConfig::from(loader, EXTRACTOR);

        let basic = BasicExtractorConfig {
            db_type: db_type.clone(),
            extract_type: extract_type.clone(),
            url: url.clone(),
            connection_auth: connection_auth.clone(),
            max_connections,
        };

        let not_supported_err =
            Error::ConfigError(format!("extract type: {} not supported", extract_type));

        let extractor = match db_type {
            DbType::Mysql => match extract_type {
                ExtractType::Snapshot => ExtractorConfig::MysqlSnapshot {
                    url,
                    connection_auth,
                    db: String::new(),
                    tb: String::new(),
                    sample_interval: loader.get_with_default(EXTRACTOR, SAMPLE_INTERVAL, 1),
                    parallel_size: loader.get_with_default(EXTRACTOR, PARALLEL_SIZE, 1),
                    batch_size,
                    partition_cols: loader.get_optional(EXTRACTOR, PARTITION_COLS),
                },

                ExtractType::Cdc => ExtractorConfig::MysqlCdc {
                    url,
                    connection_auth,
                    binlog_filename: loader.get_optional(EXTRACTOR, "binlog_filename"),
                    binlog_position: loader.get_optional(EXTRACTOR, "binlog_position"),
                    server_id: loader.get_required(EXTRACTOR, "server_id"),
                    gtid_enabled: loader.get_optional(EXTRACTOR, "gtid_enabled"),
                    gtid_set: loader.get_optional(EXTRACTOR, "gtid_set"),
                    binlog_heartbeat_interval_secs: loader.get_with_default(
                        EXTRACTOR,
                        "binlog_heartbeat_interval_secs",
                        10,
                    ),
                    binlog_timeout_secs: loader.get_with_default(
                        EXTRACTOR,
                        "binlog_timeout_secs",
                        60,
                    ),
                    heartbeat_interval_secs,
                    heartbeat_tb,
                    keepalive_idle_secs: loader.get_with_default(
                        EXTRACTOR,
                        "keepalive_idle_secs",
                        60,
                    ),
                    keepalive_interval_secs: loader.get_with_default(
                        EXTRACTOR,
                        "keepalive_interval_secs",
                        10,
                    ),
                    start_time_utc: loader.get_optional(EXTRACTOR, "start_time_utc"),
                    end_time_utc: loader.get_optional(EXTRACTOR, "end_time_utc"),
                },

                ExtractType::CheckLog => ExtractorConfig::MysqlCheck {
                    url,
                    connection_auth,
                    check_log_dir: loader.get_required(EXTRACTOR, CHECK_LOG_DIR),
                    batch_size: loader.get_with_default(EXTRACTOR, BATCH_SIZE, 200),
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
                    ),
                },

                ExtractType::FoxlakeS3 => {
                    let s3_config = S3Config {
                        bucket: loader.get_optional(EXTRACTOR, "s3_bucket"),
                        access_key: loader.get_optional(EXTRACTOR, "s3_access_key"),
                        secret_key: loader.get_optional(EXTRACTOR, "s3_secret_key"),
                        region: loader.get_optional(EXTRACTOR, "s3_region"),
                        endpoint: loader.get_optional(EXTRACTOR, "s3_endpoint"),
                        root_dir: loader.get_optional(EXTRACTOR, "s3_root_dir"),
                        root_url: loader.get_optional(EXTRACTOR, "s3_root_url"),
                    };
                    ExtractorConfig::FoxlakeS3 {
                        url,
                        schema: String::new(),
                        tb: String::new(),
                        s3_config,
                        batch_size,
                    }
                }

                _ => bail! {not_supported_err},
            },

            DbType::Pg => match extract_type {
                ExtractType::Snapshot => ExtractorConfig::PgSnapshot {
                    url,
                    connection_auth,
                    schema: String::new(),
                    tb: String::new(),
                    sample_interval: loader.get_with_default(EXTRACTOR, SAMPLE_INTERVAL, 1),
                    parallel_size: loader.get_with_default(EXTRACTOR, PARALLEL_SIZE, 1),
                    batch_size,
                    partition_cols: loader.get_optional(EXTRACTOR, PARTITION_COLS),
                },

                ExtractType::Cdc => ExtractorConfig::PgCdc {
                    url,
                    connection_auth,
                    slot_name: loader.get_required(EXTRACTOR, "slot_name"),
                    pub_name: loader.get_optional(EXTRACTOR, "pub_name"),
                    start_lsn: loader.get_optional(EXTRACTOR, "start_lsn"),
                    recreate_slot_if_exists: loader
                        .get_optional(EXTRACTOR, "recreate_slot_if_exists"),
                    keepalive_interval_secs,
                    heartbeat_interval_secs,
                    heartbeat_tb,
                    ddl_meta_tb: loader.get_optional(EXTRACTOR, "ddl_meta_tb"),
                    start_time_utc: loader.get_optional(EXTRACTOR, "start_time_utc"),
                    end_time_utc: loader.get_optional(EXTRACTOR, "end_time_utc"),
                },

                ExtractType::CheckLog => ExtractorConfig::PgCheck {
                    url,
                    connection_auth,
                    check_log_dir: loader.get_required(EXTRACTOR, CHECK_LOG_DIR),
                    batch_size: loader.get_with_default(EXTRACTOR, BATCH_SIZE, 200),
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
                    ),
                },

                _ => bail! { not_supported_err },
            },

            DbType::Mongo => {
                let app_name: String =
                    loader.get_with_default(EXTRACTOR, APP_NAME, APE_DTS.to_string());
                match extract_type {
                    ExtractType::Snapshot => ExtractorConfig::MongoSnapshot {
                        url,
                        connection_auth,
                        app_name,
                        db: String::new(),
                        tb: String::new(),
                    },

                    ExtractType::Cdc => ExtractorConfig::MongoCdc {
                        url,
                        connection_auth,
                        app_name,
                        resume_token: loader.get_optional(EXTRACTOR, "resume_token"),
                        start_timestamp: loader.get_optional(EXTRACTOR, "start_timestamp"),
                        source: loader.get_optional(EXTRACTOR, "source"),
                        heartbeat_interval_secs,
                        heartbeat_tb,
                    },

                    ExtractType::CheckLog => ExtractorConfig::MongoCheck {
                        url,
                        connection_auth,
                        app_name,
                        check_log_dir: loader.get_required(EXTRACTOR, CHECK_LOG_DIR),
                        batch_size: loader.get_with_default(EXTRACTOR, BATCH_SIZE, 200),
                    },

                    _ => bail! { not_supported_err },
                }
            }

            DbType::Redis => match extract_type {
                ExtractType::Snapshot => {
                    let repl_port = loader.get_with_default(EXTRACTOR, REPL_PORT, 10008);
                    ExtractorConfig::RedisSnapshot {
                        url,
                        connection_auth,
                        repl_port,
                    }
                }

                ExtractType::SnapshotFile => ExtractorConfig::RedisSnapshotFile {
                    file_path: loader.get_required(EXTRACTOR, "file_path"),
                },

                ExtractType::Scan => ExtractorConfig::RedisScan {
                    url,
                    connection_auth,
                    statistic_type: loader.get_required(EXTRACTOR, "statistic_type"),
                    scan_count: loader.get_with_default(EXTRACTOR, "scan_count", 1000),
                },

                ExtractType::Cdc => {
                    let repl_port = loader.get_with_default(EXTRACTOR, REPL_PORT, 10008);
                    ExtractorConfig::RedisCdc {
                        url,
                        connection_auth,
                        repl_port,
                        repl_id: loader.get_optional(EXTRACTOR, "repl_id"),
                        repl_offset: loader.get_optional(EXTRACTOR, "repl_offset"),
                        keepalive_interval_secs,
                        heartbeat_interval_secs,
                        heartbeat_key: loader.get_optional(EXTRACTOR, "heartbeat_key"),
                        now_db_id: loader.get_optional(EXTRACTOR, "now_db_id"),
                    }
                }

                ExtractType::SnapshotAndCdc => {
                    let repl_port = loader.get_with_default(EXTRACTOR, REPL_PORT, 10008);
                    ExtractorConfig::RedisSnapshotAndCdc {
                        url,
                        connection_auth,
                        repl_port,
                        repl_id: loader.get_optional(EXTRACTOR, "repl_id"),
                        keepalive_interval_secs,
                        heartbeat_interval_secs,
                        heartbeat_key: loader.get_optional(EXTRACTOR, "heartbeat_key"),
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
                group: loader.get_required(EXTRACTOR, "group"),
                topic: loader.get_required(EXTRACTOR, "topic"),
                partition: loader.get_optional(EXTRACTOR, "partition"),
                offset: loader.get_optional(EXTRACTOR, "offset"),
                ack_interval_secs: loader.get_optional(EXTRACTOR, "ack_interval_secs"),
            },

            db_type => {
                bail! {Error::ConfigError(format!(
                    "extractor db type: {} not supported",
                    db_type
                ))}
            }
        };
        Ok((basic, extractor))
    }

    fn load_sinker_config(loader: &IniLoader) -> anyhow::Result<(BasicSinkerConfig, SinkerConfig)> {
        if !loader.ini.sections().contains(&SINKER.to_string()) {
            if loader.ini.sections().contains(&CHECKER.to_string()) {
                let db_type: DbType = loader.get_required(CHECKER, DB_TYPE);
                let url: String = loader.get_required(CHECKER, URL);
                let connection_auth = ConnectionAuthConfig::from(loader, CHECKER);
                let basic = BasicSinkerConfig {
                    sink_type: SinkType::Dummy,
                    db_type: db_type.clone(),
                    url: url.clone(),
                    connection_auth: connection_auth.clone(),
                    batch_size: loader.get_with_default(CHECKER, BATCH_SIZE, 200),
                    max_connections: loader.get_with_default(
                        CHECKER,
                        MAX_CONNECTIONS,
                        DEFAULT_MAX_CONNECTIONS,
                    ),
                };
                return Ok((basic, SinkerConfig::Dummy));
            }
            bail!(Error::ConfigError(
                "config [sinker] is required when [checker] is not set".into()
            ));
        }

        let sink_type = loader.get_with_default(SINKER, "sink_type", SinkType::Write);
        if let SinkType::Dummy = sink_type {
            return Ok((BasicSinkerConfig::default(), SinkerConfig::Dummy));
        }

        let db_type: DbType = loader.get_required(SINKER, DB_TYPE);
        let url: String = loader.get_optional(SINKER, URL);
        let batch_size: usize = loader.get_with_default(SINKER, BATCH_SIZE, 200);
        let max_connections =
            loader.get_with_default(SINKER, MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS);
        let connection_auth = ConnectionAuthConfig::from(loader, SINKER);

        let basic = BasicSinkerConfig {
            sink_type: sink_type.clone(),
            db_type: db_type.clone(),
            url: url.clone(),
            connection_auth: connection_auth.clone(),
            batch_size,
            max_connections,
        };

        let conflict_policy: ConflictPolicyEnum =
            loader.get_with_default(SINKER, "conflict_policy", ConflictPolicyEnum::Interrupt);

        let not_supported_err =
            Error::ConfigError(format!("sinker db type: {} not supported", db_type));

        let sinker = match db_type {
            DbType::Mysql | DbType::Tidb => match sink_type {
                SinkType::Write => SinkerConfig::Mysql {
                    url,
                    connection_auth,
                    batch_size,
                    replace: loader.get_with_default(SINKER, REPLACE, true),
                    disable_foreign_key_checks: loader.get_with_default(
                        SINKER,
                        DISABLE_FOREIGN_KEY_CHECKS,
                        true,
                    ),
                    transaction_isolation: loader.get_optional(SINKER, "transaction_isolation"),
                },

                SinkType::Struct => SinkerConfig::MysqlStruct {
                    url,
                    connection_auth,
                    conflict_policy,
                },

                SinkType::Sql => SinkerConfig::Sql {
                    reverse: loader.get_optional(SINKER, REVERSE),
                },

                _ => bail! { not_supported_err },
            },

            DbType::Pg => match sink_type {
                SinkType::Write => SinkerConfig::Pg {
                    url,
                    connection_auth,
                    batch_size,
                    replace: loader.get_with_default(SINKER, REPLACE, true),
                    disable_foreign_key_checks: loader.get_with_default(
                        SINKER,
                        DISABLE_FOREIGN_KEY_CHECKS,
                        true,
                    ),
                },

                SinkType::Struct => SinkerConfig::PgStruct {
                    url,
                    connection_auth,
                    conflict_policy,
                },

                SinkType::Sql => SinkerConfig::Sql {
                    reverse: loader.get_optional(SINKER, REVERSE),
                },

                _ => bail! { not_supported_err },
            },

            DbType::Mongo => {
                let app_name: String =
                    loader.get_with_default(SINKER, APP_NAME, APE_DTS.to_string());
                match sink_type {
                    SinkType::Write => SinkerConfig::Mongo {
                        url,
                        connection_auth,
                        app_name,
                        batch_size,
                    },

                    _ => bail! { not_supported_err },
                }
            }

            DbType::Kafka => SinkerConfig::Kafka {
                url,
                batch_size,
                ack_timeout_secs: loader.get_with_default(SINKER, "ack_timeout_secs", 5),
                required_acks: loader.get_with_default(SINKER, "required_acks", "one".to_string()),
                with_field_defs: loader.get_with_default(SINKER, "with_field_defs", true),
            },

            DbType::Redis => match sink_type {
                SinkType::Write => SinkerConfig::Redis {
                    url,
                    connection_auth,
                    batch_size,
                    method: loader.get_optional(SINKER, "method"),
                    is_cluster: loader.get_optional(SINKER, "is_cluster"),
                },

                SinkType::Statistic => SinkerConfig::RedisStatistic {
                    statistic_type: loader.get_required(SINKER, "statistic_type"),
                    data_size_threshold: loader.get_optional(SINKER, "data_size_threshold"),
                    freq_threshold: loader.get_optional(SINKER, "freq_threshold"),
                    statistic_log_dir: loader.get_optional(SINKER, "statistic_log_dir"),
                },

                _ => bail! { not_supported_err },
            },

            DbType::StarRocks => match sink_type {
                SinkType::Write => SinkerConfig::StarRocks {
                    url,
                    connection_auth,
                    batch_size,
                    stream_load_url: loader.get_optional(SINKER, "stream_load_url"),
                    hard_delete: loader.get_optional(SINKER, "hard_delete"),
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
                    stream_load_url: loader.get_optional(SINKER, "stream_load_url"),
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
                    ),
                },

                _ => bail! { not_supported_err },
            },

            DbType::Foxlake => {
                let s3_config = S3Config {
                    bucket: loader.get_optional(SINKER, "s3_bucket"),
                    access_key: loader.get_optional(SINKER, "s3_access_key"),
                    secret_key: loader.get_optional(SINKER, "s3_secret_key"),
                    region: loader.get_optional(SINKER, "s3_region"),
                    endpoint: loader.get_optional(SINKER, "s3_endpoint"),
                    root_dir: loader.get_optional(SINKER, "s3_root_dir"),
                    root_url: loader.get_optional(SINKER, "s3_root_url"),
                };

                match sink_type {
                    SinkType::Write => SinkerConfig::Foxlake {
                        url,
                        batch_size,
                        batch_memory_mb: loader.get_optional(SINKER, "batch_memory_mb"),
                        s3_config,
                        engine: loader.get_optional(SINKER, "engine"),
                    },

                    SinkType::Struct => SinkerConfig::FoxlakeStruct {
                        url,
                        conflict_policy,
                        engine: loader.get_optional(SINKER, "engine"),
                    },

                    SinkType::Push => SinkerConfig::FoxlakePush {
                        url,
                        batch_size,
                        batch_memory_mb: loader.get_optional(SINKER, "batch_memory_mb"),
                        s3_config,
                    },

                    SinkType::Merge => SinkerConfig::FoxlakeMerge {
                        url,
                        batch_size,
                        s3_config,
                    },

                    _ => bail! { not_supported_err },
                }
            }
        };
        Ok((basic, sinker))
    }

    fn load_parallelizer_config(loader: &IniLoader) -> anyhow::Result<ParallelizerConfig> {
        Ok(ParallelizerConfig {
            parallel_size: loader.get_with_default(PARALLELIZER, PARALLEL_SIZE, 1),
            parallel_type: loader.get_with_default(
                PARALLELIZER,
                "parallel_type",
                ParallelType::Serial,
            ),
        })
    }

    fn load_pipeline_config(loader: &IniLoader) -> PipelineConfig {
        let mut config = PipelineConfig {
            buffer_size: loader.get_with_default(PIPELINE, "buffer_size", 16000),
            checkpoint_interval_secs: loader.get_with_default(
                PIPELINE,
                "checkpoint_interval_secs",
                10,
            ),
            batch_sink_interval_secs: loader.get_optional(PIPELINE, "batch_sink_interval_secs"),
            counter_time_window_secs: loader.get_optional(PIPELINE, "counter_time_window_secs"),
            counter_max_sub_count: loader.get_with_default(PIPELINE, "counter_max_sub_count", 1000),
            max_rps: loader.get_optional(PIPELINE, "max_rps"),
            buffer_memory_mb: loader.get_optional(PIPELINE, "buffer_memory_mb"),
            pipeline_type: loader.get_with_default(PIPELINE, "pipeline_type", PipelineType::Basic),
            http_host: loader.get_with_default(PIPELINE, "http_host", "0.0.0.0".to_string()),
            http_port: loader.get_with_default(PIPELINE, "http_port", 10231),
            with_field_defs: loader.get_with_default(PIPELINE, "with_field_defs", true),
        };

        if config.counter_time_window_secs == 0 {
            config.counter_time_window_secs = config.checkpoint_interval_secs;
        }
        config
    }

    fn load_checker_config(
        loader: &IniLoader,
        sinker_basic: &BasicSinkerConfig,
    ) -> anyhow::Result<Option<CheckerConfig>> {
        if !loader.ini.sections().contains(&CHECKER.to_string()) {
            return Ok(None);
        }

        let default = CheckerConfig::default();
        let mut config = CheckerConfig {
            queue_size: loader.get_with_default(CHECKER, CHECKER_QUEUE_SIZE, default.queue_size),
            max_connections: loader.get_with_default(
                CHECKER,
                MAX_CONNECTIONS,
                default.max_connections,
            ),
            batch_size: loader.get_with_default(CHECKER, BATCH_SIZE, default.batch_size),
            sample_rate: loader.get_with_default(CHECKER, CHECKER_SAMPLE_RATE, default.sample_rate),
            output_full_row: loader.get_with_default(
                CHECKER,
                OUTPUT_FULL_ROW,
                default.output_full_row,
            ),
            output_revise_sql: loader.get_with_default(
                CHECKER,
                OUTPUT_REVISE_SQL,
                default.output_revise_sql,
            ),
            revise_match_full_row: loader.get_with_default(
                CHECKER,
                REVISE_MATCH_FULL_ROW,
                default.revise_match_full_row,
            ),
            retry_interval_secs: loader.get_with_default(
                CHECKER,
                RETRY_INTERVAL_SECS,
                default.retry_interval_secs,
            ),
            max_retries: loader.get_with_default(CHECKER, MAX_RETRIES, default.max_retries),
            check_log_dir: loader.get_with_default(CHECKER, CHECK_LOG_DIR, default.check_log_dir),
            check_log_file_size: loader.get_with_default(
                CHECKER,
                CHECK_LOG_FILE_SIZE,
                default.check_log_file_size,
            ),
            ..default
        };
        let (db_type, url, connection_auth) = Self::resolve_checker_target(loader, sinker_basic)?;
        config.db_type = Some(db_type);
        config.url = Some(url);
        config.connection_auth = Some(connection_auth);
        Ok(Some(config))
    }

    fn resolve_checker_target(
        loader: &IniLoader,
        sinker_basic: &BasicSinkerConfig,
    ) -> anyhow::Result<(DbType, String, ConnectionAuthConfig)> {
        let mut target = None;
        if !sinker_basic.url.is_empty() {
            target = Some((
                sinker_basic.db_type.clone(),
                sinker_basic.url.clone(),
                sinker_basic.connection_auth.clone(),
            ));
        }

        let checker_target_present = loader.contains(CHECKER, DB_TYPE)
            || loader.contains(CHECKER, URL)
            || loader.contains(CHECKER, USERNAME)
            || loader.contains(CHECKER, PASSWORD);
        if checker_target_present {
            let db_type: DbType = loader.get_required(CHECKER, DB_TYPE);
            let url: String = loader.get_required(CHECKER, URL);
            let connection_auth = ConnectionAuthConfig::from(loader, CHECKER);
            target = Some((db_type, url, connection_auth));
        }

        if let Some(target) = target {
            return Ok(target);
        }

        bail!(Error::ConfigError(
            "config [checker] target is required when [sinker] target is not set".into()
        ))
    }

    fn load_runtime_config(loader: &IniLoader) -> anyhow::Result<RuntimeConfig> {
        Ok(RuntimeConfig {
            log_level: loader.get_with_default(RUNTIME, "log_level", "info".to_string()),
            log_dir: loader.get_with_default(RUNTIME, "log_dir", "./logs".to_string()),
            log4rs_file: loader.get_with_default(
                RUNTIME,
                "log4rs_file",
                "./log4rs.yaml".to_string(),
            ),
            tb_parallel_size: loader.get_with_default(RUNTIME, "tb_parallel_size", 1),
        })
    }

    fn load_filter_config(loader: &IniLoader) -> anyhow::Result<FilterConfig> {
        Ok(FilterConfig {
            do_schemas: loader.get_optional(FILTER, "do_dbs"),
            ignore_schemas: loader.get_optional(FILTER, "ignore_dbs"),
            do_tbs: loader.get_optional(FILTER, "do_tbs"),
            ignore_tbs: loader.get_optional(FILTER, "ignore_tbs"),
            ignore_cols: loader.get_optional(FILTER, "ignore_cols"),
            do_events: loader.get_optional(FILTER, "do_events"),
            do_ddls: loader.get_optional(FILTER, "do_ddls"),
            do_dcls: loader.get_optional(FILTER, "do_dcls"),
            do_structures: loader.get_with_default(FILTER, "do_structures", ASTRISK.to_string()),
            ignore_cmds: loader.get_optional(FILTER, "ignore_cmds"),
            where_conditions: loader.get_optional(FILTER, "where_conditions"),
        })
    }

    fn load_router_config(loader: &IniLoader) -> anyhow::Result<RouterConfig> {
        Ok(RouterConfig::Rdb {
            schema_map: loader.get_optional(ROUTER, "db_map"),
            tb_map: loader.get_optional(ROUTER, "tb_map"),
            col_map: loader.get_optional(ROUTER, "col_map"),
            topic_map: loader.get_optional(ROUTER, "topic_map"),
        })
    }

    fn load_resumer_config(
        loader: &IniLoader,
        runtime: &RuntimeConfig,
        sinker_basic: &BasicSinkerConfig,
    ) -> anyhow::Result<ResumerConfig> {
        // compatible with older versions
        if let Some(config) = Self::load_resumer_config_deprecated(loader, runtime) {
            return Ok(config);
        }

        let resume_type = loader.get_with_default(RESUMER, RESUME_TYPE, ResumeType::Dummy);
        match resume_type {
            ResumeType::FromLog => Ok(ResumerConfig::FromLog {
                log_dir: loader.get_with_default(RESUMER, "log_dir", runtime.log_dir.clone()),
                config_file: loader.get_optional(RESUMER, "config_file"),
            }),
            ResumeType::FromTarget => Ok(ResumerConfig::FromDB {
                url: sinker_basic.url.clone(),
                connection_auth: ConnectionAuthConfig::from(loader, SINKER),
                db_type: sinker_basic.db_type.clone(),
                table_full_name: loader.get_optional(RESUMER, "table_full_name"),
                max_connections: loader.get_with_default(
                    RESUMER,
                    MAX_CONNECTIONS,
                    RESUMER_CONNECTION_LIMIT_DEFAULT,
                ),
            }),
            ResumeType::FromDB => Ok(ResumerConfig::FromDB {
                url: loader.get_required(RESUMER, URL),
                connection_auth: ConnectionAuthConfig::from(loader, RESUMER),
                db_type: loader.get_required(RESUMER, DB_TYPE),
                table_full_name: loader.get_optional(RESUMER, "table_full_name"),
                max_connections: loader.get_with_default(
                    RESUMER,
                    MAX_CONNECTIONS,
                    RESUMER_CONNECTION_LIMIT_DEFAULT,
                ),
            }),
            _ => Ok(ResumerConfig::Dummy),
        }
    }

    fn load_resumer_config_deprecated(
        loader: &IniLoader,
        runtime: &RuntimeConfig,
    ) -> Option<ResumerConfig> {
        if !loader.contains(RESUMER, RESUME_FROM_LOG) && !loader.contains(RESUMER, RESUME_LOG_DIR)
            || !loader.contains(RESUMER, RESUME_CONFIG_FILE)
        {
            return None;
        }
        let resume_from_log = loader.get_with_default(RESUMER, RESUME_FROM_LOG, false);
        let log_dir = if resume_from_log {
            loader.get_with_default(RESUMER, RESUME_LOG_DIR, runtime.log_dir.clone())
        } else {
            String::new()
        };
        Some(ResumerConfig::FromLog {
            log_dir,
            config_file: loader.get_optional(RESUMER, RESUME_CONFIG_FILE),
        })
    }

    fn load_data_marker_config(loader: &IniLoader) -> anyhow::Result<Option<DataMarkerConfig>> {
        if !loader.ini.sections().contains(&DATA_MARKER.to_string()) {
            return Ok(None);
        }

        Ok(Some(DataMarkerConfig {
            topo_name: loader.get_required(DATA_MARKER, "topo_name"),
            topo_nodes: loader.get_optional(DATA_MARKER, "topo_nodes"),
            src_node: loader.get_required(DATA_MARKER, "src_node"),
            dst_node: loader.get_required(DATA_MARKER, "dst_node"),
            do_nodes: loader.get_required(DATA_MARKER, "do_nodes"),
            ignore_nodes: loader.get_optional(DATA_MARKER, "ignore_nodes"),
            marker: loader.get_required(DATA_MARKER, "marker"),
        }))
    }

    fn load_processor_config(loader: &IniLoader) -> anyhow::Result<Option<ProcessorConfig>> {
        if !loader.ini.sections().contains(&PROCESSOR.to_string()) {
            return Ok(None);
        }

        let lua_code_file = loader.get_optional(PROCESSOR, "lua_code_file");
        let mut lua_code = String::new();

        if fs::metadata(&lua_code_file).is_ok() {
            let mut file = File::open(&lua_code_file).expect("failed to open lua code file");
            file.read_to_string(&mut lua_code)
                .expect("failed to read lua code file");
        }

        Ok(Some(ProcessorConfig {
            lua_code_file,
            lua_code,
        }))
    }

    fn load_meta_center_config(loader: &IniLoader) -> anyhow::Result<Option<MetaCenterConfig>> {
        let mut config = MetaCenterConfig::Basic;
        let db_type: DbType = loader.get_required(EXTRACTOR, DB_TYPE);
        let meta_type = loader.get_with_default(META_CENTER, "type", MetaCenterType::Basic);
        if meta_type == MetaCenterType::DbEngine && db_type == DbType::Mysql {
            let extractor_url: String = loader.get_required(EXTRACTOR, URL);
            let sinker_url: String = loader.get_required(SINKER, URL);
            let meta_center_url: String = loader.get_required(META_CENTER, URL);
            if extractor_url == meta_center_url || sinker_url == meta_center_url {
                panic!(
                    "config, [{}].{} should be different with [{}].{} and [{}].{}",
                    META_CENTER, URL, EXTRACTOR, URL, SINKER, URL
                );
            }

            config = MetaCenterConfig::MySqlDbEngine {
                url: meta_center_url,
                connection_auth: ConnectionAuthConfig::from(loader, META_CENTER),
                ddl_conflict_policy: loader.get_with_default(
                    META_CENTER,
                    DDL_CONFLICT_POLICY,
                    ConflictPolicyEnum::Interrupt,
                ),
            }
        }
        Ok(Some(config))
    }

    #[cfg(feature = "metrics")]
    fn load_metrics_config(loader: &IniLoader) -> anyhow::Result<MetricsConfig> {
        let metrics_section = "metrics";
        let labels_str: String = loader.get_optional(metrics_section, "labels");
        let mut metrics_labels = HashMap::new();
        if !labels_str.is_empty() {
            for label_pair in labels_str.split(',') {
                if let Some((key, value)) = label_pair.trim().split_once('=') {
                    metrics_labels.insert(key.trim().to_string(), value.trim().to_string());
                }
            }
        }
        Ok(MetricsConfig {
            http_host: loader.get_with_default(metrics_section, "http_host", "0.0.0.0".to_string()),
            http_port: loader.get_with_default(metrics_section, "http_port", 9090),
            workers: loader.get_with_default(metrics_section, "workers", 2),
            metrics_labels,
        })
    }
}
