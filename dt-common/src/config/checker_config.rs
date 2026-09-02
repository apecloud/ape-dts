use super::s3_config::S3Config;

pub const DEFAULT_CHECKER_BATCH_SIZE: usize = 200;
pub const DEFAULT_CHECKER_QUEUE_SIZE: usize = 200;
pub const DEFAULT_RECHECK_QUEUE_SIZE: usize = 10_000;
pub const DEFAULT_RECHECK_QUEUE_MEMORY_MB: usize = 256;
pub const DEFAULT_CHECK_LOG_FILE_SIZE: &str = "100mb";
pub const DEFAULT_CHECK_LOG_MAX_ROWS: usize = 1000;
pub const DEFAULT_CDC_CHECK_LOG_INTERVAL_SECS: u64 = 30;

/// Common checker settings.
///
/// Standalone snapshot/struct/check-log tasks use `[sinker] sink_type=check`; the checker target
/// connection is loaded through the regular MySQL/PostgreSQL/MSSQL/MongoDB sinker configuration.
/// `[checker_output]` owns result output settings. CDC inline check is enabled separately through
/// `[checker_cdc] is_enabled=true`.
#[derive(Clone, Debug)]
pub struct CheckerConfig {
    pub batch_size: usize,
    pub sample_percent: Option<u8>,
    pub recheck_count: u32,
    pub recheck_interval_secs: u64,
    pub recheck_queue_size: usize,
    pub recheck_queue_memory_mb: usize,
    pub output: CheckerOutputConfig,
    pub inline_check: Option<InlineCheckConfig>,
}

impl Default for CheckerConfig {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_CHECKER_BATCH_SIZE,
            sample_percent: None,
            recheck_count: 0,
            recheck_interval_secs: 0,
            recheck_queue_size: DEFAULT_RECHECK_QUEUE_SIZE,
            recheck_queue_memory_mb: DEFAULT_RECHECK_QUEUE_MEMORY_MB,
            output: CheckerOutputConfig::default(),
            inline_check: None,
        }
    }
}

impl CheckerConfig {
    pub fn log_dir(&self) -> &str {
        &self.output.log_dir
    }

    pub fn log_file_size(&self) -> &str {
        &self.output.log_file_size
    }

    pub fn log_max_rows(&self) -> usize {
        self.output.log_max_rows
    }

    pub fn s3_output(&self) -> Option<(&S3Config, &str)> {
        match &self.output.output_type {
            CheckerOutputType::S3 { key_prefix, config } => Some((config, key_prefix)),
            CheckerOutputType::Logs => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CheckerOutputConfig {
    pub output_full_row: bool,
    pub output_revise_sql: bool,
    pub revise_match_full_row: bool,
    pub log_dir: String,
    pub log_file_size: String,
    pub log_max_rows: usize,
    pub output_type: CheckerOutputType,
}

impl Default for CheckerOutputConfig {
    fn default() -> Self {
        Self {
            output_full_row: false,
            output_revise_sql: false,
            revise_match_full_row: false,
            log_dir: String::new(),
            log_file_size: DEFAULT_CHECK_LOG_FILE_SIZE.to_string(),
            log_max_rows: DEFAULT_CHECK_LOG_MAX_ROWS,
            output_type: CheckerOutputType::Logs,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CheckerOutputType {
    Logs,
    S3 {
        key_prefix: String,
        config: S3Config,
    },
}

#[derive(Clone, Debug)]
pub struct InlineCheckConfig {
    pub queue_size: usize,
    pub check_log_interval_secs: u64,
}

impl Default for InlineCheckConfig {
    fn default() -> Self {
        Self {
            queue_size: DEFAULT_CHECKER_QUEUE_SIZE,
            check_log_interval_secs: DEFAULT_CDC_CHECK_LOG_INTERVAL_SECS,
        }
    }
}
