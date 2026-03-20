use super::{
    config_enums::DbType, connection_auth_config::ConnectionAuthConfig, s3_config::S3Config,
};

#[derive(Clone)]
pub struct CheckerConfig {
    pub queue_size: usize,
    pub max_connections: u32,
    pub batch_size: usize,
    pub output_full_row: bool,
    pub output_revise_sql: bool,
    pub revise_match_full_row: bool,
    pub retry_interval_secs: u64,
    pub max_retries: u32,
    pub check_log_dir: String,
    pub check_log_file_size: String,
    pub check_log_max_rows: usize,
    pub db_type: DbType,
    pub url: String,
    pub connection_auth: ConnectionAuthConfig,
    pub cdc_check_log_s3: bool,
    pub s3_config: Option<S3Config>,
    pub s3_key_prefix: String,
    pub cdc_check_log_interval_secs: u64,
}

impl Default for CheckerConfig {
    fn default() -> Self {
        Self {
            queue_size: 200,
            max_connections: 8,
            batch_size: 200,
            output_full_row: false,
            output_revise_sql: false,
            revise_match_full_row: false,
            retry_interval_secs: 0,
            max_retries: 0,
            check_log_dir: String::new(),
            check_log_file_size: "100mb".to_string(),
            check_log_max_rows: 1000,
            db_type: DbType::default(),
            url: String::new(),
            connection_auth: ConnectionAuthConfig::default(),
            cdc_check_log_s3: false,
            s3_config: None,
            s3_key_prefix: String::new(),
            cdc_check_log_interval_secs: 10,
        }
    }
}
