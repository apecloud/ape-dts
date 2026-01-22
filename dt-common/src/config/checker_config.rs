use super::{config_enums::DbType, connection_auth_config::ConnectionAuthConfig};

#[derive(Clone)]
pub struct CheckerConfig {
    pub drop_on_full: bool,
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
    pub db_type: Option<DbType>,
    pub url: Option<String>,
    pub connection_auth: Option<ConnectionAuthConfig>,
}

impl Default for CheckerConfig {
    fn default() -> Self {
        Self {
            drop_on_full: true,
            queue_size: 2000,

            max_connections: 2,
            batch_size: 100,
            output_full_row: false,
            output_revise_sql: false,
            revise_match_full_row: false,
            retry_interval_secs: 0,
            max_retries: 0,
            check_log_dir: String::new(),
            check_log_file_size: "100mb".to_string(),
            db_type: None,
            url: None,
            connection_auth: None,
        }
    }
}
