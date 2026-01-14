use crate::config::{config_enums::DbType, connection_auth_config::ConnectionAuthConfig};

#[derive(Clone, Debug)]
pub enum ResumerConfig {
    // Deprecated from 2.0.25, but it continues to be compatible with the old configuration
    // pub resume_config_file: String,
    // pub resume_from_log: bool,
    // pub resume_log_dir: String,
    FromLog {
        log_dir: String,
        config_file: String,
    },
    FromDB {
        url: String,
        connection_auth: ConnectionAuthConfig,
        db_type: DbType,
        // such as public.ape_task_position or database1.table1
        table_full_name: String,
        max_connections: usize,
    },
    Dummy,
}
