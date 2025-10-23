use crate::config::config_enums::DbType;

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
        db_type: DbType,
        // such as public.ape_task_position or database1.table1
        table_full_name: String,
        connection_limit: usize,
    },
    Dummy,
}
