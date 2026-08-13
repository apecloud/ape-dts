pub mod column_value_conversion_tests;
pub mod connection_pool_tests;
pub mod meta_manager_tests;
pub mod mssql_server_behavior_tests;
pub mod resumer_tests;
pub mod sinker_tests;
pub mod snapshot_extractor_tests;
pub mod snapshot_splitter_type_tests;

pub const TASK_CONFIG_FILE: &str = "mssql_to_mssql/funtionality/config/task_config.ini";
pub const JDBC_TASK_CONFIG_FILE: &str = "mssql_to_mssql/funtionality/config/jdbc_task_config.ini";
