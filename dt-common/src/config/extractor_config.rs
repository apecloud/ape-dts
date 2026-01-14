use crate::config::connection_auth_config::ConnectionAuthConfig;

use super::{
    config_enums::{DbType, ExtractType},
    s3_config::S3Config,
};

#[derive(Clone, Debug)]
pub enum ExtractorConfig {
    MysqlStruct {
        url: String,
        connection_auth: ConnectionAuthConfig,
        db: String,
        dbs: Vec<String>,
        db_batch_size: usize,
    },

    PgStruct {
        url: String,
        connection_auth: ConnectionAuthConfig,
        schema: String,
        schemas: Vec<String>,
        do_global_structs: bool,
        db_batch_size: usize,
    },

    MysqlSnapshot {
        url: String,
        connection_auth: ConnectionAuthConfig,
        db: String,
        tb: String,
        sample_interval: usize,
        parallel_size: usize,
        batch_size: usize,
        partition_cols: String,
    },

    MysqlCdc {
        url: String,
        connection_auth: ConnectionAuthConfig,
        binlog_filename: String,
        binlog_position: u32,
        server_id: u64,
        gtid_enabled: bool,
        gtid_set: String,
        binlog_heartbeat_interval_secs: u64,
        binlog_timeout_secs: u64,
        heartbeat_interval_secs: u64,
        heartbeat_tb: String,
        start_time_utc: String,
        end_time_utc: String,
        keepalive_idle_secs: u64,
        keepalive_interval_secs: u64,
    },

    MysqlCheck {
        url: String,
        connection_auth: ConnectionAuthConfig,
        check_log_dir: String,
        batch_size: usize,
    },

    PgSnapshot {
        url: String,
        connection_auth: ConnectionAuthConfig,
        schema: String,
        tb: String,
        sample_interval: usize,
        parallel_size: usize,
        batch_size: usize,
        partition_cols: String,
    },

    PgCdc {
        url: String,
        connection_auth: ConnectionAuthConfig,
        slot_name: String,
        pub_name: String,
        start_lsn: String,
        recreate_slot_if_exists: bool,
        keepalive_interval_secs: u64,
        heartbeat_interval_secs: u64,
        heartbeat_tb: String,
        ddl_meta_tb: String,
        start_time_utc: String,
        end_time_utc: String,
    },

    PgCheck {
        url: String,
        connection_auth: ConnectionAuthConfig,
        check_log_dir: String,
        batch_size: usize,
    },

    MongoSnapshot {
        url: String,
        connection_auth: ConnectionAuthConfig,
        app_name: String,
        db: String,
        tb: String,
    },

    MongoCdc {
        url: String,
        connection_auth: ConnectionAuthConfig,
        app_name: String,
        resume_token: String,
        start_timestamp: u32,
        // op_log, change_stream
        source: String,
        heartbeat_interval_secs: u64,
        heartbeat_tb: String,
    },

    MongoCheck {
        url: String,
        connection_auth: ConnectionAuthConfig,
        app_name: String,
        check_log_dir: String,
        batch_size: usize,
    },

    RedisSnapshot {
        url: String,
        connection_auth: ConnectionAuthConfig,
        repl_port: u64,
    },

    RedisCdc {
        url: String,
        connection_auth: ConnectionAuthConfig,
        repl_id: String,
        repl_offset: u64,
        repl_port: u64,
        keepalive_interval_secs: u64,
        heartbeat_interval_secs: u64,
        heartbeat_key: String,
        now_db_id: i64,
    },

    RedisSnapshotAndCdc {
        url: String,
        connection_auth: ConnectionAuthConfig,
        repl_id: String,
        repl_port: u64,
        keepalive_interval_secs: u64,
        heartbeat_interval_secs: u64,
        heartbeat_key: String,
    },

    RedisSnapshotFile {
        file_path: String,
    },

    RedisScan {
        url: String,
        connection_auth: ConnectionAuthConfig,
        scan_count: u64,
        statistic_type: String,
    },

    RedisReshard {
        url: String,
        connection_auth: ConnectionAuthConfig,
    },

    Kafka {
        url: String,
        group: String,
        topic: String,
        partition: i32,
        offset: i64,
        ack_interval_secs: u64,
    },

    FoxlakeS3 {
        url: String,
        schema: String,
        tb: String,
        s3_config: S3Config,
        batch_size: usize,
    },
}

#[derive(Clone, Debug, Hash)]
pub struct BasicExtractorConfig {
    pub db_type: DbType,
    pub extract_type: ExtractType,
    pub url: String,
    pub connection_auth: ConnectionAuthConfig,
    pub max_connections: u32,
}
