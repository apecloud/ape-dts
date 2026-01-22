use super::{
    config_enums::{ConflictPolicyEnum, DbType},
    s3_config::S3Config,
};
use crate::config::{
    config_enums::{RdbTransactionIsolation, SinkType},
    connection_auth_config::ConnectionAuthConfig,
};

#[derive(Clone, Debug)]
pub enum SinkerConfig {
    Dummy,

    Mysql {
        url: String,
        connection_auth: ConnectionAuthConfig,
        batch_size: usize,
        replace: bool,
        disable_foreign_key_checks: bool,
        // Specifies the transaction isolation level used for writes. The database default is used if not specified.
        // If ReadCommitted or ReadUncommitted is set, the target database must have BINLOG_FORMAT set to at least MIXED (ROW is recommended). Otherwise, write operations will fail.
        transaction_isolation: RdbTransactionIsolation,
    },

    Pg {
        url: String,
        connection_auth: ConnectionAuthConfig,
        batch_size: usize,
        replace: bool,
        disable_foreign_key_checks: bool,
    },

    Mongo {
        url: String,
        connection_auth: ConnectionAuthConfig,
        app_name: String,
        batch_size: usize,
    },

    MysqlStruct {
        url: String,
        connection_auth: ConnectionAuthConfig,
        conflict_policy: ConflictPolicyEnum,
    },

    PgStruct {
        url: String,
        connection_auth: ConnectionAuthConfig,
        conflict_policy: ConflictPolicyEnum,
    },

    Kafka {
        url: String,
        batch_size: usize,
        ack_timeout_secs: u64,
        required_acks: String,
        with_field_defs: bool,
    },

    Redis {
        url: String,
        connection_auth: ConnectionAuthConfig,
        batch_size: usize,
        method: String,
        is_cluster: bool,
    },

    RedisStatistic {
        statistic_type: String,
        data_size_threshold: usize,
        freq_threshold: i64,
        statistic_log_dir: String,
    },

    StarRocks {
        url: String,
        connection_auth: ConnectionAuthConfig,
        batch_size: usize,
        stream_load_url: String,
        hard_delete: bool,
    },

    DorisStruct {
        url: String,
        connection_auth: ConnectionAuthConfig,
        conflict_policy: ConflictPolicyEnum,
    },

    Doris {
        url: String,
        connection_auth: ConnectionAuthConfig,
        batch_size: usize,
        stream_load_url: String,
    },

    StarRocksStruct {
        url: String,
        connection_auth: ConnectionAuthConfig,
        conflict_policy: ConflictPolicyEnum,
    },

    ClickHouse {
        url: String,
        batch_size: usize,
    },

    ClickhouseStruct {
        url: String,
        conflict_policy: ConflictPolicyEnum,
        engine: String,
    },

    Foxlake {
        url: String,
        batch_size: usize,
        batch_memory_mb: usize,
        s3_config: S3Config,
        engine: String,
    },

    FoxlakePush {
        url: String,
        batch_size: usize,
        batch_memory_mb: usize,
        s3_config: S3Config,
    },

    FoxlakeMerge {
        url: String,
        batch_size: usize,
        s3_config: S3Config,
    },

    FoxlakeStruct {
        url: String,
        conflict_policy: ConflictPolicyEnum,
        engine: String,
    },

    Sql {
        reverse: bool,
    },
}

#[derive(Clone, Debug, Default, Hash)]
pub struct BasicSinkerConfig {
    pub sink_type: SinkType,
    pub db_type: DbType,
    pub url: String,
    pub connection_auth: ConnectionAuthConfig,
    pub batch_size: usize,
    pub max_connections: u32,
}
