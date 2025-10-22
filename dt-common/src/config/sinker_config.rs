use super::{
    config_enums::{ConflictPolicyEnum, DbType},
    s3_config::S3Config,
};
use crate::config::config_enums::SinkType;

#[derive(Clone, Debug)]
pub enum SinkerConfig {
    Dummy,

    Mysql {
        url: String,
        batch_size: usize,
        replace: bool,
        disable_foreign_key_checks: bool,
    },

    Pg {
        url: String,
        batch_size: usize,
        replace: bool,
        disable_foreign_key_checks: bool,
    },

    Mongo {
        url: String,
        app_name: String,
        batch_size: usize,
    },

    MysqlCheck {
        url: String,
        batch_size: usize,
        check_log_dir: String,
    },

    PgCheck {
        url: String,
        batch_size: usize,
        check_log_dir: String,
    },

    MongoCheck {
        url: String,
        app_name: String,
        batch_size: usize,
        check_log_dir: String,
    },

    MysqlStruct {
        url: String,
        conflict_policy: ConflictPolicyEnum,
    },

    PgStruct {
        url: String,
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
        batch_size: usize,
        stream_load_url: String,
        hard_delete: bool,
    },

    DorisStruct {
        url: String,
        conflict_policy: ConflictPolicyEnum,
    },

    Doris {
        url: String,
        batch_size: usize,
        stream_load_url: String,
    },

    StarRocksStruct {
        url: String,
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

#[derive(Clone, Debug, Default)]
pub struct BasicSinkerConfig {
    pub sink_type: SinkType,
    pub db_type: DbType,
    pub url: String,
    pub batch_size: usize,
    pub max_connections: u32,
}
