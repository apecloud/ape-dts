use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};

#[derive(
    Clone,
    Display,
    EnumString,
    IntoStaticStr,
    Debug,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    Hash,
)]
pub enum DbType {
    #[default]
    #[strum(serialize = "mysql")]
    Mysql,
    #[strum(serialize = "pg")]
    Pg,
    #[strum(serialize = "kafka")]
    Kafka,
    #[strum(serialize = "mongo")]
    Mongo,
    #[strum(serialize = "redis")]
    Redis,
    #[strum(serialize = "clickhouse")]
    ClickHouse,
    #[strum(serialize = "starrocks")]
    StarRocks,
    #[strum(serialize = "doris")]
    Doris,
    #[strum(serialize = "foxlake")]
    Foxlake,
    #[strum(serialize = "tidb")]
    Tidb,
}

#[derive(Display, EnumString, IntoStaticStr, Debug, Clone, Hash)]
pub enum ExtractType {
    #[strum(serialize = "snapshot")]
    Snapshot,
    #[strum(serialize = "cdc")]
    Cdc,
    #[strum(serialize = "snapshot_and_cdc")]
    SnapshotAndCdc,
    #[strum(serialize = "check_log")]
    CheckLog,
    #[strum(serialize = "struct")]
    Struct,
    #[strum(serialize = "snapshot_file")]
    SnapshotFile,
    #[strum(serialize = "scan")]
    Scan,
    #[strum(serialize = "reshard")]
    Reshard,
    #[strum(serialize = "foxlake_s3")]
    FoxlakeS3,
}

#[derive(Display, EnumString, IntoStaticStr, Clone, Debug, Default, Hash)]
pub enum SinkType {
    #[default]
    #[strum(serialize = "dummy")]
    Dummy,
    #[strum(serialize = "write")]
    Write,
    #[strum(serialize = "struct")]
    Struct,
    #[strum(serialize = "statistic")]
    Statistic,
    #[strum(serialize = "sql")]
    Sql,
    #[strum(serialize = "push")]
    Push,
    #[strum(serialize = "merge")]
    Merge,
}

#[derive(EnumString, IntoStaticStr, Clone, Display)]
pub enum ParallelType {
    #[strum(serialize = "serial")]
    Serial,
    #[strum(serialize = "snapshot")]
    Snapshot,
    #[strum(serialize = "rdb_partition")]
    RdbPartition,
    #[strum(serialize = "rdb_merge")]
    RdbMerge,
    #[strum(serialize = "rdb_check")]
    RdbCheck,
    #[strum(serialize = "table")]
    Table,
    #[strum(serialize = "mongo")]
    Mongo,
    #[strum(serialize = "redis")]
    Redis,
    #[strum(serialize = "foxlake")]
    Foxlake,
}

#[derive(EnumString, IntoStaticStr, Clone, Display)]
pub enum PipelineType {
    #[strum(serialize = "basic")]
    Basic,
    #[strum(serialize = "http_server")]
    HttpServer,
}

#[derive(Clone, Debug, EnumString, IntoStaticStr, PartialEq, Default)]
pub enum ConflictPolicyEnum {
    #[strum(serialize = "ignore")]
    Ignore,
    #[default]
    #[strum(serialize = "interrupt")]
    Interrupt,
}

#[derive(Display, EnumString, IntoStaticStr, PartialEq)]
pub enum MetaCenterType {
    #[strum(serialize = "basic")]
    Basic,
    #[strum(serialize = "dbengine")]
    DbEngine,
}

#[derive(Debug, Clone, PartialEq, Eq, Display, EnumString, IntoStaticStr)]
pub enum TaskType {
    #[strum(serialize = "struct")]
    Struct,
    #[strum(serialize = "snapshot")]
    Snapshot,
    #[strum(serialize = "cdc")]
    Cdc,
    #[strum(serialize = "check")]
    Check,
}

#[derive(Display, EnumString, IntoStaticStr, PartialEq, Default)]
pub enum ResumeType {
    #[strum(serialize = "from_log")]
    FromLog,
    #[strum(serialize = "from_target")]
    FromTarget,
    #[strum(serialize = "from_db")]
    FromDB,
    #[default]
    #[strum(serialize = "dummy")]
    Dummy,
}

#[derive(Display, EnumString, IntoStaticStr, PartialEq, Default, Clone, Debug)]
pub enum RdbTransactionIsolation {
    #[strum(serialize = "read_uncommitted")]
    ReadUncommitted,
    #[strum(serialize = "read_committed")]
    ReadCommitted,
    #[strum(serialize = "repeatable_read")]
    RepeatableRead,
    #[strum(serialize = "serializable")]
    Serializable,
    #[default]
    #[strum(serialize = "default")]
    Default,
}

pub fn build_task_type(
    extract_type: &ExtractType,
    sink_type: &SinkType,
    checker_enabled: bool,
) -> Option<TaskType> {
    match (extract_type, sink_type, checker_enabled) {
        (ExtractType::Struct, SinkType::Struct, _) => Some(TaskType::Struct),
        (ExtractType::Snapshot, SinkType::Write, _) => Some(TaskType::Snapshot),
        (ExtractType::Cdc, SinkType::Write, _) => Some(TaskType::Cdc),
        (_, SinkType::Dummy, true) => Some(TaskType::Check),
        _ => None,
    }
}
