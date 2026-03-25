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

#[derive(Display, EnumString, IntoStaticStr, Debug, Clone, Hash, PartialEq, Eq)]
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

// TaskKind/TaskType cover only regular struct/snapshot/cdc flows, with optional check mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskKind {
    Struct,
    Snapshot,
    Cdc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CheckMode {
    Standalone,
    Inline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskType {
    pub kind: TaskKind,
    pub check: Option<CheckMode>,
}

impl TaskType {
    pub const fn new(kind: TaskKind, check: Option<CheckMode>) -> Self {
        Self { kind, check }
    }

    pub const fn has_check(&self) -> bool {
        self.check.is_some()
    }

    pub const fn is_inline_check(&self) -> bool {
        matches!(self.check, Some(CheckMode::Inline))
    }

    pub const fn is_cdc_inline_check(&self) -> bool {
        matches!(self.kind, TaskKind::Cdc) && self.is_inline_check()
    }
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
