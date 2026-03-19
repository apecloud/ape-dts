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

    pub const fn is_standalone_check(&self) -> bool {
        matches!(self.check, Some(CheckMode::Standalone))
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

pub fn write_sink_supports_inline_checker(sink_db_type: &DbType) -> bool {
    matches!(sink_db_type, DbType::Mysql | DbType::Pg | DbType::Mongo)
}

fn task_kind_from_extract_type(extract_type: &ExtractType) -> Option<TaskKind> {
    match extract_type {
        ExtractType::Struct => Some(TaskKind::Struct),
        ExtractType::Snapshot => Some(TaskKind::Snapshot),
        ExtractType::Cdc => Some(TaskKind::Cdc),
        _ => None,
    }
}

fn supports_task_without_checker(kind: TaskKind, sink_type: &SinkType) -> bool {
    matches!(
        (kind, sink_type),
        (TaskKind::Struct, SinkType::Struct)
            | (TaskKind::Snapshot, SinkType::Write)
            | (TaskKind::Cdc, SinkType::Write)
    )
}

fn supports_standalone_checker(kind: TaskKind, sink_type: &SinkType) -> bool {
    matches!(
        (kind, sink_type),
        (TaskKind::Struct, SinkType::Dummy) | (TaskKind::Snapshot, SinkType::Dummy)
    )
}

fn supports_inline_checker(kind: TaskKind, sink_type: &SinkType, sink_db_type: &DbType) -> bool {
    match (kind, sink_type, sink_db_type) {
        (TaskKind::Snapshot, SinkType::Write, db_type) => {
            write_sink_supports_inline_checker(db_type)
        }
        (TaskKind::Cdc, SinkType::Write, DbType::Mysql | DbType::Pg) => true,
        _ => false,
    }
}

pub fn build_task_type(
    extract_type: &ExtractType,
    sink_type: &SinkType,
    sink_db_type: &DbType,
    checker_enabled: bool,
) -> Option<TaskType> {
    let kind = task_kind_from_extract_type(extract_type)?;

    let check = if !checker_enabled {
        if supports_task_without_checker(kind, sink_type) {
            None
        } else {
            return None;
        }
    } else if supports_standalone_checker(kind, sink_type) {
        Some(CheckMode::Standalone)
    } else if supports_inline_checker(kind, sink_type, sink_db_type) {
        Some(CheckMode::Inline)
    } else {
        return None;
    };

    Some(TaskType::new(kind, check))
}

#[cfg(test)]
mod tests {
    use super::{build_task_type, CheckMode, DbType, ExtractType, SinkType, TaskKind, TaskType};

    #[test]
    fn cdc_write_checker_support_matrix_excludes_mongo() {
        assert_eq!(
            build_task_type(&ExtractType::Cdc, &SinkType::Write, &DbType::Mysql, true),
            Some(TaskType::new(TaskKind::Cdc, Some(CheckMode::Inline)))
        );
        assert_eq!(
            build_task_type(&ExtractType::Cdc, &SinkType::Write, &DbType::Pg, true),
            Some(TaskType::new(TaskKind::Cdc, Some(CheckMode::Inline)))
        );
        assert_eq!(
            build_task_type(&ExtractType::Cdc, &SinkType::Write, &DbType::Mongo, true),
            None
        );
        assert_eq!(
            build_task_type(&ExtractType::Cdc, &SinkType::Write, &DbType::Redis, true),
            None
        );
    }

    #[test]
    fn snapshot_write_checker_support_matrix_includes_mongo() {
        assert_eq!(
            build_task_type(&ExtractType::Snapshot, &SinkType::Write, &DbType::Mongo, true),
            Some(TaskType::new(TaskKind::Snapshot, Some(CheckMode::Inline)))
        );
    }
}
