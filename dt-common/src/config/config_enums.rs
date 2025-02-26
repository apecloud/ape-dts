use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};

#[derive(
    Clone, Display, EnumString, IntoStaticStr, Debug, PartialEq, Eq, Default, Serialize, Deserialize,
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
    #[strum(serialize = "databend")]
    Databend,
}

#[derive(Display, EnumString, IntoStaticStr, Debug, Clone)]
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

#[derive(Display, EnumString, IntoStaticStr)]
pub enum SinkType {
    #[strum(serialize = "dummy")]
    Dummy,
    #[strum(serialize = "write")]
    Write,
    #[strum(serialize = "check")]
    Check,
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
