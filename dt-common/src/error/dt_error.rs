use super::{ClassifyError, DtErrorContext, ErrorCode, OriginError, Stage};

#[derive(Debug, thiserror::Error)]
pub enum DtError {
    #[error("{0}")]
    MissingConfig(String),

    #[error("{0}")]
    MissingConfigItem(String),

    #[error("{0}")]
    InvalidConfig(String),

    #[error("{0}")]
    RedisInvalidConfig(String),

    #[error("{0}")]
    PostgresInvalidConfig(String),

    #[error("{0}")]
    MetricsInitializationFailed(String),

    #[error("{0}")]
    ConnectionFailed(String),

    #[error("{0}")]
    ConnectionTimeout(String),

    #[error("{0}")]
    TlsFailed(String),

    #[error("{0}")]
    AuthenticationFailed(String),

    #[error("{0}")]
    RedisAuthenticationFailed(String),

    #[error("{0}")]
    PermissionDenied(String),

    #[error("{0}")]
    PrerequisiteNotMet(String),

    #[error("{0}")]
    RedisTopology(String),

    #[error("{0}")]
    RedisPrerequisiteNotMet(String),

    #[error("{0}")]
    RedisUnsupportedVersion(String),

    #[error("{0}")]
    RedisInvariant(String),

    #[error("{0}")]
    UnsupportedDatabaseVersion(String),

    #[error("{0}")]
    MongoUnsupportedVersion(String),

    #[error("{0}")]
    CdcNotEnabled(String),

    #[error("{0}")]
    ReplicationCapacityExhausted(String),

    #[error("{0}")]
    UnsupportedTableStructure(String),

    #[error("{0}")]
    ObjectNotFound(String),

    #[error("{0}")]
    DatabaseNotFound(String),

    #[error("{0}")]
    MetadataReadFailed(String),

    #[error("{0}")]
    StatementFailed(String),

    #[error("{0}")]
    MongoStatementFailed(String),

    #[error("{0}")]
    PostgresStatementFailed(String),

    #[error("{0}")]
    IntegrityViolation(String),

    #[error("{0}")]
    CheckpointReadFailed(String),

    #[error("{0}")]
    PostgresCheckpointReadFailed(String),

    #[error("{0}")]
    MySqlBinlogUnavailable(String),

    #[error("{0}")]
    MySqlBinlogTableMapMissing(String),

    #[error("{0}")]
    MySqlBinlogDecode(String),

    #[error("{0}")]
    MySqlInvariant(String),

    #[error("{0}")]
    IoFailed(String),

    #[error("{0}")]
    WorkerFailed(String),

    #[error("{0}")]
    OperationInterrupted(String),

    #[error("{0}")]
    InvariantViolated(String),

    #[error("the configured source connection client is missing")]
    MissingSourceClient,

    #[error("the configured destination connection client is missing")]
    MissingDestinationClient,

    #[error("expected {0} connection client is missing")]
    MissingTaskClient(String),

    #[error("{detail}")]
    HttpRejected { status: u16, detail: String },

    #[error("{0}")]
    ClickHouseSourceMetadataMissing(String),

    #[error("parse Redis RDB error: {0}")]
    RedisRdbError(String),

    #[error("parse Redis command error: {0}")]
    RedisCmdError(String),

    #[error("parse Redis result error: {0}")]
    RedisResultError(String),

    #[error("{0}")]
    Unclassified(String),
}

impl DtError {
    pub fn invalid_config(detail: impl Into<String>) -> Self {
        Self::InvalidConfig(detail.into())
    }

    pub fn redis_rdb(detail: impl Into<String>) -> Self {
        Self::RedisRdbError(detail.into())
    }

    pub fn mongo_statement(detail: impl Into<String>) -> Self {
        Self::MongoStatementFailed(detail.into())
    }

    pub fn mysql_binlog_table_map_missing(table_id: u64, event_type: &'static str) -> Self {
        Self::MySqlBinlogTableMapMissing(format!(
            "the {event_type} event for source table ID {table_id} arrived before Ape-DTS received its table definition"
        ))
    }
}

impl ClassifyError for DtError {
    fn classify(&self) -> DtErrorContext {
        let code = match self {
            DtError::MissingConfig(_) => ErrorCode::MissingConfig,
            DtError::MissingConfigItem(_) => ErrorCode::MissingConfigItem,
            DtError::InvalidConfig(_)
            | DtError::RedisInvalidConfig(_)
            | DtError::PostgresInvalidConfig(_) => ErrorCode::InvalidConfig,
            DtError::MetricsInitializationFailed(_) => ErrorCode::InvalidConfig,
            DtError::ConnectionFailed(_) => ErrorCode::ConnectionFailed,
            DtError::ConnectionTimeout(_) => ErrorCode::ConnectionTimeout,
            DtError::TlsFailed(_) => ErrorCode::TlsFailed,
            DtError::AuthenticationFailed(_) | DtError::RedisAuthenticationFailed(_) => {
                ErrorCode::AuthenticationFailed
            }
            DtError::PermissionDenied(_) => ErrorCode::PermissionDenied,
            DtError::PrerequisiteNotMet(_) => ErrorCode::PrerequisiteNotMet,
            DtError::RedisTopology(_) | DtError::RedisPrerequisiteNotMet(_) => {
                ErrorCode::PrerequisiteNotMet
            }
            DtError::RedisUnsupportedVersion(_)
            | DtError::UnsupportedDatabaseVersion(_)
            | DtError::MongoUnsupportedVersion(_) => ErrorCode::UnsupportedDatabaseVersion,
            DtError::CdcNotEnabled(_) => ErrorCode::CdcNotEnabled,
            DtError::ReplicationCapacityExhausted(_) => ErrorCode::ReplicationCapacityExhausted,
            DtError::UnsupportedTableStructure(_) => ErrorCode::UnsupportedTableStructure,
            DtError::ObjectNotFound(_) => ErrorCode::ObjectNotFound,
            DtError::DatabaseNotFound(_) => ErrorCode::DatabaseNotFound,
            DtError::MetadataReadFailed(_) => ErrorCode::MetadataReadFailed,
            DtError::StatementFailed(_)
            | DtError::MongoStatementFailed(_)
            | DtError::PostgresStatementFailed(_)
            | DtError::RedisRdbError(_)
            | DtError::RedisCmdError(_)
            | DtError::RedisResultError(_) => ErrorCode::StatementFailed,
            DtError::IntegrityViolation(_) => ErrorCode::IntegrityViolation,
            DtError::CheckpointReadFailed(_) | DtError::PostgresCheckpointReadFailed(_) => {
                ErrorCode::CheckpointReadFailed
            }
            DtError::MySqlBinlogUnavailable(_) => ErrorCode::CheckpointReadFailed,
            DtError::MySqlBinlogTableMapMissing(_) | DtError::MySqlBinlogDecode(_) => {
                ErrorCode::StatementFailed
            }
            DtError::IoFailed(_) => ErrorCode::IoFailed,
            DtError::WorkerFailed(_) => ErrorCode::WorkerFailed,
            DtError::OperationInterrupted(_) => ErrorCode::OperationInterrupted,
            DtError::InvariantViolated(_)
            | DtError::RedisInvariant(_)
            | DtError::MySqlInvariant(_) => ErrorCode::InvariantViolated,
            DtError::MissingSourceClient
            | DtError::MissingDestinationClient
            | DtError::MissingTaskClient(_) => ErrorCode::InvariantViolated,
            DtError::HttpRejected { .. } => ErrorCode::StatementFailed,
            DtError::ClickHouseSourceMetadataMissing(_) => ErrorCode::ObjectNotFound,
            DtError::Unclassified(_) => ErrorCode::Unclassified,
        };
        let context = DtErrorContext::new().code(code);
        match self {
            DtError::MissingConfig(_)
            | DtError::MissingConfigItem(_)
            | DtError::InvalidConfig(_) => context.stage(Stage::Bootstrap),
            DtError::RedisInvalidConfig(_) => context
                .stage(Stage::Bootstrap)
                .endpoint(super::EndpointRole::Source)
                .origin(OriginError::new("redis", None::<String>)),
            DtError::PostgresInvalidConfig(_) => context
                .stage(Stage::Bootstrap)
                .origin(OriginError::new("postgres", None::<String>)),
            DtError::MetricsInitializationFailed(_) => context
                .message("Metrics configuration is invalid")
                .stage(Stage::Bootstrap),
            DtError::MissingSourceClient
            | DtError::MissingDestinationClient
            | DtError::MissingTaskClient(_) => context.stage(Stage::Task),
            DtError::RedisRdbError(_)
            | DtError::RedisCmdError(_)
            | DtError::RedisResultError(_)
            | DtError::RedisUnsupportedVersion(_)
            | DtError::RedisInvariant(_) => {
                context.origin(OriginError::new("redis", None::<String>))
            }
            DtError::RedisAuthenticationFailed(_) => {
                context.origin(OriginError::new("redis", None::<String>))
            }
            DtError::RedisTopology(_) => context
                .message("The Redis cluster topology is invalid or incomplete")
                .hint(
                    "Ensure all 16384 Redis cluster slots are assigned to stable master nodes, then retry.",
                )
                .origin(OriginError::new("redis", None::<String>)),
            DtError::RedisPrerequisiteNotMet(_) => {
                context.origin(OriginError::new("redis", None::<String>))
            }
            DtError::MySqlBinlogUnavailable(_) => context
                .message("The requested MySQL binlog is no longer available")
                .hint(
                    "Start from a retained binlog position or take a new snapshot, then increase the source binlog retention period.",
                )
                .origin(OriginError::new("mysql", Some("1236"))),
            DtError::MySqlBinlogTableMapMissing(_) => context
                .message("A MySQL row event could not be decoded")
                .hint(
                    "Restart from an earlier binlog position so Ape-DTS can reload the table definition. If it repeats, check binlog retention and the source database logs.",
                )
                .origin(OriginError::new("mysql", None::<String>)),
            DtError::MySqlBinlogDecode(_) => context
                .message("A MySQL row event could not be decoded")
                .hint(
                    "Restart from an earlier binlog position. If it repeats, check binlog integrity and the source database logs.",
                )
                .origin(OriginError::new("mysql", None::<String>)),
            DtError::MySqlInvariant(_) => {
                context.origin(OriginError::new("mysql", None::<String>))
            }
            DtError::HttpRejected { status, .. } => context.origin(OriginError::new(
                "http",
                Some(status.to_string()),
            )),
            DtError::ClickHouseSourceMetadataMissing(_) => context
                .message(
                    "Source table metadata is unavailable for ClickHouse structure migration",
                )
                .hint(
                    "Verify that the source table still exists and rerun structure migration. If it repeats, contact support with the task ID and error code.",
                )
                .origin(OriginError::new("clickhouse", None::<String>)),
            DtError::MongoStatementFailed(_) | DtError::MongoUnsupportedVersion(_) => {
                context.origin(OriginError::new("mongodb", None::<String>))
            }
            DtError::PostgresStatementFailed(_) | DtError::PostgresCheckpointReadFailed(_) => {
                context.origin(OriginError::new("postgres", None::<String>))
            }
            _ => context,
        }
    }
}
