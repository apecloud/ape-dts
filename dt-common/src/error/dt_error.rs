use crate::config::config_enums::DbType;

use super::{ClassifyError, DtErrorContext, ErrorCode, Stage};

#[derive(Clone, Debug, thiserror::Error)]
pub enum DtError {
    #[error("{0}")]
    MissingConfig(String),

    #[error("{0}")]
    MissingConfigItem(String),

    #[error("{0}")]
    InvalidConfig(String),

    #[error("{} invalid configuration: {1}", .0.diagnostic_name())]
    DatabaseInvalidConfig(DbType, String),

    #[error("{0}")]
    MetricsInitializationFailed(String),

    #[error("{0}")]
    ConnectionFailed(String),

    #[error("{} connection failed: {1}", .0.diagnostic_name())]
    DatabaseConnectionFailed(DbType, String),

    #[error("{0}")]
    ConnectionTimeout(String),

    #[error("{} connection timed out: {1}", .0.diagnostic_name())]
    DatabaseConnectionTimeout(DbType, String),

    #[error("{0}")]
    TlsFailed(String),

    #[error("{} TLS setup failed: {1}", .0.diagnostic_name())]
    DatabaseTlsFailed(DbType, String),

    #[error("{0}")]
    AuthenticationFailed(String),

    #[error("{} authentication failed: {1}", .0.diagnostic_name())]
    DatabaseAuthenticationFailed(DbType, String),

    #[error("{0}")]
    PermissionDenied(String),

    #[error("{0}")]
    PrerequisiteNotMet(String),

    #[error("Redis cluster topology is invalid: {0}")]
    RedisTopology(String),

    #[error("Redis prerequisite is not met: {0}")]
    RedisPrerequisiteNotMet(String),

    #[error("{} database version is unsupported: {1}", .0.diagnostic_name())]
    UnsupportedDatabaseVersion(DbType, String),

    #[error("{} invariant was violated: {1}", .0.diagnostic_name())]
    DatabaseInvariant(DbType, String),

    #[error("{0}")]
    CdcNotEnabled(String),

    #[error("{0}")]
    ReplicationCapacityExhausted(String),

    #[error("{0}")]
    UnsupportedTableStructure(String),

    #[error("{} table structure is unsupported: {1}", .0.diagnostic_name())]
    DatabaseUnsupportedTableStructure(DbType, String),

    #[error("{0}")]
    ObjectNotFound(String),

    #[error("{} object was not found: {1}", .0.diagnostic_name())]
    DatabaseObjectNotFound(DbType, String),

    #[error("{} database was not found: {1}", .0.diagnostic_name())]
    DatabaseNotFound(DbType, String),

    #[error("{0}")]
    MetadataReadFailed(String),

    #[error("{0}")]
    StatementFailed(String),

    #[error("{} statement execution failed: {1}", .0.diagnostic_name())]
    DatabaseStatementFailed(DbType, String),

    #[error("{0}")]
    IntegrityViolation(String),

    #[error("{0}")]
    CheckpointReadFailed(String),

    #[error("{} checkpoint read failed: {1}", .0.diagnostic_name())]
    DatabaseCheckpointReadFailed(DbType, String),

    #[error("MySQL binlog is unavailable: {0}")]
    MySqlBinlogUnavailable(String),

    #[error("MySQL binlog table map is missing: {0}")]
    MySqlBinlogTableMapMissing(String),

    #[error("MySQL binlog event decoding failed: {0}")]
    MySqlBinlogDecode(String),

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

    #[error("expected {} connection client is missing", .0.diagnostic_name())]
    MissingTaskClient(DbType),

    #[error("HTTP request was rejected with status {status}: {detail}")]
    HttpRejected { status: u16, detail: String },

    #[error("{} source metadata was not found: {1}", .0.diagnostic_name())]
    DatabaseMetadataNotFound(DbType, String),

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
        Self::DatabaseStatementFailed(DbType::Mongo, detail.into())
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
            DtError::InvalidConfig(_) | DtError::DatabaseInvalidConfig(_, _) => {
                ErrorCode::InvalidConfig
            }
            DtError::MetricsInitializationFailed(_) => ErrorCode::InvalidConfig,
            DtError::ConnectionFailed(_) | DtError::DatabaseConnectionFailed(_, _) => {
                ErrorCode::ConnectionFailed
            }
            DtError::ConnectionTimeout(_) | DtError::DatabaseConnectionTimeout(_, _) => {
                ErrorCode::ConnectionTimeout
            }
            DtError::TlsFailed(_) | DtError::DatabaseTlsFailed(_, _) => ErrorCode::TlsFailed,
            DtError::AuthenticationFailed(_) | DtError::DatabaseAuthenticationFailed(_, _) => {
                ErrorCode::AuthenticationFailed
            }
            DtError::PermissionDenied(_) => ErrorCode::PermissionDenied,
            DtError::PrerequisiteNotMet(_) => ErrorCode::PrerequisiteNotMet,
            DtError::RedisTopology(_) | DtError::RedisPrerequisiteNotMet(_) => {
                ErrorCode::PrerequisiteNotMet
            }
            DtError::UnsupportedDatabaseVersion(_, _) => ErrorCode::UnsupportedDatabaseVersion,
            DtError::CdcNotEnabled(_) => ErrorCode::CdcNotEnabled,
            DtError::ReplicationCapacityExhausted(_) => ErrorCode::ReplicationCapacityExhausted,
            DtError::UnsupportedTableStructure(_)
            | DtError::DatabaseUnsupportedTableStructure(_, _) => {
                ErrorCode::UnsupportedTableStructure
            }
            DtError::ObjectNotFound(_)
            | DtError::DatabaseObjectNotFound(_, _)
            | DtError::DatabaseMetadataNotFound(_, _) => ErrorCode::ObjectNotFound,
            DtError::DatabaseNotFound(_, _) => ErrorCode::DatabaseNotFound,
            DtError::MetadataReadFailed(_) => ErrorCode::MetadataReadFailed,
            DtError::StatementFailed(_)
            | DtError::DatabaseStatementFailed(_, _)
            | DtError::RedisRdbError(_)
            | DtError::RedisCmdError(_)
            | DtError::RedisResultError(_) => ErrorCode::StatementFailed,
            DtError::IntegrityViolation(_) => ErrorCode::IntegrityViolation,
            DtError::CheckpointReadFailed(_) | DtError::DatabaseCheckpointReadFailed(_, _) => {
                ErrorCode::CheckpointReadFailed
            }
            DtError::MySqlBinlogUnavailable(_) => ErrorCode::CheckpointReadFailed,
            DtError::MySqlBinlogTableMapMissing(_) | DtError::MySqlBinlogDecode(_) => {
                ErrorCode::StatementFailed
            }
            DtError::IoFailed(_) => ErrorCode::IoFailed,
            DtError::WorkerFailed(_) => ErrorCode::WorkerFailed,
            DtError::OperationInterrupted(_) => ErrorCode::OperationInterrupted,
            DtError::InvariantViolated(_) | DtError::DatabaseInvariant(_, _) => {
                ErrorCode::InvariantViolated
            }
            DtError::MissingSourceClient
            | DtError::MissingDestinationClient
            | DtError::MissingTaskClient(_) => ErrorCode::InvariantViolated,
            DtError::HttpRejected { .. } => ErrorCode::StatementFailed,
            DtError::Unclassified(_) => ErrorCode::Unclassified,
        };
        let context = DtErrorContext::new()
            .with_code(code)
            .with_detail(self.to_string());
        match self {
            DtError::MissingConfig(_)
            | DtError::MissingConfigItem(_)
            | DtError::InvalidConfig(_) => context.with_stage(Stage::Bootstrap),
            DtError::DatabaseInvalidConfig(_, _) => context.with_stage(Stage::Bootstrap),
            DtError::MetricsInitializationFailed(_) => context
                .with_message("Metrics configuration is invalid")
                .with_stage(Stage::Bootstrap),
            DtError::MissingSourceClient
            | DtError::MissingDestinationClient
            | DtError::MissingTaskClient(_) => context.with_stage(Stage::Task),
            DtError::RedisRdbError(_)
            | DtError::RedisCmdError(_)
            | DtError::RedisResultError(_)
            | DtError::RedisPrerequisiteNotMet(_) => context,
            DtError::RedisTopology(_) => context
                .with_message("The Redis cluster topology is invalid or incomplete")
                .with_hint(
                    "Ensure all 16384 Redis cluster slots are assigned to stable master nodes, then retry.",
                ),
            DtError::MySqlBinlogUnavailable(_) => context
                .with_message("The requested MySQL binlog is no longer available")
                .with_hint(
                    "Start from a retained binlog position or take a new snapshot, then increase the source binlog retention period.",
                ),
            DtError::MySqlBinlogTableMapMissing(_) => context
                .with_message("A MySQL row event could not be decoded")
                .with_hint(
                    "Restart from an earlier binlog position so Ape-DTS can reload the table definition. If it repeats, check binlog retention and the source database logs.",
                ),
            DtError::MySqlBinlogDecode(_) => context
                .with_message("A MySQL row event could not be decoded")
                .with_hint(
                    "Restart from an earlier binlog position. If it repeats, check binlog integrity and the source database logs.",
                ),
            DtError::DatabaseMetadataNotFound(db_type, _) => context
                .with_message(format!(
                    "Source table metadata is unavailable for {} structure migration",
                    db_type.diagnostic_name()
                ))
                .with_hint(
                    "Verify that the source table still exists and rerun structure migration. If it repeats, contact support with the task ID and error code.",
                ),
            _ => context,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classification_includes_full_display_as_detail() {
        let error = DtError::HttpRejected {
            status: 403,
            detail: "request is not authorized".to_string(),
        };

        let context = error.classify();
        let expected_detail = error.to_string();

        assert_eq!(context.code, Some(ErrorCode::StatementFailed));
        assert_eq!(context.detail.as_deref(), Some(expected_detail.as_str()));
    }
}
