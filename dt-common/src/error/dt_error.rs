use super::{ClassifyError, DtErrorContext, ErrorCode, OriginError};

#[derive(Debug, thiserror::Error)]
pub enum DtError {
    #[error("{0}")]
    MissingConfig(String),

    #[error("{0}")]
    MissingConfigItem(String),

    #[error("{0}")]
    InvalidConfig(String),

    #[error("{0}")]
    ConnectionFailed(String),

    #[error("{0}")]
    ConnectionTimeout(String),

    #[error("{0}")]
    TlsFailed(String),

    #[error("{0}")]
    AuthenticationFailed(String),

    #[error("{0}")]
    PermissionDenied(String),

    #[error("{0}")]
    PrerequisiteNotMet(String),

    #[error("{0}")]
    UnsupportedDatabaseVersion(String),

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
    IntegrityViolation(String),

    #[error("{0}")]
    CheckpointReadFailed(String),

    #[error("{0}")]
    IoFailed(String),

    #[error("{0}")]
    WorkerFailed(String),

    #[error("{0}")]
    OperationInterrupted(String),

    #[error("{0}")]
    InvariantViolated(String),

    #[error("parse Redis RDB error: {0}")]
    RedisRdbError(String),

    #[error("parse Redis command error: {0}")]
    RedisCmdError(String),

    #[error("parse Redis result error: {0}")]
    RedisResultError(String),

    #[error("{0}")]
    Unclassified(String),
}

impl ClassifyError for DtError {
    fn classify(&self) -> DtErrorContext {
        let code = match self {
            DtError::MissingConfig(_) => ErrorCode::MissingConfig,
            DtError::MissingConfigItem(_) => ErrorCode::MissingConfigItem,
            DtError::InvalidConfig(_) => ErrorCode::InvalidConfig,
            DtError::ConnectionFailed(_) => ErrorCode::ConnectionFailed,
            DtError::ConnectionTimeout(_) => ErrorCode::ConnectionTimeout,
            DtError::TlsFailed(_) => ErrorCode::TlsFailed,
            DtError::AuthenticationFailed(_) => ErrorCode::AuthenticationFailed,
            DtError::PermissionDenied(_) => ErrorCode::PermissionDenied,
            DtError::PrerequisiteNotMet(_) => ErrorCode::PrerequisiteNotMet,
            DtError::UnsupportedDatabaseVersion(_) => ErrorCode::UnsupportedDatabaseVersion,
            DtError::CdcNotEnabled(_) => ErrorCode::CdcNotEnabled,
            DtError::ReplicationCapacityExhausted(_) => ErrorCode::ReplicationCapacityExhausted,
            DtError::UnsupportedTableStructure(_) => ErrorCode::UnsupportedTableStructure,
            DtError::ObjectNotFound(_) => ErrorCode::ObjectNotFound,
            DtError::DatabaseNotFound(_) => ErrorCode::DatabaseNotFound,
            DtError::MetadataReadFailed(_) => ErrorCode::MetadataReadFailed,
            DtError::StatementFailed(_)
            | DtError::RedisRdbError(_)
            | DtError::RedisCmdError(_)
            | DtError::RedisResultError(_) => ErrorCode::StatementFailed,
            DtError::IntegrityViolation(_) => ErrorCode::IntegrityViolation,
            DtError::CheckpointReadFailed(_) => ErrorCode::CheckpointReadFailed,
            DtError::IoFailed(_) => ErrorCode::IoFailed,
            DtError::WorkerFailed(_) => ErrorCode::WorkerFailed,
            DtError::OperationInterrupted(_) => ErrorCode::OperationInterrupted,
            DtError::InvariantViolated(_) => ErrorCode::InvariantViolated,
            DtError::Unclassified(_) => ErrorCode::Unclassified,
        };
        let context = DtErrorContext::new().code(code);
        match self {
            DtError::RedisRdbError(_)
            | DtError::RedisCmdError(_)
            | DtError::RedisResultError(_) => {
                context.origin(OriginError::new("redis", None::<String>))
            }
            _ => context,
        }
    }
}
