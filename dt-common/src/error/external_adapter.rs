use mongodb::error::ErrorKind as MongoErrorKind;
use redis::ErrorKind as RedisErrorKind;

use super::{DtError, ErrorCode, OriginError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalErrorClassification {
    pub code: ErrorCode,
    pub origin: OriginError,
}

pub fn classify_redis_error(
    error: &redis::RedisError,
    fallback: ErrorCode,
) -> ExternalErrorClassification {
    let provider_code = error.code().map(str::to_string);
    let code = if error.is_timeout() {
        ErrorCode::ConnectionTimeout
    } else {
        match (error.kind(), error.code()) {
            (RedisErrorKind::AuthenticationFailed, _) | (_, Some("NOAUTH" | "WRONGPASS")) => {
                ErrorCode::AuthenticationFailed
            }
            (RedisErrorKind::ReadOnly, _) | (_, Some("NOPERM")) => ErrorCode::PermissionDenied,
            (RedisErrorKind::InvalidClientConfig, _) => ErrorCode::InvalidConfig,
            (
                RedisErrorKind::IoError
                | RedisErrorKind::ClusterDown
                | RedisErrorKind::MasterDown
                | RedisErrorKind::ClusterConnectionNotFound
                | RedisErrorKind::NoValidReplicasFoundBySentinel,
                _,
            ) => ErrorCode::ConnectionFailed,
            _ => fallback,
        }
    };

    ExternalErrorClassification {
        code,
        origin: OriginError::new("redis", provider_code),
    }
}

pub fn classify_mongodb_error(
    error: &mongodb::error::Error,
    fallback: ErrorCode,
) -> ExternalErrorClassification {
    let (code, provider_code) = classify_mongodb_kind(&error.kind, fallback);
    ExternalErrorClassification {
        code,
        origin: OriginError::new("mongodb", provider_code),
    }
}

#[track_caller]
pub fn dt_error_from_mongodb(error: mongodb::error::Error, fallback: ErrorCode) -> DtError {
    let classification = classify_mongodb_error(&error, fallback);
    DtError::new(classification.code)
        .origin(classification.origin)
        .source(error)
}

#[track_caller]
pub fn dt_error_from_redis(error: redis::RedisError, fallback: ErrorCode) -> DtError {
    let classification = classify_redis_error(&error, fallback);
    DtError::new(classification.code)
        .origin(classification.origin)
        .source(error)
}

fn classify_mongodb_kind(
    kind: &MongoErrorKind,
    fallback: ErrorCode,
) -> (ErrorCode, Option<String>) {
    match kind {
        MongoErrorKind::InvalidArgument { .. } => (fallback, None),
        MongoErrorKind::Authentication { .. } => (ErrorCode::AuthenticationFailed, None),
        MongoErrorKind::InvalidTlsConfig { .. } => (ErrorCode::TlsFailed, None),
        MongoErrorKind::Io(error) if is_timeout_kind(error.kind()) => {
            (ErrorCode::ConnectionTimeout, None)
        }
        MongoErrorKind::Io(_)
        | MongoErrorKind::DnsResolve { .. }
        | MongoErrorKind::ConnectionPoolCleared { .. }
        | MongoErrorKind::ServerSelection { .. }
        | MongoErrorKind::Shutdown => (ErrorCode::ConnectionFailed, None),
        MongoErrorKind::Command(error) => (
            classify_mongodb_command_code(error.code, fallback),
            Some(error.code.to_string()),
        ),
        _ => (fallback, None),
    }
}

fn classify_mongodb_command_code(code: i32, fallback: ErrorCode) -> ErrorCode {
    match code {
        13 => ErrorCode::PermissionDenied,
        18 => ErrorCode::AuthenticationFailed,
        26 => ErrorCode::ObjectNotFound,
        11000 | 11001 | 12582 => ErrorCode::IntegrityViolation,
        _ => fallback,
    }
}

fn is_timeout_kind(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    )
}

#[cfg(test)]
mod tests {
    use std::{io, sync::Arc};

    use super::*;

    #[test]
    fn classifies_redis_errors() {
        let auth = redis::RedisError::from((
            RedisErrorKind::AuthenticationFailed,
            "authentication failed",
        ));
        assert_eq!(
            classify_redis_error(&auth, ErrorCode::StatementFailed).code,
            ErrorCode::AuthenticationFailed
        );

        let invalid =
            redis::RedisError::from((RedisErrorKind::InvalidClientConfig, "invalid client config"));
        assert_eq!(
            classify_redis_error(&invalid, ErrorCode::StatementFailed).code,
            ErrorCode::InvalidConfig
        );

        let timeout = redis::RedisError::from(io::Error::from(io::ErrorKind::TimedOut));
        assert_eq!(
            classify_redis_error(&timeout, ErrorCode::StatementFailed).code,
            ErrorCode::ConnectionTimeout
        );

        let unavailable =
            redis::RedisError::from(io::Error::from(io::ErrorKind::ConnectionRefused));
        assert_eq!(
            classify_redis_error(&unavailable, ErrorCode::StatementFailed).code,
            ErrorCode::ConnectionFailed
        );
    }

    #[test]
    fn classifies_mongodb_transport_and_command_codes() {
        let timeout = MongoErrorKind::Io(Arc::new(io::Error::from(io::ErrorKind::TimedOut)));
        assert_eq!(
            classify_mongodb_kind(&timeout, ErrorCode::StatementFailed).0,
            ErrorCode::ConnectionTimeout
        );
        assert_eq!(
            classify_mongodb_command_code(13, ErrorCode::StatementFailed),
            ErrorCode::PermissionDenied
        );
        assert_eq!(
            classify_mongodb_command_code(26, ErrorCode::StatementFailed),
            ErrorCode::ObjectNotFound
        );
        assert_eq!(
            classify_mongodb_command_code(11000, ErrorCode::StatementFailed),
            ErrorCode::IntegrityViolation
        );
        assert_eq!(
            classify_mongodb_command_code(99999, ErrorCode::StatementFailed),
            ErrorCode::StatementFailed
        );
    }
}
