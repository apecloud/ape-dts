use mongodb::error::ErrorKind as MongoErrorKind;

use super::{
    super::{DtError, ErrorCode, OriginError},
    ProviderErrorClassification,
};

pub fn classify_mongodb_error(
    error: &mongodb::error::Error,
    fallback: ErrorCode,
) -> ProviderErrorClassification {
    let (code, provider_code) = classify_mongodb_kind(&error.kind, fallback);
    ProviderErrorClassification::new(code, OriginError::new("mongodb", provider_code))
}

#[track_caller]
pub fn dt_error_from_mongodb(error: mongodb::error::Error, fallback: ErrorCode) -> DtError {
    let classification = classify_mongodb_error(&error, fallback);
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
