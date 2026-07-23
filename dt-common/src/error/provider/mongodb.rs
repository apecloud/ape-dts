use mongodb::error::ErrorKind as MongoErrorKind;

use super::{
    super::{ClassifyError, DtErrorContext, ErrorCode, OriginError},
    classification::provider_context,
};

impl ClassifyError for mongodb::error::Error {
    fn classify(&self) -> DtErrorContext {
        let (code, provider_code) = classify_mongodb_kind(&self.kind);
        provider_context(code, OriginError::new("mongodb", provider_code))
    }
}

fn classify_mongodb_kind(kind: &MongoErrorKind) -> (Option<ErrorCode>, Option<String>) {
    match kind {
        MongoErrorKind::InvalidArgument { .. } => (None, None),
        MongoErrorKind::Authentication { .. } => (Some(ErrorCode::AuthenticationFailed), None),
        MongoErrorKind::InvalidTlsConfig { .. } => (Some(ErrorCode::TlsFailed), None),
        MongoErrorKind::Io(error) if is_timeout_kind(error.kind()) => {
            (Some(ErrorCode::ConnectionTimeout), None)
        }
        MongoErrorKind::Io(_)
        | MongoErrorKind::DnsResolve { .. }
        | MongoErrorKind::ConnectionPoolCleared { .. }
        | MongoErrorKind::ServerSelection { .. }
        | MongoErrorKind::Shutdown => (Some(ErrorCode::ConnectionFailed), None),
        MongoErrorKind::Command(error) => (
            classify_mongodb_command_code(error.code),
            Some(error.code.to_string()),
        ),
        _ => (None, None),
    }
}

fn classify_mongodb_command_code(code: i32) -> Option<ErrorCode> {
    match code {
        13 => Some(ErrorCode::PermissionDenied),
        18 => Some(ErrorCode::AuthenticationFailed),
        26 => Some(ErrorCode::ObjectNotFound),
        11000 | 11001 | 12582 => Some(ErrorCode::IntegrityViolation),
        _ => None,
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
            classify_mongodb_kind(&timeout).0,
            Some(ErrorCode::ConnectionTimeout)
        );
        for (provider_code, expected) in [
            (13, Some(ErrorCode::PermissionDenied)),
            (26, Some(ErrorCode::ObjectNotFound)),
            (11000, Some(ErrorCode::IntegrityViolation)),
            (99999, None),
        ] {
            assert_eq!(classify_mongodb_command_code(provider_code), expected);
        }
    }
}
