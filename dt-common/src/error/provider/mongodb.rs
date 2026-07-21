use mongodb::error::ErrorKind as MongoErrorKind;

use super::{
    super::{ErrorCode, OriginError},
    ProviderErrorClassification,
};

pub fn classify_mongodb_error(error: &mongodb::error::Error) -> ProviderErrorClassification {
    let (code, provider_code) = classify_mongodb_kind(&error.kind);
    ProviderErrorClassification::new(code, OriginError::new("mongodb", provider_code))
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
        assert_eq!(
            classify_mongodb_command_code(13),
            Some(ErrorCode::PermissionDenied)
        );
        assert_eq!(
            classify_mongodb_command_code(26),
            Some(ErrorCode::ObjectNotFound)
        );
        assert_eq!(
            classify_mongodb_command_code(11000),
            Some(ErrorCode::IntegrityViolation)
        );
        assert_eq!(classify_mongodb_command_code(99999), None);
    }
}
