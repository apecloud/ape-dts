use std::io::ErrorKind;

use mongodb::error::ErrorKind as MongoErrorKind;

use super::{
    super::{ClassifyError, DtErrorContext, ErrorCode},
    classification::{provider_context, provider_detail},
};

impl ClassifyError for mongodb::error::Error {
    fn classify(&self) -> DtErrorContext {
        let provider_code = match self.kind.as_ref() {
            MongoErrorKind::Command(command) => Some(command.code.to_string()),
            _ => None,
        };
        provider_context(
            classify_mongodb_kind(&self.kind),
            provider_detail("mongodb", provider_code, self),
        )
    }
}

fn classify_mongodb_kind(kind: &MongoErrorKind) -> Option<ErrorCode> {
    match kind {
        MongoErrorKind::InvalidArgument { .. } => None,
        MongoErrorKind::Authentication { .. } => Some(ErrorCode::AuthenticationFailed),
        MongoErrorKind::InvalidTlsConfig { .. } => Some(ErrorCode::TlsFailed),
        MongoErrorKind::Io(error) if is_timeout_kind(error.kind()) => {
            Some(ErrorCode::ConnectionTimeout)
        }
        MongoErrorKind::Io(_)
        | MongoErrorKind::DnsResolve { .. }
        | MongoErrorKind::ConnectionPoolCleared { .. }
        | MongoErrorKind::ServerSelection { .. }
        | MongoErrorKind::Shutdown => Some(ErrorCode::ConnectionFailed),
        MongoErrorKind::Command(error) => classify_mongodb_command_code(error.code),
        _ => None,
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

fn is_timeout_kind(kind: ErrorKind) -> bool {
    matches!(kind, ErrorKind::TimedOut | ErrorKind::WouldBlock)
}

#[cfg(test)]
mod tests {
    use std::{io, sync::Arc};

    use super::*;

    #[test]
    fn classifies_mongodb_transport_and_command_codes() {
        let timeout = MongoErrorKind::Io(Arc::new(io::Error::from(io::ErrorKind::TimedOut)));
        assert_eq!(
            classify_mongodb_kind(&timeout),
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
