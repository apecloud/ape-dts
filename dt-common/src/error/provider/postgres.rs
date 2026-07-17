use std::error::Error as StdError;

use super::{
    super::{DtError, ErrorCode, OriginError},
    classification::classify_postgres_code,
    ProviderErrorClassification,
};

pub fn classify_tokio_postgres_error(
    error: &tokio_postgres::Error,
    fallback: ErrorCode,
) -> ProviderErrorClassification {
    let provider_code = error
        .as_db_error()
        .map(|error| error.code().code().to_string());
    let code = if let Some(provider_code) = provider_code.as_deref() {
        classify_postgres_code(provider_code, fallback)
    } else if let Some(kind) = find_io_kind(error) {
        if matches!(
            kind,
            std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
        ) {
            ErrorCode::ConnectionTimeout
        } else {
            ErrorCode::ConnectionFailed
        }
    } else {
        fallback
    };

    ProviderErrorClassification::new(code, OriginError::new("postgres", provider_code))
}

#[track_caller]
pub fn dt_error_from_tokio_postgres(error: tokio_postgres::Error, fallback: ErrorCode) -> DtError {
    let classification = classify_tokio_postgres_error(&error, fallback);
    DtError::new(classification.code)
        .origin(classification.origin)
        .source(error)
}

fn find_io_kind(error: &tokio_postgres::Error) -> Option<std::io::ErrorKind> {
    let mut cause: Option<&(dyn StdError + 'static)> = Some(error);
    while let Some(current) = cause {
        if let Some(io_error) = current.downcast_ref::<std::io::Error>() {
            return Some(io_error.kind());
        }
        cause = current.source();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::classify_postgres_code;
    use crate::error::ErrorCode;

    #[test]
    fn classifies_postgres_provider_codes() {
        assert_eq!(
            classify_postgres_code("28P01", ErrorCode::StatementFailed),
            ErrorCode::AuthenticationFailed
        );
        assert_eq!(
            classify_postgres_code("42501", ErrorCode::StatementFailed),
            ErrorCode::PermissionDenied
        );
        assert_eq!(
            classify_postgres_code("42P01", ErrorCode::StatementFailed),
            ErrorCode::ObjectNotFound
        );
        assert_eq!(
            classify_postgres_code("08006", ErrorCode::StatementFailed),
            ErrorCode::ConnectionFailed
        );
        assert_eq!(
            classify_postgres_code("23505", ErrorCode::StatementFailed),
            ErrorCode::IntegrityViolation
        );
    }
}
