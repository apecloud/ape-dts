use std::error::Error as StdError;

use dt_common::error::{DtError, EndpointRole, ErrorCode, OriginError, Stage};

#[track_caller]
pub(super) fn postgres(
    error: tokio_postgres::Error,
    fallback: ErrorCode,
    operation: &'static str,
) -> DtError {
    let provider_code = error
        .as_db_error()
        .map(|error| error.code().code().to_string());
    let code = if let Some(provider_code) = provider_code.as_deref() {
        classify_provider_code(provider_code, fallback)
    } else if let Some(kind) = find_io_kind(&error) {
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

    DtError::new(code)
        .stage(Stage::Extractor)
        .operation(operation)
        .endpoint(EndpointRole::Source)
        .origin(OriginError::new("postgres", provider_code))
        .source(error)
}

fn classify_provider_code(code: &str, fallback: ErrorCode) -> ErrorCode {
    match code {
        "42P01" | "42703" | "42704" => ErrorCode::ObjectNotFound,
        "3D000" => ErrorCode::DatabaseNotFound,
        code if code.starts_with("28") => ErrorCode::AuthenticationFailed,
        "42501" => ErrorCode::PermissionDenied,
        code if code.starts_with("08") => ErrorCode::ConnectionFailed,
        code if code.starts_with("23") => ErrorCode::IntegrityViolation,
        _ => fallback,
    }
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
    use super::*;

    #[test]
    fn classifies_postgres_provider_codes() {
        assert_eq!(
            classify_provider_code("28P01", ErrorCode::StatementFailed),
            ErrorCode::AuthenticationFailed
        );
        assert_eq!(
            classify_provider_code("42501", ErrorCode::StatementFailed),
            ErrorCode::PermissionDenied
        );
        assert_eq!(
            classify_provider_code("42P01", ErrorCode::StatementFailed),
            ErrorCode::ObjectNotFound
        );
        assert_eq!(
            classify_provider_code("08006", ErrorCode::StatementFailed),
            ErrorCode::ConnectionFailed
        );
    }
}
