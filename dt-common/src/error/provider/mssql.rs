use std::io::ErrorKind;

use bb8::RunError;
use bb8_tiberius::Error as Bb8TiberiusError;
use tiberius::error::Error as TiberiusError;

use super::{
    super::{ClassifyError, DtErrorContext, ErrorCode},
    classification::{provider_context, provider_detail},
};

type MssqlPoolError = RunError<Bb8TiberiusError>;

pub fn classify_mssql_error(error: &TiberiusError) -> DtErrorContext {
    let code = match error {
        TiberiusError::Io { kind, .. } if is_timeout(*kind) => ErrorCode::ConnectionTimeout,
        TiberiusError::Io { .. } | TiberiusError::Protocol(_) | TiberiusError::Routing { .. } => {
            ErrorCode::ConnectionFailed
        }
        TiberiusError::Tls(_) => ErrorCode::TlsFailed,
        TiberiusError::Server(error) => classify_mssql_code(error.code()),
        TiberiusError::Encoding(_)
        | TiberiusError::Conversion(_)
        | TiberiusError::Utf8
        | TiberiusError::Utf16
        | TiberiusError::ParseInt(_) => ErrorCode::DataDecodeFailed,
        TiberiusError::BulkInput(_) => ErrorCode::DatabaseOperationFailed,
        #[allow(unreachable_patterns)]
        _ => ErrorCode::DatabaseOperationFailed,
    };

    provider_context(
        Some(code),
        provider_detail("mssql", error.code().map(|code| code.to_string()), error),
    )
}

impl ClassifyError for TiberiusError {
    fn classify(&self) -> DtErrorContext {
        classify_mssql_error(self)
    }
}

impl ClassifyError for Bb8TiberiusError {
    fn classify(&self) -> DtErrorContext {
        match self {
            Self::Tiberius(error) => error.classify(),
            Self::Io(error) => {
                let code = if is_timeout(error.kind()) {
                    ErrorCode::ConnectionTimeout
                } else {
                    ErrorCode::ConnectionFailed
                };
                provider_context(Some(code), provider_detail("mssql", None, error))
            }
        }
    }
}

impl ClassifyError for MssqlPoolError {
    fn classify(&self) -> DtErrorContext {
        match self {
            Self::TimedOut => provider_context(
                Some(ErrorCode::ConnectionTimeout),
                "mssql-pool: timed out waiting for a connection",
            ),
            Self::User(_) => provider_context(None, "mssql-pool: connection manager error"),
        }
    }
}

fn is_timeout(kind: ErrorKind) -> bool {
    matches!(kind, ErrorKind::TimedOut | ErrorKind::WouldBlock)
}

fn classify_mssql_code(code: u32) -> ErrorCode {
    match code {
        18452 | 18456 => ErrorCode::AuthenticationFailed,
        229 | 230 | 262 | 297 => ErrorCode::PermissionDenied,
        207 | 208 | 2812 | 3701 | 4902 => ErrorCode::ObjectNotFound,
        911 | 4060 => ErrorCode::DatabaseNotFound,
        515 | 547 | 2601 | 2627 => ErrorCode::IntegrityViolation,
        1205 | 1222 | 3960 => ErrorCode::DatabaseOperationConflict,
        701 | 802 | 1101 | 1105 | 8645 | 10928 | 10929 | 40501 | 49918 | 49919 | 49920 => {
            ErrorCode::ResourceExhausted
        }
        40197 | 40613 => ErrorCode::ConnectionFailed,
        _ => ErrorCode::DatabaseOperationFailed,
    }
}
