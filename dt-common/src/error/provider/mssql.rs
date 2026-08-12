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
        | TiberiusError::ParseInt(_)
        | TiberiusError::BulkInput(_) => ErrorCode::StatementFailed,
        #[allow(unreachable_patterns)]
        _ => ErrorCode::StatementFailed,
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
        207 | 208 | 2812 => ErrorCode::ObjectNotFound,
        911 | 4060 => ErrorCode::DatabaseNotFound,
        515 | 547 | 2601 | 2627 => ErrorCode::IntegrityViolation,
        _ => ErrorCode::StatementFailed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorReport;

    #[test]
    fn classifies_transport_and_client_errors() {
        let cases = [
            (
                TiberiusError::Io {
                    kind: ErrorKind::TimedOut,
                    message: "connection timed out".to_string(),
                },
                ErrorCode::ConnectionTimeout,
            ),
            (
                TiberiusError::Io {
                    kind: ErrorKind::ConnectionRefused,
                    message: "connection refused".to_string(),
                },
                ErrorCode::ConnectionFailed,
            ),
            (
                TiberiusError::Tls("invalid certificate".to_string()),
                ErrorCode::TlsFailed,
            ),
            (
                TiberiusError::Conversion("invalid value".into()),
                ErrorCode::StatementFailed,
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(error.classify().error_code(), Some(expected));
        }
    }

    #[test]
    fn classifies_sql_server_error_codes() {
        for (code, expected) in [
            (18456, ErrorCode::AuthenticationFailed),
            (229, ErrorCode::PermissionDenied),
            (208, ErrorCode::ObjectNotFound),
            (4060, ErrorCode::DatabaseNotFound),
            (2627, ErrorCode::IntegrityViolation),
            (1205, ErrorCode::StatementFailed),
        ] {
            assert_eq!(classify_mssql_code(code), expected);
        }
    }

    #[test]
    fn classifies_pool_timeout_and_nested_tiberius_error() {
        let timeout = MssqlPoolError::TimedOut;
        assert_eq!(
            timeout.classify().error_code(),
            Some(ErrorCode::ConnectionTimeout)
        );

        let nested = MssqlPoolError::User(Bb8TiberiusError::Tiberius(TiberiusError::Tls(
            "invalid certificate".to_string(),
        )));
        let report = ErrorReport::from_anyhow(&anyhow::Error::new(nested));
        assert_eq!(report.code, ErrorCode::TlsFailed);
        assert_eq!(
            report.details,
            [
                "mssql-pool: connection manager error",
                "mssql: Error forming TLS connection: invalid certificate",
            ]
        );
    }
}
