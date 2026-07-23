use mysql_binlog_connector_rust::binlog_error::BinlogError;

use super::super::{ClassifyError, DtErrorContext, ErrorCode, OriginError};

impl ClassifyError for BinlogError {
    fn classify(&self) -> DtErrorContext {
        match self {
            Self::IoError(error) => {
                let code = if matches!(
                    error.kind(),
                    std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                ) {
                    ErrorCode::ConnectionTimeout
                } else {
                    ErrorCode::ConnectionFailed
                };
                mysql_context(code)
            }
            Self::ConnectError(message) if binlog_is_unavailable(message) => {
                mysql_context(ErrorCode::CheckpointReadFailed)
                    .message("The requested MySQL binlog is no longer available")
                    .hint(
                        "Start from a retained binlog position or take a new snapshot, then increase the source binlog retention period.",
                    )
                    .origin(OriginError::new("mysql", Some("1236")))
            }
            Self::ConnectError(_) => mysql_context(ErrorCode::ConnectionFailed),
            Self::InvalidGtid(_) => mysql_context(ErrorCode::InvalidConfig),
            _ => mysql_context(ErrorCode::StatementFailed),
        }
    }
}

fn mysql_context(code: ErrorCode) -> DtErrorContext {
    DtErrorContext::new()
        .code(code)
        .origin(OriginError::new("mysql", None::<String>))
}

fn binlog_is_unavailable(message: &str) -> bool {
    // v0.3.4 exposes MySQL error 1236 as ConnectError(String).
    let message = message.to_ascii_lowercase();
    message.contains("fatal error 1236")
        || message.contains("could not find first log file name")
        || message.contains("binlog has been purged")
        || message.contains("not in binlog index")
        || message.contains("start replication from impossible position")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_binlog_failures() {
        for (error, expected) in [
            (
                BinlogError::IoError(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "timed out",
                )),
                ErrorCode::ConnectionTimeout,
            ),
            (
                BinlogError::ConnectError("connection closed".to_string()),
                ErrorCode::ConnectionFailed,
            ),
            (
                BinlogError::InvalidGtid("invalid".to_string()),
                ErrorCode::InvalidConfig,
            ),
            (
                BinlogError::UnexpectedData("invalid event".to_string()),
                ErrorCode::StatementFailed,
            ),
        ] {
            assert_eq!(error.classify().error_code(), Some(expected));
        }

        let purged = BinlogError::ConnectError(
            "Could not find first log file name in binary log index file".to_string(),
        )
        .classify();
        assert_eq!(purged.error_code(), Some(ErrorCode::CheckpointReadFailed));
        assert_eq!(
            purged
                .origin_error()
                .and_then(|origin| origin.code.as_deref()),
            Some("1236")
        );
    }
}
