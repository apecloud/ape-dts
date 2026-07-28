use mysql_binlog_connector_rust::binlog_error::BinlogError;

use super::{
    super::{ClassifyError, DtErrorContext, ErrorCode},
    classification::provider_detail,
};

impl ClassifyError for BinlogError {
    fn classify(&self) -> DtErrorContext {
        let detail = provider_detail("mysql", diagnostic_code(self).map(str::to_string), self);
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
                mysql_context(code, detail)
            }
            Self::ConnectError(message) if binlog_is_unavailable(message) => {
                mysql_context(ErrorCode::CheckpointReadFailed, detail)
                    .with_message("The requested MySQL binlog is no longer available")
                    .with_hint(
                        "Start from a retained binlog position or take a new snapshot, then increase the source binlog retention period.",
                    )
            }
            Self::ConnectError(_) => mysql_context(ErrorCode::ConnectionFailed, detail),
            Self::InvalidGtid(_) => mysql_context(ErrorCode::InvalidConfig, detail),
            _ => mysql_context(ErrorCode::StatementFailed, detail),
        }
    }
}

fn mysql_context(code: ErrorCode, detail: String) -> DtErrorContext {
    DtErrorContext::new().with_code(code).with_detail(detail)
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

pub(super) fn diagnostic_code(error: &BinlogError) -> Option<&'static str> {
    match error {
        BinlogError::ConnectError(message) if binlog_is_unavailable(message) => Some("1236"),
        _ => None,
    }
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
    }
}
