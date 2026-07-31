use std::error::Error as StdError;
use std::io::{Error, ErrorKind};

use super::{
    super::{ClassifyError, DtErrorContext, ErrorCode},
    classification::{classify_postgres_code, provider_context, provider_detail},
};

impl ClassifyError for tokio_postgres::Error {
    fn classify(&self) -> DtErrorContext {
        let provider_code = self
            .as_db_error()
            .map(|error| error.code().code().to_string());
        let code = if let Some(provider_code) = provider_code.as_deref() {
            classify_postgres_code(provider_code)
        } else if let Some(kind) = find_io_kind(self) {
            if matches!(kind, ErrorKind::TimedOut | ErrorKind::WouldBlock) {
                Some(ErrorCode::ConnectionTimeout)
            } else {
                Some(ErrorCode::ConnectionFailed)
            }
        } else {
            None
        };

        provider_context(code, provider_detail("postgres", provider_code, self))
    }
}

fn find_io_kind(error: &tokio_postgres::Error) -> Option<ErrorKind> {
    let mut cause: Option<&(dyn StdError + 'static)> = Some(error);
    while let Some(current) = cause {
        if let Some(io_error) = current.downcast_ref::<Error>() {
            return Some(io_error.kind());
        }
        cause = current.source();
    }
    None
}
