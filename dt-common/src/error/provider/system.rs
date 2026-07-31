use std::io::Error;

use tokio::task::JoinError;

use super::super::{ClassifyError, DtErrorContext, ErrorCode};

impl ClassifyError for Error {
    fn classify(&self) -> DtErrorContext {
        DtErrorContext::new()
            .with_code(ErrorCode::IoFailed)
            .with_detail(self.to_string())
    }
}

impl ClassifyError for JoinError {
    fn classify(&self) -> DtErrorContext {
        DtErrorContext::new()
            .with_code(ErrorCode::WorkerFailed)
            .with_detail(self.to_string())
    }
}
