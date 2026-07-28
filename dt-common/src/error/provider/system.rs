use super::super::{ClassifyError, DtErrorContext, ErrorCode};

impl ClassifyError for std::io::Error {
    fn classify(&self) -> DtErrorContext {
        DtErrorContext::new()
            .with_code(ErrorCode::IoFailed)
            .with_detail(self.to_string())
    }
}

impl ClassifyError for tokio::task::JoinError {
    fn classify(&self) -> DtErrorContext {
        DtErrorContext::new()
            .with_code(ErrorCode::WorkerFailed)
            .with_detail(self.to_string())
    }
}
