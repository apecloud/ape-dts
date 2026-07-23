use super::super::{ClassifyError, DtErrorContext, ErrorCode};

impl ClassifyError for std::io::Error {
    fn classify(&self) -> DtErrorContext {
        DtErrorContext::new().with_code(ErrorCode::IoFailed)
    }
}

impl ClassifyError for tokio::task::JoinError {
    fn classify(&self) -> DtErrorContext {
        DtErrorContext::new().with_code(ErrorCode::WorkerFailed)
    }
}
