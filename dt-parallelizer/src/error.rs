use dt_common::error::{DtError, ErrorCode, Stage};

#[track_caller]
pub(crate) fn worker(error: tokio::task::JoinError, operation: &'static str) -> DtError {
    DtError::new(ErrorCode::WorkerFailed)
        .stage(Stage::Parallelizer)
        .operation(operation)
        .source(error)
}

#[track_caller]
pub(crate) fn invariant(operation: &'static str) -> DtError {
    DtError::new(ErrorCode::InvariantViolated)
        .stage(Stage::Parallelizer)
        .operation(operation)
}

#[track_caller]
pub(crate) fn invalid_config(operation: &'static str) -> DtError {
    DtError::new(ErrorCode::InvalidConfig)
        .stage(Stage::Parallelizer)
        .operation(operation)
}
