use dt_common::error::{DtError, ErrorCode, Stage};

#[track_caller]
pub(crate) fn config(detail: impl Into<String>, operation: &'static str) -> DtError {
    DtError::new(ErrorCode::InvalidConfig)
        .detail(detail)
        .stage(Stage::Bootstrap)
        .operation(operation)
}

#[track_caller]
pub(crate) fn task(code: ErrorCode, detail: impl Into<String>, operation: &'static str) -> DtError {
    DtError::new(code)
        .detail(detail)
        .stage(Stage::Task)
        .operation(operation)
}
