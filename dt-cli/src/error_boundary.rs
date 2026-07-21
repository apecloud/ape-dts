use std::error::Error as StdError;

use dt_common::error::{DtError, DtErrorContext, DtErrorContextExt, ErrorCode, Stage};
fn scoped(context: DtErrorContext, stage: Stage) -> DtErrorContext {
    DtErrorContext::new().stage(stage).inherit(context)
}
pub(crate) fn config_error(detail: impl Into<String>) -> anyhow::Error {
    DtError::ConfigError(detail.into())
        .with_code(ErrorCode::InvalidConfig)
        .with_stage(Stage::Bootstrap)
}
pub(crate) fn config_source<E>(detail: impl Into<String>, error: E) -> anyhow::Error
where
    E: StdError + Send + Sync + 'static,
{
    scoped(
        DtErrorContext::new()
            .code(ErrorCode::InvalidConfig)
            .detail(detail),
        Stage::Bootstrap,
    )
    .attach(error)
}
pub(crate) fn task_error(code: ErrorCode, detail: impl Into<String>) -> anyhow::Error {
    DtError::Unexpected(detail.into())
        .with_code(code)
        .with_stage(Stage::Task)
}
pub(crate) fn task_source<E>(code: ErrorCode, detail: impl Into<String>, error: E) -> anyhow::Error
where
    E: StdError + Send + Sync + 'static,
{
    scoped(DtErrorContext::new().code(code).detail(detail), Stage::Task).attach(error)
}
