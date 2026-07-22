use std::error::Error as StdError;

use dt_common::error::{DtError, DtErrorContext, DtErrorContextExt, ErrorCode, Stage};
pub(crate) fn config_error(detail: impl Into<String>) -> anyhow::Error {
    DtError::ConfigError(detail.into())
        .with_code(ErrorCode::InvalidConfig)
        .with_stage(Stage::Bootstrap)
}
pub(crate) fn config_source<E>(detail: impl Into<String>, error: E) -> anyhow::Error
where
    E: StdError + Send + Sync + 'static,
{
    DtErrorContext::new()
        .code(ErrorCode::InvalidConfig)
        .attach(error)
        .with_stage(Stage::Bootstrap)
        .context(detail.into())
}
pub(crate) fn task_error(code: ErrorCode, detail: impl Into<String>) -> anyhow::Error {
    DtError::General(detail.into())
        .with_code(code)
        .with_stage(Stage::Task)
}
pub(crate) fn task_source<E>(code: ErrorCode, detail: impl Into<String>, error: E) -> anyhow::Error
where
    E: StdError + Send + Sync + 'static,
{
    DtErrorContext::new()
        .code(code)
        .attach(error)
        .with_stage(Stage::Task)
        .context(detail.into())
}
