use dt_common::error::{DtError, DtErrorContext, DtErrorContextExt, ErrorCode, OriginError, Stage};
pub(crate) fn worker(error: tokio::task::JoinError) -> anyhow::Error {
    DtErrorContext::new()
        .code(ErrorCode::WorkerFailed)
        .attach(error)
        .with_stage(Stage::Parallelizer)
}
pub(crate) fn invariant() -> anyhow::Error {
    DtError::General("parallelizer invariant violated".to_string())
        .with_code(ErrorCode::InvariantViolated)
        .with_stage(Stage::Parallelizer)
}
pub(crate) fn invariant_source(error: anyhow::Error) -> anyhow::Error {
    error
        .with_code(ErrorCode::InvariantViolated)
        .with_stage(Stage::Parallelizer)
}
pub(crate) fn invalid_config() -> anyhow::Error {
    DtError::ConfigError("parallelizer configuration is invalid".to_string())
        .with_code(ErrorCode::InvalidConfig)
        .with_stage(Stage::Parallelizer)
}
pub(crate) fn redis_command(detail: impl Into<String>) -> anyhow::Error {
    DtError::RedisCmdError(detail.into())
        .with_code(ErrorCode::StatementFailed)
        .with_origin(OriginError::new("redis", None::<String>))
        .with_stage(Stage::Parallelizer)
}
