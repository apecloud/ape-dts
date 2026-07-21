use dt_common::error::{DtErrorContext, Stage};

fn scoped(context: DtErrorContext, stage: Stage) -> DtErrorContext {
    DtErrorContext::new().stage(stage).inherit(context)
}

pub(crate) mod connection_error {
    use dt_common::error::{
        classify_mongodb_error, classify_sqlx_error, DtError, DtErrorContextExt, EndpointRole,
        ErrorCode, SqlxProvider, Stage,
    };
    pub(crate) fn sqlx(error: sqlx::Error, provider: SqlxProvider) -> anyhow::Error {
        let context = classify_sqlx_error(&error, provider).into_context();
        error
            .with_code(ErrorCode::ConnectionFailed)
            .with_context(context)
    }
    pub(crate) fn mongodb_config(error: mongodb::error::Error) -> anyhow::Error {
        let context = classify_mongodb_error(&error).into_context();
        error
            .with_code(ErrorCode::InvalidConfig)
            .with_context(context)
            .with_stage(Stage::Bootstrap)
    }
    pub(crate) fn task_sqlx_metadata_error(
        error: sqlx::Error,
        provider: SqlxProvider,
    ) -> anyhow::Error {
        let context = classify_sqlx_error(&error, provider).into_context();
        error
            .with_code(ErrorCode::MetadataReadFailed)
            .with_context(context)
            .with_stage(Stage::Task)
    }
    pub(crate) fn missing_task_client(expected: &'static str) -> anyhow::Error {
        DtError::Unexpected(format!("expected {expected} connection client is missing"))
            .with_code(ErrorCode::InvariantViolated)
            .with_stage(Stage::Task)
    }
    pub(crate) fn invalid_task_config(detail: impl Into<String>) -> anyhow::Error {
        DtError::ConfigError(detail.into())
            .with_code(ErrorCode::InvalidConfig)
            .with_stage(Stage::Bootstrap)
    }

    pub(crate) fn attach_endpoint(error: anyhow::Error, endpoint: EndpointRole) -> anyhow::Error {
        error.with_endpoint(endpoint)
    }
}

pub(crate) mod extractor {
    use std::error::Error as StdError;

    use dt_common::error::{DtError, DtErrorContext, DtErrorContextExt, ErrorCode, Stage};
    pub(crate) fn missing_extractor_client() -> anyhow::Error {
        DtError::ExtractorError("the configured source connection client is missing".to_string())
            .with_code(ErrorCode::InvariantViolated)
            .with_stage(Stage::Task)
    }
    pub(crate) fn invalid_config_source<E>(detail: impl Into<String>, error: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        super::scoped(
            DtErrorContext::new()
                .code(ErrorCode::InvalidConfig)
                .detail(detail),
            Stage::Bootstrap,
        )
        .attach(error)
    }
}

pub(crate) mod sinker {
    use std::error::Error as StdError;

    use dt_common::error::{DtError, DtErrorContext, DtErrorContextExt, ErrorCode, Stage};
    pub(crate) fn missing_sinker_client() -> anyhow::Error {
        DtError::SinkerError("the configured destination connection client is missing".to_string())
            .with_code(ErrorCode::InvariantViolated)
            .with_stage(Stage::Task)
    }
    pub(crate) fn invalid_http_endpoint(detail: impl Into<String>) -> anyhow::Error {
        DtError::HttpError(detail.into())
            .with_code(ErrorCode::InvalidConfig)
            .with_stage(Stage::Bootstrap)
    }
    pub(crate) fn invalid_http_endpoint_source<E>(error: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        super::scoped(
            DtErrorContext::new().code(ErrorCode::InvalidConfig),
            Stage::Bootstrap,
        )
        .attach(error)
    }
    pub(crate) fn invalid_http_client<E>(error: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        super::scoped(
            DtErrorContext::new().code(ErrorCode::InvalidConfig),
            Stage::Bootstrap,
        )
        .attach(error)
    }
}

pub(crate) mod runner {
    use std::error::Error as StdError;

    use dt_common::error::{DtError, DtErrorContext, DtErrorContextExt, ErrorCode, Stage};
    pub(crate) fn invalid_task_config(detail: impl Into<String>) -> anyhow::Error {
        DtError::ConfigError(detail.into())
            .with_code(ErrorCode::InvalidConfig)
            .with_stage(Stage::Bootstrap)
    }
    pub(crate) fn task_worker_error(error: tokio::task::JoinError) -> anyhow::Error {
        super::scoped(
            DtErrorContext::new().code(ErrorCode::WorkerFailed),
            Stage::Task,
        )
        .attach(error)
    }
    pub(crate) fn task_io_error<E>(error: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        super::scoped(
            DtErrorContext::new().code(ErrorCode::IoFailed),
            Stage::Bootstrap,
        )
        .attach(error)
    }
    pub(crate) fn invalid_task_config_source<E>(error: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        super::scoped(
            DtErrorContext::new().code(ErrorCode::InvalidConfig),
            Stage::Bootstrap,
        )
        .attach(error)
    }
}
