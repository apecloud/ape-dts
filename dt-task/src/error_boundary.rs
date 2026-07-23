use dt_common::error::{DtError, DtErrorContextExt, Stage};

pub(crate) fn invalid_task_config(detail: impl Into<String>) -> anyhow::Error {
    DtError::InvalidConfig(detail.into()).with_stage(Stage::Bootstrap)
}

pub(crate) mod connection_error {
    use dt_common::error::{
        classify_mongodb_error, classify_sqlx_error, DtError, DtErrorContextExt, ErrorCode,
        SqlxProvider, Stage,
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
        DtError::InvariantViolated(format!("expected {expected} connection client is missing"))
            .with_stage(Stage::Task)
    }
}

pub(crate) mod extractor {
    use std::error::Error as StdError;

    use dt_common::error::{DtError, DtErrorContext, DtErrorContextExt, ErrorCode, Stage};
    pub(crate) fn missing_extractor_client() -> anyhow::Error {
        DtError::InvariantViolated("the configured source connection client is missing".to_string())
            .with_stage(Stage::Task)
    }
    pub(crate) fn invalid_config_source<E>(detail: impl Into<String>, error: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        DtErrorContext::new()
            .code(ErrorCode::InvalidConfig)
            .attach(error)
            .with_stage(Stage::Bootstrap)
            .context(detail.into())
    }
}

pub(crate) mod sinker {
    use std::error::Error as StdError;

    use dt_common::error::{
        classify_kafka_error, DtError, DtErrorContext, DtErrorContextExt, ErrorCode, Stage,
    };
    pub(crate) fn missing_sinker_client() -> anyhow::Error {
        DtError::InvariantViolated(
            "the configured destination connection client is missing".to_string(),
        )
        .with_stage(Stage::Task)
    }
    pub(crate) fn invalid_http_endpoint(detail: impl Into<String>) -> anyhow::Error {
        DtError::InvalidConfig(detail.into()).with_stage(Stage::Bootstrap)
    }
    pub(crate) fn invalid_http_cause<E>(error: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        DtErrorContext::new()
            .code(ErrorCode::InvalidConfig)
            .attach(error)
            .with_stage(Stage::Bootstrap)
    }
    pub(crate) fn kafka_error(error: kafka::Error, default_code: ErrorCode) -> anyhow::Error {
        let context = classify_kafka_error(&error).into_context();
        error.with_code(default_code).with_context(context)
    }
}

pub(crate) mod runner {
    use std::error::Error as StdError;

    use dt_common::error::{DtErrorContext, DtErrorContextExt, ErrorCode, Stage};
    pub(crate) fn task_worker_error(error: tokio::task::JoinError) -> anyhow::Error {
        DtErrorContext::new()
            .code(ErrorCode::WorkerFailed)
            .attach(error)
            .with_stage(Stage::Task)
    }
    pub(crate) fn task_io_error<E>(error: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        DtErrorContext::new()
            .code(ErrorCode::IoFailed)
            .attach(error)
            .with_stage(Stage::Bootstrap)
    }
    pub(crate) fn invalid_task_config_source<E>(error: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        DtErrorContext::new()
            .code(ErrorCode::InvalidConfig)
            .attach(error)
            .with_stage(Stage::Bootstrap)
    }
}
