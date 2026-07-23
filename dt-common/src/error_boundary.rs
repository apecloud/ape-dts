pub(crate) mod config {
    use std::error::Error as StdError;

    use crate::error::{DtError, DtErrorContext, DtErrorContextExt, ErrorCode, Stage};
    pub(crate) fn invalid_task_config(detail: impl Into<String>) -> anyhow::Error {
        DtError::InvalidConfig(detail.into()).with_stage(Stage::Bootstrap)
    }
    pub(crate) fn invalid_filter_source<E>(detail: impl Into<String>, error: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        DtErrorContext::new()
            .code(ErrorCode::InvalidConfig)
            .stage(Stage::Bootstrap)
            .attach(error)
            .context(detail.into())
    }
    pub(crate) fn source<E>(
        error: E,
        code: ErrorCode,
        message: impl Into<String>,
        detail: impl Into<String>,
    ) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        DtErrorContext::new()
            .code(code)
            .message(message)
            .stage(Stage::Bootstrap)
            .attach(error)
            .context(detail.into())
    }
    pub(crate) fn anyhow_source(error: anyhow::Error, detail: impl Into<String>) -> anyhow::Error {
        error
            .with_code(ErrorCode::InvalidConfig)
            .with_stage(Stage::Bootstrap)
            .context(detail.into())
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn metrics_initialization_error(
        error: prometheus::Error,
        metrics_name: &str,
    ) -> anyhow::Error {
        DtErrorContext::new()
            .code(ErrorCode::InvalidConfig)
            .message("Metrics configuration is invalid")
            .stage(Stage::Bootstrap)
            .attach(error)
            .context(format!("Failed to initialize metric [{metrics_name}]"))
    }
}

pub(crate) mod metadata {
    use std::error::Error as StdError;

    use crate::error::{
        classify_sqlx_error, ClassifyError, DtError, DtErrorContext, DtErrorContextExt, ErrorCode,
        ErrorObject, OriginError, SqlxProvider,
    };
    pub(crate) fn invariant(detail: impl Into<String>) -> anyhow::Error {
        DtError::InvariantViolated(detail.into()).into()
    }
    pub(crate) fn mongodb_provider(
        error: mongodb::error::Error,
        default_code: ErrorCode,
    ) -> anyhow::Error {
        let context = error.classify();
        error.with_code(default_code).with_context(context)
    }
    pub(crate) fn postgres_sqlx(error: sqlx::Error, default_code: ErrorCode) -> anyhow::Error {
        let context = classify_sqlx_error(&error, SqlxProvider::Postgres);
        error.with_code(default_code).with_context(context)
    }
    pub(crate) fn row_conversion_error(
        error: anyhow::Error,
        schema: &str,
        table: &str,
        column: &str,
    ) -> anyhow::Error {
        error
            .with_code(ErrorCode::StatementFailed)
            .with_object(ErrorObject {
                schema: Some(schema.to_string()),
                table: Some(table.to_string()),
                column: Some(column.to_string()),
                ..Default::default()
            })
            .context(format!(
                "failed to convert column {schema}.{table}.{column}"
            ))
    }
    pub(crate) fn mongo_ddl_error(detail: impl Into<String>) -> anyhow::Error {
        DtError::StatementFailed(detail.into())
            .with_origin(OriginError::new("mongodb", None::<String>))
    }
    pub(crate) fn mongo_ddl_source<E>(detail: impl Into<String>, error: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        DtErrorContext::new()
            .code(ErrorCode::StatementFailed)
            .origin(OriginError::new("mongodb", None::<String>))
            .attach(error)
            .context(detail.into())
    }
    pub(crate) fn mongodb_version_source<E>(detail: impl Into<String>, error: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        DtErrorContext::new()
            .code(ErrorCode::UnsupportedDatabaseVersion)
            .origin(OriginError::new("mongodb", None::<String>))
            .attach(error)
            .context(detail.into())
    }
    pub(crate) fn avro_source<E>(
        error: E,
        code: ErrorCode,
        detail: impl Into<String>,
    ) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        DtErrorContext::new()
            .code(code)
            .attach(error)
            .context(detail.into())
    }
}

pub(crate) mod redis {
    use std::error::Error as StdError;

    use crate::error::{DtError, DtErrorContext, DtErrorContextExt, ErrorCode, OriginError};
    fn redis_context(code: ErrorCode) -> DtErrorContext {
        DtErrorContext::new()
            .code(code)
            .origin(OriginError::new("redis", None::<String>))
    }
    pub(crate) fn redis_version_error(detail: impl Into<String>) -> anyhow::Error {
        DtError::UnsupportedDatabaseVersion(detail.into())
            .with_origin(OriginError::new("redis", None::<String>))
    }
    pub(crate) fn redis_result_error(detail: impl Into<String>) -> anyhow::Error {
        DtError::RedisResultError(detail.into())
            .with_origin(OriginError::new("redis", None::<String>))
    }
    pub(crate) fn redis_invariant(detail: impl Into<String>) -> anyhow::Error {
        DtError::InvariantViolated(detail.into())
            .with_origin(OriginError::new("redis", None::<String>))
    }
    pub(crate) fn redis_source_error<E>(error: DtError, source: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        anyhow::Error::new(source)
            .with_origin(OriginError::new("redis", None::<String>))
            .context(error)
    }
    pub(crate) fn redis_topology_error(detail: impl Into<String>) -> anyhow::Error {
        let detail = detail.into();
        DtError::PrerequisiteNotMet(detail.clone())
            .with_message("The Redis cluster topology is invalid or incomplete")
            .with_hint(
                "Ensure all 16384 Redis cluster slots are assigned to stable master nodes, then retry.",
            )
            .with_origin(OriginError::new("redis", None::<String>))
    }
    pub(crate) fn redis_topology_source_error<E>(
        detail: impl Into<String>,
        error: E,
    ) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        let context = redis_context(ErrorCode::PrerequisiteNotMet)
            .message("The Redis cluster topology is invalid or incomplete")
            .hint(
                "Ensure all 16384 Redis cluster slots are assigned to stable master nodes, then retry.",
            );
        context.attach(error).context(detail.into())
    }
    pub(crate) fn redis_command_error(detail: impl Into<String>) -> anyhow::Error {
        DtError::RedisCmdError(detail.into()).with_origin(OriginError::new("redis", None::<String>))
    }
    pub(crate) fn redis_command_source<E>(detail: impl Into<String>, error: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        redis_context(ErrorCode::StatementFailed)
            .attach(error)
            .context(detail.into())
    }
    pub(crate) fn redis_command_catalog_error(error: serde_json::Error) -> anyhow::Error {
        redis_context(ErrorCode::InvariantViolated)
            .attach(error)
            .context("the embedded Redis command catalog is invalid")
    }
}
