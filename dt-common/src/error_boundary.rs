pub(crate) mod config {
    use std::error::Error as StdError;

    use crate::error::{DtError, DtErrorContext, DtErrorContextExt, ErrorCode, Stage};
    pub(crate) fn invalid_task_config(detail: impl Into<String>) -> anyhow::Error {
        DtError::ConfigError(detail.into())
            .with_code(ErrorCode::InvalidConfig)
            .with_stage(Stage::Bootstrap)
    }
    pub(crate) fn invalid_filter_source<E>(detail: impl Into<String>, error: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        DtErrorContext::new()
            .code(ErrorCode::InvalidConfig)
            .detail(detail)
            .stage(Stage::Bootstrap)
            .attach(error)
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
            .detail(detail)
            .stage(Stage::Bootstrap)
            .attach(error)
    }
    pub(crate) fn anyhow_source(error: anyhow::Error, detail: impl Into<String>) -> anyhow::Error {
        error
            .with_code(ErrorCode::InvalidConfig)
            .with_detail(detail)
            .with_stage(Stage::Bootstrap)
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn metrics_initialization_error(
        error: prometheus::Error,
        metrics_name: &str,
    ) -> anyhow::Error {
        DtErrorContext::new()
            .code(ErrorCode::InvalidConfig)
            .message("Metrics configuration is invalid")
            .detail(format!(
                "Failed to initialize metric [{metrics_name}]: {error}"
            ))
            .stage(Stage::Bootstrap)
            .attach(error)
    }
}

pub(crate) mod metadata {
    use std::error::Error as StdError;

    use crate::error::{
        classify_mongodb_error, classify_sqlx_error, DtError, DtErrorContext, DtErrorContextExt,
        ErrorCode, ErrorObject, OriginError, SqlxProvider,
    };
    pub(crate) fn mongodb_provider(
        error: mongodb::error::Error,
        default_code: ErrorCode,
    ) -> anyhow::Error {
        let context = classify_mongodb_error(&error).into_context();
        error.with_code(default_code).with_context(context)
    }
    pub(crate) fn postgres_sqlx(error: sqlx::Error, default_code: ErrorCode) -> anyhow::Error {
        let context = classify_sqlx_error(&error, SqlxProvider::Postgres).into_context();
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
            .with_detail(format!(
                "failed to convert column {schema}.{table}.{column}"
            ))
            .with_object(ErrorObject {
                schema: Some(schema.to_string()),
                table: Some(table.to_string()),
                column: Some(column.to_string()),
                ..Default::default()
            })
    }
    pub(crate) fn mongo_ddl_error(detail: impl Into<String>) -> anyhow::Error {
        DtError::MetadataError(detail.into())
            .with_code(ErrorCode::StatementFailed)
            .with_origin(OriginError::new("mongodb", None::<String>))
    }
    pub(crate) fn mongo_ddl_source<E>(detail: impl Into<String>, error: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        DtErrorContext::new()
            .code(ErrorCode::StatementFailed)
            .detail(detail)
            .origin(OriginError::new("mongodb", None::<String>))
            .attach(error)
    }
    pub(crate) fn mongodb_version_source<E>(detail: impl Into<String>, error: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        DtErrorContext::new()
            .code(ErrorCode::UnsupportedDatabaseVersion)
            .detail(detail)
            .origin(OriginError::new("mongodb", None::<String>))
            .attach(error)
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
            .detail(detail)
            .attach(error)
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
    pub(crate) fn redis_error(code: ErrorCode) -> anyhow::Error {
        DtError::Unexpected(code.default_message().to_string())
            .with_code(code)
            .with_origin(OriginError::new("redis", None::<String>))
    }
    pub(crate) fn redis_error_detail(code: ErrorCode, detail: impl Into<String>) -> anyhow::Error {
        DtError::RedisResultError(detail.into())
            .with_code(code)
            .with_origin(OriginError::new("redis", None::<String>))
    }
    pub(crate) fn redis_source_error<E>(code: ErrorCode, error: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        redis_context(code).attach(error)
    }
    pub(crate) fn redis_topology_error(detail: impl Into<String>) -> anyhow::Error {
        let detail = detail.into();
        DtError::RedisResultError(detail.clone())
            .with_code(ErrorCode::PrerequisiteNotMet)
            .with_message("The Redis cluster topology is invalid or incomplete")
            .with_detail(detail)
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
            .detail(detail)
            .hint(
                "Ensure all 16384 Redis cluster slots are assigned to stable master nodes, then retry.",
            );
        context.attach(error)
    }
    pub(crate) fn redis_command_error(detail: impl Into<String>) -> anyhow::Error {
        DtError::RedisCmdError(detail.into())
            .with_code(ErrorCode::StatementFailed)
            .with_origin(OriginError::new("redis", None::<String>))
    }
    pub(crate) fn redis_command_source<E>(detail: impl Into<String>, error: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        redis_context(ErrorCode::StatementFailed)
            .detail(detail)
            .attach(error)
    }
    pub(crate) fn redis_command_catalog_error(error: serde_json::Error) -> anyhow::Error {
        redis_context(ErrorCode::InvariantViolated)
            .detail("the embedded Redis command catalog is invalid")
            .attach(error)
    }
}
