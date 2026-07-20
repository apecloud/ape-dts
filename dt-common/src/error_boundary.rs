pub(crate) mod config {
    use crate::error::{DtError, ErrorCode, Stage};

    #[track_caller]
    pub(crate) fn invalid_task(detail: impl Into<String>) -> DtError {
        DtError::new(ErrorCode::InvalidConfig)
            .detail(detail)
            .stage(Stage::Bootstrap)
    }

    #[track_caller]
    pub(crate) fn invalid_filter(detail: impl Into<String>) -> DtError {
        DtError::new(ErrorCode::InvalidConfig)
            .detail(detail)
            .stage(Stage::Bootstrap)
            .operation("parse_filter_config")
    }

    #[cfg(feature = "metrics")]
    #[track_caller]
    pub(crate) fn metrics_initialization(error: prometheus::Error, metrics_name: &str) -> DtError {
        DtError::new(ErrorCode::InvalidConfig)
            .message("Metrics configuration is invalid")
            .detail(format!(
                "Failed to initialize metric [{metrics_name}]: {error}"
            ))
            .stage(Stage::Bootstrap)
            .operation("initialize_metrics")
            .source(error)
    }
}

pub(crate) mod metadata {
    use crate::error::{
        dt_error_from_mongodb, dt_error_from_sqlx, DtError, ErrorCode, ErrorObject, OriginError,
        SqlxProvider,
    };

    #[track_caller]
    pub(crate) fn mongodb_provider(
        error: mongodb::error::Error,
        fallback: ErrorCode,
        operation: &'static str,
    ) -> DtError {
        dt_error_from_mongodb(error, fallback).operation(operation)
    }

    #[track_caller]
    pub(crate) fn postgres_sqlx(
        error: sqlx::Error,
        fallback: ErrorCode,
        operation: &'static str,
    ) -> DtError {
        dt_error_from_sqlx(error, SqlxProvider::Postgres, fallback).operation(operation)
    }

    #[track_caller]
    pub(crate) fn row_conversion(
        error: anyhow::Error,
        schema: &str,
        table: &str,
        column: &str,
        operation: &'static str,
    ) -> DtError {
        DtError::new(ErrorCode::StatementFailed)
            .detail(format!(
                "failed to convert column {schema}.{table}.{column}"
            ))
            .operation(operation)
            .object(ErrorObject {
                schema: Some(schema.to_string()),
                table: Some(table.to_string()),
                column: Some(column.to_string()),
                ..Default::default()
            })
            .source(error)
    }

    #[track_caller]
    pub(crate) fn mongodb_ddl(detail: impl Into<String>) -> DtError {
        DtError::new(ErrorCode::StatementFailed)
            .detail(detail)
            .operation("parse_mongodb_ddl")
            .origin(OriginError::new("mongodb", None::<String>))
    }
}

pub(crate) mod redis {
    use crate::error::{BoxError, DtError, ErrorCode, OriginError};

    #[track_caller]
    pub(crate) fn redis_error(code: ErrorCode, operation: &'static str) -> DtError {
        DtError::new(code)
            .operation(operation)
            .origin(OriginError::new("redis", None::<String>))
    }

    #[track_caller]
    pub(crate) fn redis_error_detail(
        code: ErrorCode,
        detail: impl Into<String>,
        operation: &'static str,
    ) -> DtError {
        redis_error(code, operation).detail(detail)
    }

    #[track_caller]
    pub(crate) fn redis_source_error(
        code: ErrorCode,
        error: impl Into<BoxError>,
        operation: &'static str,
    ) -> DtError {
        redis_error(code, operation).source(error)
    }

    #[track_caller]
    pub(crate) fn redis_topology_error(
        detail: impl Into<String>,
        operation: &'static str,
    ) -> DtError {
        redis_error(ErrorCode::PrerequisiteNotMet, operation)
            .message("The Redis cluster topology is invalid or incomplete")
            .detail(detail)
            .hint(
                "Ensure all 16384 Redis cluster slots are assigned to stable master nodes, then retry.",
            )
    }

    #[track_caller]
    pub(crate) fn redis_topology_source_error(
        detail: impl Into<String>,
        error: impl Into<BoxError>,
        operation: &'static str,
    ) -> DtError {
        redis_topology_error(detail, operation).source(error)
    }

    #[track_caller]
    pub(crate) fn command(detail: impl Into<String>) -> DtError {
        redis_error(ErrorCode::StatementFailed, "parse_redis_command_keys").detail(detail)
    }

    #[track_caller]
    pub(crate) fn command_catalog(error: serde_json::Error, operation: &'static str) -> DtError {
        redis_error(ErrorCode::InvariantViolated, operation)
            .detail("the embedded Redis command catalog is invalid")
            .source(error)
    }
}
