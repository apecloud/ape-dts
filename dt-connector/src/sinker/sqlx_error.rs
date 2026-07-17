use dt_common::error::{
    dt_error_from_sqlx, try_dt_error_from_anyhow_sqlx, DtError, EndpointRole, ErrorCode,
    SqlxProvider, Stage,
};

#[track_caller]
pub(super) fn mysql(error: sqlx::Error, fallback: ErrorCode, operation: &'static str) -> DtError {
    from_sqlx(error, SqlxProvider::MySql, fallback, operation)
}

#[track_caller]
pub(super) fn postgres(
    error: sqlx::Error,
    fallback: ErrorCode,
    operation: &'static str,
) -> DtError {
    from_sqlx(error, SqlxProvider::Postgres, fallback, operation)
}

#[track_caller]
pub(super) fn mysql_from_anyhow(
    error: anyhow::Error,
    fallback: ErrorCode,
    operation: &'static str,
) -> anyhow::Error {
    from_anyhow(error, SqlxProvider::MySql, fallback, operation)
}

#[track_caller]
pub(super) fn postgres_from_anyhow(
    error: anyhow::Error,
    fallback: ErrorCode,
    operation: &'static str,
) -> anyhow::Error {
    from_anyhow(error, SqlxProvider::Postgres, fallback, operation)
}

#[track_caller]
fn from_sqlx(
    error: sqlx::Error,
    provider: SqlxProvider,
    fallback: ErrorCode,
    operation: &'static str,
) -> DtError {
    enrich(dt_error_from_sqlx(error, provider, fallback), operation)
}

#[track_caller]
fn from_anyhow(
    error: anyhow::Error,
    provider: SqlxProvider,
    fallback: ErrorCode,
    operation: &'static str,
) -> anyhow::Error {
    match try_dt_error_from_anyhow_sqlx(error, provider, fallback) {
        Ok(error) => enrich(error, operation).into(),
        Err(error) => error,
    }
}

fn enrich(error: DtError, operation: &'static str) -> DtError {
    error
        .stage(Stage::Sinker)
        .operation(operation)
        .endpoint(EndpointRole::Destination)
}

#[cfg(test)]
mod tests {
    use dt_common::error::{ErrorReport, OriginError};

    use super::*;

    #[test]
    fn enriches_mysql_errors_with_sinker_context() {
        let caller_line = line!() + 1;
        let error = mysql(
            sqlx::Error::PoolClosed,
            ErrorCode::StatementFailed,
            "sink_dml",
        );

        assert_eq!(error.code(), ErrorCode::ConnectionFailed);
        assert_eq!(error.root_stage(), Some(Stage::Sinker));
        assert_eq!(error.root_operation(), Some("sink_dml"));
        assert_eq!(error.endpoint, Some(EndpointRole::Destination));
        assert_eq!(error.location.file(), file!());
        assert_eq!(error.location.line(), caller_line);
        assert_eq!(
            error.origin_error(),
            Some(&OriginError::new("mysql", None::<String>))
        );
    }

    #[test]
    fn anyhow_path_preserves_existing_structured_error() {
        let original = DtError::new(ErrorCode::WorkerFailed)
            .stage(Stage::Task)
            .operation("join_worker");
        let error = mysql_from_anyhow(
            anyhow::Error::new(original),
            ErrorCode::StatementFailed,
            "sink_struct",
        );
        let report = ErrorReport::from_anyhow(&error);

        assert_eq!(report.code, ErrorCode::WorkerFailed);
        assert_eq!(report.stage, Stage::Task);
        assert_eq!(report.operation.as_deref(), Some("join_worker"));
        assert_eq!(report.endpoint, None);
    }
}
