use dt_common::error::{dt_error_from_sqlx, DtError, EndpointRole, ErrorCode, SqlxProvider, Stage};

#[track_caller]
pub(crate) fn mysql(error: sqlx::Error, fallback: ErrorCode, operation: &'static str) -> DtError {
    enrich(
        dt_error_from_sqlx(error, SqlxProvider::MySql, fallback),
        operation,
    )
}

#[track_caller]
pub(crate) fn postgres(
    error: sqlx::Error,
    fallback: ErrorCode,
    operation: &'static str,
) -> DtError {
    enrich(
        dt_error_from_sqlx(error, SqlxProvider::Postgres, fallback),
        operation,
    )
}

fn enrich(error: DtError, operation: &'static str) -> DtError {
    error
        .stage(Stage::Extractor)
        .operation(operation)
        .endpoint(EndpointRole::Source)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enriches_extractor_sqlx_errors() {
        let caller_line = line!() + 1;
        let error = mysql(
            sqlx::Error::PoolTimedOut,
            ErrorCode::MetadataFailed,
            "list_binary_logs",
        );

        assert_eq!(error.code, ErrorCode::ConnectionTimeout);
        assert_eq!(error.stage, Some(Stage::Extractor));
        assert_eq!(error.operation, Some("list_binary_logs"));
        assert_eq!(error.endpoint, Some(EndpointRole::Source));
        assert_eq!(error.location.file(), file!());
        assert_eq!(error.location.line(), caller_line);
    }
}
