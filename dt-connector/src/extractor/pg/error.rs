use dt_common::error::{dt_error_from_tokio_postgres, DtError, EndpointRole, ErrorCode, Stage};

#[track_caller]
pub(super) fn postgres(
    error: tokio_postgres::Error,
    fallback: ErrorCode,
    operation: &'static str,
) -> DtError {
    dt_error_from_tokio_postgres(error, fallback)
        .stage(Stage::Extractor)
        .operation(operation)
        .endpoint(EndpointRole::Source)
}
