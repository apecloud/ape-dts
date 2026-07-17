use dt_common::error::{
    dt_error_from_reqwest, DtError, EndpointRole, ErrorCode, OriginError, Stage,
};

#[track_caller]
pub(super) fn reqwest(error: reqwest::Error, operation: &'static str) -> DtError {
    dt_error_from_reqwest(error, ErrorCode::StatementFailed)
        .stage(Stage::Sinker)
        .operation(operation)
        .endpoint(EndpointRole::Destination)
}

#[track_caller]
pub(super) fn status(status: reqwest::StatusCode, operation: &'static str) -> DtError {
    DtError::new(ErrorCode::StatementFailed)
        .stage(Stage::Sinker)
        .operation(operation)
        .endpoint(EndpointRole::Destination)
        .origin(OriginError::new("http", Some(status.as_u16().to_string())))
        .detail(format!(
            "the destination rejected the request with HTTP status {}",
            status.as_u16()
        ))
}

#[track_caller]
pub(super) fn rejected(status: reqwest::StatusCode, operation: &'static str) -> DtError {
    DtError::new(ErrorCode::StatementFailed)
        .stage(Stage::Sinker)
        .operation(operation)
        .endpoint(EndpointRole::Destination)
        .origin(OriginError::new("http", Some(status.as_u16().to_string())))
        .detail("the destination rejected the data load request")
}

#[track_caller]
pub(super) fn invalid_response(error: serde_json::Error, operation: &'static str) -> DtError {
    DtError::new(ErrorCode::StatementFailed)
        .stage(Stage::Sinker)
        .operation(operation)
        .endpoint(EndpointRole::Destination)
        .origin(OriginError::new("http", None::<String>))
        .source(error)
}
