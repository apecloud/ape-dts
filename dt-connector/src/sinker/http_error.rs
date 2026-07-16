use dt_common::error::{DtError, EndpointRole, ErrorCode, OriginError, Stage};

#[track_caller]
pub(super) fn reqwest(error: reqwest::Error, operation: &'static str) -> DtError {
    let code = if error.is_builder() {
        ErrorCode::InvalidConfig
    } else if error.is_timeout() {
        ErrorCode::ConnectionTimeout
    } else if error.is_connect() || error.is_request() {
        ErrorCode::ConnectionFailed
    } else {
        ErrorCode::StatementFailed
    };
    let status = error.status().map(|status| status.as_u16().to_string());
    DtError::new(code)
        .stage(Stage::Sinker)
        .operation(operation)
        .endpoint(EndpointRole::Destination)
        .origin(OriginError::new("http", status))
        .source(error)
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
