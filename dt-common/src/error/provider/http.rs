use super::{
    super::{DtError, ErrorCode, OriginError},
    ProviderErrorClassification,
};

pub fn classify_reqwest_error(
    error: &reqwest::Error,
    fallback: ErrorCode,
) -> ProviderErrorClassification {
    let code = if error.is_builder() {
        ErrorCode::InvalidConfig
    } else if error.is_timeout() {
        ErrorCode::ConnectionTimeout
    } else if error.is_connect() || error.is_request() {
        ErrorCode::ConnectionFailed
    } else {
        fallback
    };
    let status = error.status().map(|status| status.as_u16().to_string());
    ProviderErrorClassification::new(code, OriginError::new("http", status))
}

#[track_caller]
pub fn dt_error_from_reqwest(error: reqwest::Error, fallback: ErrorCode) -> DtError {
    let classification = classify_reqwest_error(&error, fallback);
    DtError::new(classification.code)
        .origin(classification.origin)
        .source(error)
}
