use super::{
    super::{ErrorCode, OriginError},
    ProviderErrorClassification,
};

pub fn classify_reqwest_error(error: &reqwest::Error) -> ProviderErrorClassification {
    let code = if error.is_builder() {
        Some(ErrorCode::InvalidConfig)
    } else if error.is_timeout() {
        Some(ErrorCode::ConnectionTimeout)
    } else if error.is_connect() || error.is_request() {
        Some(ErrorCode::ConnectionFailed)
    } else {
        None
    };
    let status = error.status().map(|status| status.as_u16().to_string());
    ProviderErrorClassification::new(code, OriginError::new("http", status))
}
