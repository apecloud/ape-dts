use super::{
    super::{ClassifyError, DtErrorContext, ErrorCode},
    classification::{provider_context, provider_detail},
};

impl ClassifyError for reqwest::Error {
    fn classify(&self) -> DtErrorContext {
        let code = if self.is_builder() {
            ErrorCode::InvalidConfig
        } else if self.is_timeout() && self.is_connect() {
            ErrorCode::ConnectionTimeout
        } else if self.is_timeout() {
            ErrorCode::DatabaseOperationTimeout
        } else if self.is_connect() {
            ErrorCode::ConnectionFailed
        } else if self.is_decode() {
            ErrorCode::DataDecodeFailed
        } else if let Some(status) = self.status() {
            classify_http_status(status)
        } else {
            ErrorCode::Unclassified
        };
        let provider_code = self.status().map(|status| status.as_u16().to_string());
        provider_context(Some(code), provider_detail("http", provider_code, self))
    }
}

fn classify_http_status(status: reqwest::StatusCode) -> ErrorCode {
    match status.as_u16() {
        401 => ErrorCode::AuthenticationFailed,
        403 => ErrorCode::PermissionDenied,
        404 | 410 => ErrorCode::ObjectNotFound,
        408 | 504 => ErrorCode::DatabaseOperationTimeout,
        409 => ErrorCode::DatabaseOperationConflict,
        429 | 507 => ErrorCode::ResourceExhausted,
        502 | 503 => ErrorCode::ConnectionFailed,
        _ => ErrorCode::Unclassified,
    }
}
