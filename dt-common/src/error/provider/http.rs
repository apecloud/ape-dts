use super::{
    super::{ClassifyError, DtErrorContext, ErrorCode, OriginError},
    classification::provider_context,
};

impl ClassifyError for reqwest::Error {
    fn classify(&self) -> DtErrorContext {
        let code = if self.is_builder() {
            Some(ErrorCode::InvalidConfig)
        } else if self.is_timeout() {
            Some(ErrorCode::ConnectionTimeout)
        } else if self.is_connect() || self.is_request() {
            Some(ErrorCode::ConnectionFailed)
        } else {
            None
        };
        let status = self.status().map(|status| status.as_u16().to_string());
        provider_context(code, OriginError::new("http", status))
    }
}
