use super::super::{ErrorCode, ErrorObject, OriginError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderErrorClassification {
    pub code: ErrorCode,
    pub origin: OriginError,
    pub object: Option<ErrorObject>,
}

impl ProviderErrorClassification {
    pub fn new(code: ErrorCode, origin: OriginError) -> Self {
        Self {
            code,
            origin,
            object: None,
        }
    }

    pub fn object(mut self, object: Option<ErrorObject>) -> Self {
        self.object = object;
        self
    }
}

pub(super) fn classify_postgres_code(code: &str, fallback: ErrorCode) -> ErrorCode {
    match code {
        "42P01" | "42703" | "42704" => ErrorCode::ObjectNotFound,
        "3D000" => ErrorCode::DatabaseNotFound,
        code if code.starts_with("28") => ErrorCode::AuthenticationFailed,
        "42501" => ErrorCode::PermissionDenied,
        code if code.starts_with("08") => ErrorCode::ConnectionFailed,
        code if code.starts_with("23") => ErrorCode::IntegrityViolation,
        _ => fallback,
    }
}

pub(super) fn classify_mysql_code(code: &str, fallback: ErrorCode) -> ErrorCode {
    match code {
        "1054" | "1146" => ErrorCode::ObjectNotFound,
        "1049" => ErrorCode::DatabaseNotFound,
        "1045" => ErrorCode::AuthenticationFailed,
        "1044" | "1142" | "1143" | "1227" | "1370" => ErrorCode::PermissionDenied,
        "2002" | "2003" | "2006" | "2013" => ErrorCode::ConnectionFailed,
        _ => fallback,
    }
}
