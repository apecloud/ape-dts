use std::fmt::Display;

use super::super::{DtErrorContext, ErrorCode};

pub(super) fn provider_context(
    code: Option<ErrorCode>,
    detail: impl Into<String>,
) -> DtErrorContext {
    let context = DtErrorContext::new().with_detail(detail);
    match code {
        Some(code) => context.with_code(code),
        None => context,
    }
}

pub(super) fn provider_detail(
    provider: &str,
    provider_code: Option<String>,
    error: impl Display,
) -> String {
    let provider = match provider_code {
        Some(code) => format!("{provider}/{code}"),
        None => provider.to_string(),
    };
    format!("{provider}: {error}")
}

pub(super) fn classify_postgres_code(code: &str) -> Option<ErrorCode> {
    match code {
        "42P01" | "42703" | "42704" => Some(ErrorCode::ObjectNotFound),
        "3D000" => Some(ErrorCode::DatabaseNotFound),
        code if code.starts_with("28") => Some(ErrorCode::AuthenticationFailed),
        "42501" => Some(ErrorCode::PermissionDenied),
        code if code.starts_with("08") => Some(ErrorCode::ConnectionFailed),
        code if code.starts_with("23") => Some(ErrorCode::IntegrityViolation),
        _ => None,
    }
}

pub(super) fn classify_mysql_code(code: &str) -> Option<ErrorCode> {
    match code {
        "1054" | "1146" => Some(ErrorCode::ObjectNotFound),
        "1049" => Some(ErrorCode::DatabaseNotFound),
        "1045" => Some(ErrorCode::AuthenticationFailed),
        "1044" | "1142" | "1143" | "1227" | "1370" => Some(ErrorCode::PermissionDenied),
        "2002" | "2003" | "2006" | "2013" => Some(ErrorCode::ConnectionFailed),
        _ => None,
    }
}
