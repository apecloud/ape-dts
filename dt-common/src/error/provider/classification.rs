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
        "3F000" | "42P01" | "42703" | "42704" | "42883" => Some(ErrorCode::ObjectNotFound),
        "3D000" | "57P04" => Some(ErrorCode::DatabaseNotFound),
        code if code.starts_with("28") => Some(ErrorCode::AuthenticationFailed),
        "42501" => Some(ErrorCode::PermissionDenied),
        "40002" => Some(ErrorCode::IntegrityViolation),
        code if code.starts_with("23") => Some(ErrorCode::IntegrityViolation),
        "40001" | "40P01" | "55006" | "55P03" => Some(ErrorCode::DatabaseOperationConflict),
        "57014" => Some(ErrorCode::OperationInterrupted),
        "57P01" | "57P02" | "57P03" => Some(ErrorCode::ConnectionFailed),
        code if code.starts_with("08") => Some(ErrorCode::ConnectionFailed),
        code if code.starts_with("53") || code.starts_with("54") => {
            Some(ErrorCode::ResourceExhausted)
        }
        code if code.starts_with("0A") => Some(ErrorCode::PrerequisiteNotMet),
        _ => None,
    }
}

pub(super) fn classify_mysql_code(code: &str) -> Option<ErrorCode> {
    match code {
        "1051" | "1054" | "1091" | "1109" | "1146" | "1305" => Some(ErrorCode::ObjectNotFound),
        "1049" => Some(ErrorCode::DatabaseNotFound),
        "1045" | "1698" | "3118" => Some(ErrorCode::AuthenticationFailed),
        "1044" | "1142" | "1143" | "1144" | "1145" | "1227" | "1370" | "1410" => {
            Some(ErrorCode::PermissionDenied)
        }
        "1205" | "1213" | "3572" => Some(ErrorCode::DatabaseOperationConflict),
        "1317" => Some(ErrorCode::OperationInterrupted),
        "3024" => Some(ErrorCode::DatabaseOperationTimeout),
        "1021" | "1037" | "1038" | "1040" | "1041" | "1114" | "1203" | "1226" => {
            Some(ErrorCode::ResourceExhausted)
        }
        "1042" | "1053" | "1152" | "1158" | "1159" | "1160" | "1161" | "1927" | "2002" | "2003"
        | "2006" | "2013" => Some(ErrorCode::ConnectionFailed),
        "1235" | "1289" | "1295" => Some(ErrorCode::PrerequisiteNotMet),
        _ => None,
    }
}
