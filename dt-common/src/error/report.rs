use std::{backtrace::BacktraceStatus, fmt};

use serde::Serialize;

use super::{
    error_context::DT_ERROR_CONTEXT_MARKER, DtErrorContext, EndpointRole, ErrorCode, ErrorObject,
    OriginError, Stage,
};

pub const ERROR_REPORT_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ErrorReport {
    pub schema_version: u16,
    pub code: ErrorCode,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    #[serde(skip_serializing)]
    pub stage: Stage,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<EndpointRole>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<ErrorObject>,
    #[serde(skip_serializing)]
    pub origin: Option<OriginError>,
    #[serde(skip_serializing)]
    pub error_chain: Vec<String>,
    #[serde(skip_serializing)]
    pub context_count: usize,
    #[serde(skip_serializing)]
    pub backtrace: Option<String>,
}

impl ErrorReport {
    pub fn from_anyhow(error: &anyhow::Error) -> Self {
        let context = error.downcast_ref::<DtErrorContext>();
        let (error_chain, context_count) = split_error_chain(error);
        let detail = detail_from_error_chain(&error_chain);
        let backtrace = captured_backtrace(error);

        if let Some(context) = context {
            return Self::from_context(context, detail, error_chain, context_count, backtrace);
        }

        Self {
            schema_version: ERROR_REPORT_SCHEMA_VERSION,
            code: ErrorCode::Unclassified,
            message: ErrorCode::Unclassified.default_message().to_string(),
            detail,
            hint: Some(ErrorCode::Unclassified.default_hint().to_string()),
            stage: Stage::Unknown,
            task_id: None,
            endpoint: None,
            object: None,
            origin: None,
            error_chain,
            context_count,
            backtrace,
        }
    }

    fn from_context(
        context: &DtErrorContext,
        detail: Option<String>,
        error_chain: Vec<String>,
        context_count: usize,
        backtrace: Option<String>,
    ) -> Self {
        let code = context.error_code().unwrap_or(ErrorCode::Unclassified);
        Self {
            schema_version: ERROR_REPORT_SCHEMA_VERSION,
            code,
            message: sanitize_user_text(
                context
                    .message_text()
                    .unwrap_or_else(|| code.default_message()),
            ),
            detail,
            hint: Some(sanitize_user_text(
                context.hint_text().unwrap_or_else(|| code.default_hint()),
            )),
            stage: context.stage_value().unwrap_or(Stage::Unknown),
            task_id: context.task_id_value().map(str::to_string),
            endpoint: context.endpoint_role(),
            object: context.error_object().cloned(),
            origin: context.origin_error().cloned(),
            error_chain,
            context_count,
            backtrace,
        }
    }

    fn affected_resource(&self) -> Option<String> {
        let mut parts = Vec::new();
        if let Some(endpoint) = self.endpoint {
            parts.push(endpoint.user_description().to_string());
        }
        if let Some(origin) = &self.origin {
            if origin.system != "sqlx" {
                parts.push(origin.system.clone());
            }
        }
        if let Some(object) = &self.object {
            if let Some(name) = format_object(object) {
                parts.push(name);
            }
        }
        (!parts.is_empty()).then(|| parts.join(" "))
    }
}

fn sanitize_user_text(value: &str) -> String {
    rtb_redact::string(value).into_owned()
}

fn format_object(object: &ErrorObject) -> Option<String> {
    let qualified_name = match (&object.schema, &object.table) {
        (Some(schema), Some(table)) => Some(format!("{schema}.{table}")),
        (Some(schema), None) => Some(schema.clone()),
        (None, Some(table)) => Some(table.clone()),
        (None, None) => None,
    };
    let mut parts = Vec::new();
    if let Some(name) = qualified_name {
        parts.push(name);
    }
    if let Some(column) = &object.column {
        parts.push(format!("column {column}"));
    }
    if let Some(constraint) = &object.constraint {
        parts.push(format!("constraint {constraint}"));
    }
    (!parts.is_empty()).then(|| parts.join(", "))
}

fn split_error_chain(error: &anyhow::Error) -> (Vec<String>, usize) {
    let raw_chain: Vec<_> = error
        .chain()
        .map(|cause| sanitize_user_text(&cause.to_string()))
        .collect();
    let marker = sanitize_user_text(DT_ERROR_CONTEXT_MARKER);
    let Some(root_context_index) = raw_chain.iter().rposition(|item| item == &marker) else {
        let context_count = raw_chain.len().saturating_sub(1);
        return (raw_chain, context_count);
    };

    let mut chain = Vec::with_capacity(raw_chain.len());
    let mut context_count = 0;
    for (index, item) in raw_chain.into_iter().enumerate() {
        if item == marker {
            continue;
        }
        if index < root_context_index {
            context_count += 1;
        }
        chain.push(item);
    }
    (chain, context_count)
}

fn detail_from_error_chain(error_chain: &[String]) -> Option<String> {
    let mut parts = Vec::new();
    for item in error_chain {
        if !item.is_empty() && !parts.contains(item) {
            parts.push(item.clone());
        }
    }
    (!parts.is_empty()).then(|| parts.join(": "))
}

fn captured_backtrace(error: &anyhow::Error) -> Option<String> {
    (error.backtrace().status() == BacktraceStatus::Captured)
        .then(|| format!("{}", error.backtrace()))
}

impl fmt::Display for ErrorReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ERROR [{}]: {}", self.code, self.message)?;
        if let Some(task_id) = &self.task_id {
            write!(f, "\nTASK: {task_id}")?;
        }
        if let Some(affected) = self.affected_resource() {
            write!(f, "\nAFFECTED: {affected}")?;
        }
        if let Some(detail) = &self.detail {
            write!(f, "\nDETAIL: {detail}")?;
        }
        if let Some(hint) = &self.hint {
            write!(f, "\nHINT: {hint}")?;
        }
        write!(f, "\n\nDIAGNOSTIC [{}]", self.code)?;
        write!(f, "\nSTAGE: {}", self.stage.diagnostic_name())?;
        if let Some(endpoint) = self.endpoint {
            write!(f, "\nENDPOINT: {}", endpoint.user_description())?;
        }
        if let Some(origin) = &self.origin {
            write!(f, "\nORIGIN: {}", origin.system)?;
            if let Some(code) = &origin.code {
                write!(f, "/{code}")?;
            }
        }
        for (index, item) in self.error_chain.iter().enumerate() {
            if index < self.context_count {
                write!(f, "\nCONTEXT {}: {item}", index + 1)?;
            } else {
                write!(f, "\nCAUSE {}: {item}", index - self.context_count + 1)?;
            }
        }
        if let Some(backtrace) = &self.backtrace {
            write!(f, "\nBACKTRACE:\n{backtrace}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use crate::error::{DtError, DtErrorContextExt};

    use super::*;

    #[test]
    fn preserves_structured_error_through_anyhow_context() {
        let error = DtErrorContext::new()
            .code(ErrorCode::ObjectNotFound)
            .message("inner message")
            .hint("inner hint")
            .object(ErrorObject {
                schema: Some("ape_dts".to_string()),
                table: Some("resume_position".to_string()),
                ..Default::default()
            })
            .origin(OriginError::new("postgres", Some("42P01")))
            .attach(io::Error::new(io::ErrorKind::NotFound, "relation missing")) // error-boundary-audit: allow-test
            .context("loading task state")
            .with_message("outer message")
            .with_stage(Stage::Resumer)
            .with_endpoint(EndpointRole::Metadata)
            .with_hint("outer hint")
            .with_task_id("task-42")
            .context("starting task");

        let report = ErrorReport::from_anyhow(&error);
        assert_eq!(report.code, ErrorCode::ObjectNotFound);
        assert_eq!(report.message, "outer message");
        assert_eq!(
            report.detail.as_deref(),
            Some("starting task: loading task state: relation missing")
        );
        assert_eq!(report.hint.as_deref(), Some("outer hint"));
        assert_eq!(report.stage, Stage::Resumer);
        assert_eq!(report.task_id.as_deref(), Some("task-42"));
        assert_eq!(report.endpoint, Some(EndpointRole::Metadata));
        assert_eq!(
            report.origin.as_ref().unwrap().code.as_deref(),
            Some("42P01")
        );
        assert_eq!(
            report.object.as_ref().unwrap().table.as_deref(),
            Some("resume_position")
        );
        assert_eq!(
            error.downcast_ref::<io::Error>().map(io::Error::kind),
            Some(io::ErrorKind::NotFound)
        );
        assert_eq!(
            report.error_chain,
            ["starting task", "loading task state", "relation missing"]
        );
        assert_eq!(report.context_count, 2);
        let rendered = report.to_string();
        assert!(rendered.contains("DIAGNOSTIC [MD001]"));
        assert!(!rendered.contains("LOCATION:"));
        assert!(rendered.contains("STAGE: resumer"));
        assert!(!rendered.contains("OPERATION:"));
        assert!(rendered.contains("TASK: task-42"));
        assert!(rendered.contains("ENDPOINT: metadata"));
        assert!(rendered.contains("AFFECTED: metadata store postgres ape_dts.resume_position"));
        assert!(rendered.contains("ORIGIN: postgres/42P01"));
        assert!(rendered.contains("CONTEXT 1: starting task"));
        assert!(rendered.contains("CAUSE 1: relation missing"));
    }

    #[test]
    fn maps_unknown_errors() {
        let unknown = anyhow::anyhow!("plain error");
        let report = ErrorReport::from_anyhow(&unknown);
        assert_eq!(report.code, ErrorCode::Unclassified);
        assert_eq!(report.detail.as_deref(), Some("plain error"));
    }

    #[test]
    fn text_and_json_include_redacted_detail_while_json_excludes_diagnostics() {
        let error = anyhow::anyhow!("password=secret, sql=INSERT, row_data=private")
            .context("internal worker context");
        let report = ErrorReport::from_anyhow(&error);
        let rendered = report.to_string();
        let json = serde_json::to_string(&report).unwrap();

        assert_eq!(report.message, ErrorCode::Unclassified.default_message());
        assert!(rendered.contains("DIAGNOSTIC [IN999]"));
        assert!(rendered.contains("CONTEXT 1: internal worker context"));
        assert!(rendered.contains("CAUSE 1:"));
        assert!(rendered.contains("sql=INSERT"));
        assert!(rendered.contains("row_data=private"));
        assert!(!rendered.contains("password=secret"));
        assert!(json.contains("internal worker context"));
        assert!(json.contains("sql=INSERT"));
        assert!(json.contains("row_data=private"));
        assert!(!json.contains("password=secret"));
        for diagnostic_field in [
            "phase",
            "stage",
            "origin",
            "error_chain",
            "context_count",
            "backtrace",
        ] {
            assert!(!json.contains(diagnostic_field));
        }
        assert_eq!(report.context_count, 1);
        assert_eq!(report.error_chain.len(), 2);
    }

    #[test]
    fn serializes_schema_version_and_redacts_user_text() {
        let error = DtError::ConfigError("password=secret is invalid".to_string())
            .with_code(ErrorCode::InvalidConfig)
            .with_message("password=secret is invalid")
            .with_hint("set token=abc123 before retrying");
        let error = error.context("endpoint=mysql://user:hunter2@localhost:3306/db");
        let report = ErrorReport::from_anyhow(&error);
        let json = serde_json::to_value(&report).unwrap();

        assert_eq!(report.schema_version, 1);
        assert_eq!(json["schema_version"], 1);
        let rendered = report.to_string();
        assert!(!rendered.contains("secret"));
        assert!(!rendered.contains("hunter2"));
        assert!(!rendered.contains("abc123"));
        assert!(rendered.contains("[redacted]"));
    }

    #[test]
    fn raw_provider_errors_remain_unclassified() {
        let url_error = anyhow::Error::new(url::Url::parse("://bad").unwrap_err());
        assert_eq!(
            ErrorReport::from_anyhow(&url_error).code,
            ErrorCode::Unclassified
        );

        let yaml_error =
            anyhow::Error::new(serde_yaml::from_str::<serde_yaml::Value>("key: [").unwrap_err());
        assert_eq!(
            ErrorReport::from_anyhow(&yaml_error).code,
            ErrorCode::Unclassified
        );
    }
}
