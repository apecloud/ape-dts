use std::{backtrace::BacktraceStatus, fmt};

use serde::Serialize;

use super::{
    error_context::{DtErrorContexts, DT_ERROR_CONTEXT_MARKER},
    provider::{classify_raw_errors, provider_error_detail},
    ClassifyError, DtError, DtErrorContext, EndpointRole, ErrorCode, ErrorObject, Stage,
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
    pub error_chain: Vec<String>,
    #[serde(skip_serializing)]
    pub context_count: usize,
    #[serde(skip_serializing)]
    pub backtrace: Option<String>,
}

impl ErrorReport {
    pub fn from_anyhow(error: &anyhow::Error) -> Self {
        let metadata = CollectedMetadata::from_anyhow(error);
        let (error_chain, context_count) = split_error_chain(error);
        let detail = detail_from_error_chain(&error_chain);
        let backtrace = captured_backtrace(error);
        let code = metadata.code().unwrap_or(ErrorCode::Unclassified);
        Self {
            schema_version: ERROR_REPORT_SCHEMA_VERSION,
            code,
            message: sanitize_user_text(
                metadata.message().unwrap_or_else(|| code.default_message()),
            ),
            detail,
            hint: Some(sanitize_user_text(
                metadata.hint().unwrap_or_else(|| code.default_hint()),
            )),
            stage: metadata.stage().unwrap_or(Stage::Unknown),
            task_id: metadata.task_id().map(str::to_string),
            endpoint: metadata.endpoint(),
            object: metadata.object(),
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
        if let Some(object) = &self.object {
            if let Some(name) = format_object(object) {
                parts.push(name);
            }
        }
        (!parts.is_empty()).then(|| parts.join(" "))
    }
}

#[derive(Default)]
struct CollectedMetadata {
    explicit: MetadataValues,
    project: MetadataValues,
    provider: MetadataValues,
    system: MetadataValues,
}

impl CollectedMetadata {
    fn from_anyhow(error: &anyhow::Error) -> Self {
        let mut metadata = Self::default();
        if let Some(contexts) = error.downcast_ref::<DtErrorContexts>() {
            for context in contexts.iter_outer_to_inner() {
                metadata.explicit.push(context);
            }
        }
        if let Some(error) = error.downcast_ref::<DtError>() {
            metadata.project.push(&error.classify());
        }
        let (provider_contexts, system_contexts) = classify_raw_errors(error);
        for context in provider_contexts {
            metadata.provider.push(&context);
        }
        for context in system_contexts {
            metadata.system.push(&context);
        }
        metadata
    }

    fn code(&self) -> Option<ErrorCode> {
        self.provider
            .codes
            .first()
            .or_else(|| self.explicit.codes.first())
            .or_else(|| self.project.codes.first())
            .or_else(|| self.system.codes.first())
            .copied()
    }

    fn message(&self) -> Option<&str> {
        self.explicit
            .messages
            .first()
            .or_else(|| self.project.messages.first())
            .or_else(|| self.provider.messages.first())
            .or_else(|| self.system.messages.first())
            .map(String::as_str)
    }

    fn hint(&self) -> Option<&str> {
        self.explicit
            .hints
            .first()
            .or_else(|| self.project.hints.first())
            .or_else(|| self.provider.hints.first())
            .or_else(|| self.system.hints.first())
            .map(String::as_str)
    }

    fn stage(&self) -> Option<Stage> {
        self.provider
            .stages
            .first()
            .or_else(|| self.explicit.stages.last())
            .or_else(|| self.project.stages.first())
            .or_else(|| self.system.stages.first())
            .copied()
    }

    fn task_id(&self) -> Option<&str> {
        self.explicit
            .task_ids
            .first()
            .or_else(|| self.project.task_ids.first())
            .or_else(|| self.provider.task_ids.first())
            .or_else(|| self.system.task_ids.first())
            .map(String::as_str)
    }

    fn endpoint(&self) -> Option<EndpointRole> {
        self.provider
            .endpoints
            .first()
            .or_else(|| self.project.endpoints.first())
            .or_else(|| self.explicit.endpoints.last())
            .or_else(|| self.system.endpoints.first())
            .copied()
    }

    fn object(&self) -> Option<ErrorObject> {
        let mut merged: Option<ErrorObject> = None;
        for object in self
            .provider
            .objects
            .iter()
            .chain(&self.project.objects)
            .chain(self.explicit.objects.iter().rev())
            .chain(&self.system.objects)
        {
            match &mut merged {
                Some(merged) => merged.fill_missing_from(object),
                None => merged = Some(object.clone()),
            }
        }
        merged
    }
}

#[derive(Default)]
struct MetadataValues {
    codes: Vec<ErrorCode>,
    messages: Vec<String>,
    hints: Vec<String>,
    stages: Vec<Stage>,
    task_ids: Vec<String>,
    endpoints: Vec<EndpointRole>,
    objects: Vec<ErrorObject>,
}

impl MetadataValues {
    fn push(&mut self, context: &DtErrorContext) {
        self.codes.extend(context.code);
        self.messages.extend(context.message.clone());
        self.hints.extend(context.hint.clone());
        self.stages.extend(context.stage);
        self.task_ids.extend(context.task_id.clone());
        self.endpoints.extend(context.endpoint);
        self.objects.extend(context.object.clone());
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
        .map(|cause| {
            let display = cause.to_string();
            let detail = match provider_error_detail(cause) {
                Some(provider) => format!("{provider}: {display}"),
                None => display,
            };
            sanitize_user_text(&detail)
        })
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
    let backtrace = error.backtrace();
    (backtrace.status() == BacktraceStatus::Captured).then(|| backtrace.to_string())
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
    use crate::error::{DtError, DtErrorContext, DtErrorContextExt};

    use super::*;

    #[test]
    fn preserves_structured_error_through_anyhow_context() {
        let source = "relation missing".parse::<u64>().unwrap_err();
        let error = anyhow::Error::new(source)
            .dt_context(
                DtErrorContext::new()
                    .with_code(ErrorCode::ObjectNotFound)
                    .with_message("inner message")
                    .with_hint("inner hint")
                    .with_object(ErrorObject {
                        schema: Some("ape_dts".to_string()),
                        table: Some("resume_position".to_string()),
                        ..Default::default()
                    }),
            ) // error-boundary-audit: allow-test
            .context("loading task state")
            .message("outer message")
            .stage(Stage::Resumer)
            .endpoint(EndpointRole::Metadata)
            .hint("outer hint")
            .task_id("task-42")
            .context("starting task");

        let report = ErrorReport::from_anyhow(&error);
        assert_eq!(report.code, ErrorCode::ObjectNotFound);
        assert_eq!(report.message, "outer message");
        assert_eq!(
            report.detail.as_deref(),
            Some("starting task: loading task state: invalid digit found in string")
        );
        assert_eq!(report.hint.as_deref(), Some("outer hint"));
        assert_eq!(report.stage, Stage::Resumer);
        assert_eq!(report.task_id.as_deref(), Some("task-42"));
        assert_eq!(report.endpoint, Some(EndpointRole::Metadata));
        assert_eq!(
            report.object.as_ref().unwrap().table.as_deref(),
            Some("resume_position")
        );
        assert!(error.downcast_ref::<std::num::ParseIntError>().is_some());
        assert_eq!(
            report.error_chain,
            [
                "starting task",
                "loading task state",
                "invalid digit found in string"
            ]
        );
        assert_eq!(report.context_count, 2);
        let rendered = report.to_string();
        assert!(rendered.contains("DIAGNOSTIC [MD001]"));
        assert!(!rendered.contains("LOCATION:"));
        assert!(rendered.contains("STAGE: resumer"));
        assert!(!rendered.contains("OPERATION:"));
        assert!(rendered.contains("TASK: task-42"));
        assert!(rendered.contains("ENDPOINT: metadata"));
        assert!(rendered.contains("AFFECTED: metadata store ape_dts.resume_position"));
        assert!(!rendered.contains("ORIGIN:"));
        assert!(rendered.contains("CONTEXT 1: starting task"));
        assert!(rendered.contains("CAUSE 1: invalid digit found in string"));
    }

    #[test]
    fn classifies_raw_supported_provider_errors() {
        let error = anyhow::Error::new(sqlx::Error::PoolTimedOut)
            .code(ErrorCode::StatementFailed)
            .stage(Stage::Sinker)
            .endpoint(EndpointRole::Destination)
            .context("pipeline.start failed");

        let report = ErrorReport::from_anyhow(&error);
        assert_eq!(report.code, ErrorCode::ConnectionTimeout);
        assert_eq!(report.stage, Stage::Sinker);
        assert_eq!(report.endpoint, Some(EndpointRole::Destination));
    }

    #[test]
    fn classifies_project_errors_unless_context_overrides_the_code() {
        let source = std::io::Error::other("invalid port");
        let error = anyhow::Error::new(source)
            .context(DtError::InvalidConfig("invalid port".to_string()))
            .context("loading source config");
        let report = ErrorReport::from_anyhow(&error);
        assert_eq!(report.code, ErrorCode::InvalidConfig);
        assert!(error.downcast_ref::<std::io::Error>().is_some());

        let error = error.code(ErrorCode::InvariantViolated);
        assert_eq!(
            ErrorReport::from_anyhow(&error).code,
            ErrorCode::InvariantViolated
        );

        let redis_error = anyhow::Error::new(DtError::RedisRdbError(
            "invalid snapshot payload".to_string(),
        ));
        let report = ErrorReport::from_anyhow(&redis_error);
        assert_eq!(report.code, ErrorCode::StatementFailed);
    }

    #[test]
    fn text_and_json_include_redacted_detail_while_json_excludes_diagnostics() {
        let error = anyhow::anyhow!("password=secret, sql=INSERT, row_data=private")
            .message("password=secret is invalid")
            .hint("set token=abc123 before retrying")
            .context("internal worker context");
        let report = ErrorReport::from_anyhow(&error);
        let rendered = report.to_string();
        let json_value = serde_json::to_value(&report).unwrap();
        let json = json_value.to_string();

        assert_eq!(report.schema_version, ERROR_REPORT_SCHEMA_VERSION);
        assert_eq!(json_value["schema_version"], ERROR_REPORT_SCHEMA_VERSION);
        assert!(rendered.contains("DIAGNOSTIC [IN999]"));
        assert!(rendered.contains("CONTEXT 1: internal worker context"));
        assert!(rendered.contains("CAUSE 1:"));
        assert!(rendered.contains("sql=INSERT"));
        assert!(rendered.contains("row_data=private"));
        assert!(!rendered.contains("password=secret"));
        assert!(!rendered.contains("abc123"));
        assert!(rendered.contains("[redacted]"));
        assert!(json.contains("internal worker context"));
        assert!(json.contains("sql=INSERT"));
        assert!(json.contains("row_data=private"));
        assert!(!json.contains("password=secret"));
        assert!(!json.contains("abc123"));
        for diagnostic_field in [
            "phase",
            "stage",
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
    fn unsupported_raw_errors_remain_unclassified() {
        let url_error = anyhow::Error::new(url::Url::parse("://bad").unwrap_err());
        assert_eq!(
            ErrorReport::from_anyhow(&url_error).code,
            ErrorCode::Unclassified
        );
    }
}
