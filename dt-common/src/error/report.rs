use std::{backtrace::BacktraceStatus, fmt};

use chrono::{SecondsFormat, Utc};
use serde::Serialize;

use super::{
    classifier::collect_contexts, DtErrorContext, EndpointRole, ErrorCode, ErrorObject, Stage,
};

pub const ERROR_REPORT_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ErrorReport {
    pub schema_version: u16,
    pub code: ErrorCode,
    pub messages: Vec<String>,
    pub details: Vec<String>,
    pub hints: Vec<String>,
    pub stage: Stage,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<EndpointRole>,
    pub objects: Vec<ErrorObject>,
    timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    backtrace: Option<String>,
}

impl ErrorReport {
    pub fn from_anyhow(error: &anyhow::Error) -> Self {
        let mut report = Self {
            schema_version: ERROR_REPORT_SCHEMA_VERSION,
            code: ErrorCode::Unclassified,
            messages: Vec::new(),
            details: Vec::new(),
            hints: Vec::new(),
            stage: Stage::Unknown,
            task_id: None,
            endpoint: None,
            objects: Vec::new(),
            timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true),
            backtrace: captured_backtrace(error),
        };

        for context in collect_contexts(error) {
            report.push_context(&context);
        }

        if report.messages.is_empty() {
            report
                .messages
                .push(sanitize_user_text(report.code.default_message()));
        }
        if report.hints.is_empty() {
            report
                .hints
                .push(sanitize_user_text(report.code.default_hint()));
        }
        report
    }

    pub fn to_log_json(&self) -> String {
        serde_json::to_string(self).expect("ErrorReport fields should always serialize to JSON")
    }

    fn push_context(&mut self, context: &DtErrorContext) {
        if self.code == ErrorCode::Unclassified {
            if let Some(code) = context.code {
                self.code = code;
            }
        }
        if let Some(message) = &context.message {
            push_unique(&mut self.messages, sanitize_user_text(message));
        }
        if let Some(detail) = &context.detail {
            self.push_detail(detail);
        }
        if let Some(hint) = &context.hint {
            push_unique(&mut self.hints, sanitize_user_text(hint));
        }
        if self.stage == Stage::Unknown {
            if let Some(stage) = context.stage {
                self.stage = stage;
            }
        }
        if self.task_id.is_none() {
            self.task_id.clone_from(&context.task_id);
        }
        if self.endpoint.is_none() {
            self.endpoint = context.endpoint;
        }
        if let Some(object) = &context.object {
            push_unique(&mut self.objects, object.clone());
        }
    }

    fn push_detail(&mut self, detail: impl AsRef<str>) {
        let detail = sanitize_user_text(detail.as_ref());
        if !detail.is_empty() {
            push_unique(&mut self.details, detail);
        }
    }
}

fn captured_backtrace(error: &anyhow::Error) -> Option<String> {
    let backtrace = error.backtrace();
    (backtrace.status() == BacktraceStatus::Captured).then(|| backtrace.to_string())
}

fn push_unique<T: PartialEq>(values: &mut Vec<T>, value: T) {
    if !values.contains(&value) {
        values.push(value);
    }
}

fn sanitize_user_text(value: &str) -> String {
    rtb_redact::string(value).into_owned()
}

impl fmt::Display for ErrorReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ERROR REPORT\n  [{}]:", self.code)?;
        for (index, detail) in self.details.iter().enumerate() {
            write!(f, "\n    {index}: {detail}")?;
        }
        if let Some(backtrace) = &self.backtrace {
            write!(f, "\n  BACKTRACE:")?;
            for line in backtrace.lines() {
                write!(f, "\n    {}", line.trim_start())?;
            }
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
        assert_eq!(report.messages, ["outer message", "inner message"]);
        assert_eq!(report.hints, ["outer hint", "inner hint"]);
        assert_eq!(report.stage, Stage::Resumer);
        assert_eq!(report.task_id.as_deref(), Some("task-42"));
        assert_eq!(report.endpoint, Some(EndpointRole::Metadata));
        assert_eq!(report.objects[0].table.as_deref(), Some("resume_position"));
        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["code"], "MD001");
        assert_eq!(json["stage"], "resumer");
        assert_eq!(json["task_id"], "task-42");
        assert_eq!(json["endpoint"], "metadata");
        for scalar_field in ["code", "stage", "task_id", "endpoint"] {
            assert!(!json[scalar_field].is_array());
        }
        assert!(error.downcast_ref::<std::num::ParseIntError>().is_some());
        assert_eq!(
            report.details,
            [
                "starting task",
                "loading task state",
                "invalid digit found in string"
            ]
        );
        let rendered = report.to_string();
        assert!(rendered.starts_with("ERROR REPORT\n  [MD001]:"));
        assert!(rendered.contains("    0: starting task"));
        assert!(rendered.contains("    2: invalid digit found in string"));
        for omitted_label in ["MESSAGE", "HINT", "STAGE", "TASK", "ENDPOINT", "AFFECTED"] {
            assert!(!rendered.contains(omitted_label));
        }
    }

    #[test]
    fn explicit_context_owns_scalars_and_provider_enriches_detail() {
        let error = anyhow::Error::new(sqlx::Error::PoolTimedOut)
            .code(ErrorCode::StatementFailed)
            .stage(Stage::Sinker)
            .endpoint(EndpointRole::Destination)
            .context("pipeline.start failed");

        let report = ErrorReport::from_anyhow(&error);
        assert_eq!(report.code, ErrorCode::StatementFailed);
        assert_eq!(report.stage, Stage::Sinker);
        assert_eq!(report.endpoint, Some(EndpointRole::Destination));
        assert_eq!(
            report.messages,
            [ErrorCode::StatementFailed.default_message()]
        );
        assert_eq!(
            report.details,
            [
                "pipeline.start failed",
                "sqlx: pool timed out while waiting for an open connection"
            ]
        );
    }

    #[test]
    fn sqlx_wrapper_and_source_are_distinct_details() {
        let error = anyhow::Error::new(sqlx::Error::Io(std::io::Error::other(
            "could not resolve database address",
        )));

        let report = ErrorReport::from_anyhow(&error);

        assert_eq!(report.code, ErrorCode::ConnectionFailed);
        assert_eq!(
            report.details,
            [
                "sqlx: error communicating with database",
                "could not resolve database address"
            ]
        );
    }

    #[test]
    fn collects_project_and_source_error_contexts() {
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
    fn json_includes_internal_fields_while_text_stays_minimal() {
        let error = anyhow::anyhow!("password=secret, sql=INSERT, row_data=private")
            .message("password=secret is invalid")
            .hint("set token=abc123 before retrying")
            .context("internal worker context");
        let mut report = ErrorReport::from_anyhow(&error);
        report.timestamp = "2026-07-28T10:20:30.123456Z".to_string();
        report.backtrace = Some("   0: internal frame\n   1: caller frame".to_string());
        let rendered = report.to_string();
        let json_value = serde_json::to_value(&report).unwrap();
        let json = json_value.to_string();
        let log_json = report.to_log_json();
        let log_json_value: serde_json::Value = serde_json::from_str(&log_json).unwrap();

        assert_eq!(report.schema_version, ERROR_REPORT_SCHEMA_VERSION);
        assert_eq!(json_value["schema_version"], ERROR_REPORT_SCHEMA_VERSION);
        assert_eq!(json_value["code"], serde_json::json!("IN999"));
        for scalar_field in ["code", "stage"] {
            assert!(!json_value[scalar_field].is_array());
        }
        assert!(json_value.get("task_id").is_none());
        assert!(json_value.get("endpoint").is_none());
        for array_field in ["messages", "details", "hints", "objects"] {
            assert!(json_value[array_field].is_array());
        }
        assert!(rendered.starts_with("ERROR REPORT\n  [IN999]:"));
        assert!(rendered.contains("    0: internal worker context"));
        assert!(rendered.contains("sql=INSERT"));
        assert!(rendered.contains("row_data=private"));
        assert!(!rendered.contains("password=secret"));
        assert!(!rendered.contains("abc123"));
        assert!(rendered.contains("  BACKTRACE:\n    0: internal frame\n    1: caller frame"));
        assert!(!rendered.contains("MESSAGE"));
        assert!(!rendered.contains("HINT"));
        assert!(json.contains("internal worker context"));
        assert!(json.contains("sql=INSERT"));
        assert!(json.contains("row_data=private"));
        assert!(!json.contains("password=secret"));
        assert!(!json.contains("abc123"));
        assert!(!json.contains("error_chain"));
        assert!(!json.contains("context_count"));
        assert_eq!(json_value["timestamp"], "2026-07-28T10:20:30.123456Z");
        assert_eq!(
            json_value["backtrace"],
            "   0: internal frame\n   1: caller frame"
        );
        assert_eq!(log_json_value["timestamp"], "2026-07-28T10:20:30.123456Z");
        assert_eq!(
            log_json_value["backtrace"],
            "   0: internal frame\n   1: caller frame"
        );
        assert_eq!(log_json_value["code"], "IN999");
        assert!(log_json_value["messages"].is_array());
        assert!(!log_json.contains('\n'));
        assert_eq!(report.details.len(), 2);
    }

    #[test]
    fn report_timestamp_is_utc_rfc3339() {
        let report = ErrorReport::from_anyhow(&anyhow::anyhow!("failed"));
        let timestamp = chrono::DateTime::parse_from_rfc3339(&report.timestamp).unwrap();

        assert_eq!(timestamp.offset().local_minus_utc(), 0);
    }

    #[test]
    fn unsupported_raw_errors_use_unclassified_fallback() {
        let url_error = anyhow::Error::new(url::Url::parse("://bad").unwrap_err());
        assert_eq!(
            ErrorReport::from_anyhow(&url_error).code,
            ErrorCode::Unclassified
        );
    }

    #[test]
    fn outermost_context_owns_scalars_and_arrays_remove_duplicates() {
        let error = anyhow::anyhow!("same detail")
            .code(ErrorCode::ConnectionTimeout)
            .code(ErrorCode::StatementFailed)
            .message("same message")
            .message("same message")
            .stage(Stage::Extractor)
            .stage(Stage::Sinker)
            .task_id("inner-task")
            .task_id("outer-task")
            .endpoint(EndpointRole::Source)
            .endpoint(EndpointRole::Destination)
            .context("same detail");

        let report = ErrorReport::from_anyhow(&error);
        assert_eq!(report.code, ErrorCode::StatementFailed);
        assert_eq!(report.messages, ["same message"]);
        assert_eq!(report.stage, Stage::Sinker);
        assert_eq!(report.task_id.as_deref(), Some("outer-task"));
        assert_eq!(report.endpoint, Some(EndpointRole::Destination));
        assert_eq!(report.details, ["same detail"]);
        assert_eq!(
            report.to_string(),
            "ERROR REPORT\n  [DB001]:\n    0: same detail"
        );
        let rendered = report.to_string();
        assert!(!rendered.contains("MESSAGE"));
        assert!(!rendered.contains("HINT"));
    }

    #[test]
    fn affected_objects_remain_independent_array_items() {
        let error = anyhow::anyhow!("object failure")
            .object(ErrorObject {
                schema: Some("sales".to_string()),
                ..Default::default()
            })
            .object(ErrorObject {
                table: Some("orders".to_string()),
                ..Default::default()
            });

        let report = ErrorReport::from_anyhow(&error);
        assert_eq!(report.objects.len(), 2);
        assert_eq!(report.objects[0].table.as_deref(), Some("orders"));
        assert_eq!(report.objects[1].schema.as_deref(), Some("sales"));
        assert!(!report.to_string().contains("AFFECTED"));
    }
}
