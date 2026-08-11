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

        let contexts = collect_contexts(error);
        for context in &contexts {
            report.push_context(context);
        }

        report.code = contexts
            .iter()
            .rev()
            .filter_map(|context| context.code)
            .find(|code| *code != ErrorCode::Unclassified)
            .unwrap_or(ErrorCode::Unclassified);
        report.stage = contexts
            .iter()
            .rev()
            .filter_map(|context| context.stage)
            .find(|stage| *stage != Stage::Unknown)
            .unwrap_or(Stage::Unknown);
        report.endpoint = contexts.iter().rev().find_map(|context| context.endpoint);

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
        serde_json::to_string(self).unwrap_or_else(|_| {
            "Unknown error: failed to serialize ErrorReport fields to JSON.".to_owned()
        })
    }

    fn push_context(&mut self, context: &DtErrorContext) {
        if let Some(message) = &context.message {
            push_unique(&mut self.messages, sanitize_user_text(message));
        }
        if let Some(detail) = &context.detail {
            self.push_detail(detail);
        }
        if let Some(hint) = &context.hint {
            push_unique(&mut self.hints, sanitize_user_text(hint));
        }
        if self.task_id.is_none() {
            self.task_id.clone_from(&context.task_id);
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
        write!(
            f,
            "ERROR REPORT\n  [{}]: {}",
            self.code,
            self.messages.join("; ")
        )?;
        if !self.objects.is_empty() {
            write!(f, "\n  AFFECTED OBJECT: ")?;
            for (object_index, object) in self.objects.iter().enumerate() {
                if object_index > 0 {
                    write!(f, "; ")?;
                }
                let mut field_index = 0;
                for (name, value) in [
                    ("schema", object.schema.as_deref()),
                    ("table", object.table.as_deref()),
                    ("column", object.column.as_deref()),
                    ("constraint", object.constraint.as_deref()),
                ] {
                    if let Some(value) = value {
                        if field_index > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{name}={value}")?;
                        field_index += 1;
                    }
                }
            }
        }
        if !self.details.is_empty() {
            write!(f, "\n\n  CAUSED BY:")?;
            for (index, detail) in self.details.iter().enumerate() {
                write!(f, "\n    {index}: {detail}")?;
            }
        }
        if let Some(backtrace) = &self.backtrace {
            write!(f, "\n\n  BACKTRACE:")?;
            for line in backtrace.lines() {
                write!(f, "\n    {}", line.trim_end())?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{io, num::ParseIntError};

    use anyhow::anyhow;
    use chrono::DateTime;
    use url::Url;

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
        assert!(error.downcast_ref::<ParseIntError>().is_some());
        assert_eq!(
            report.details,
            [
                "starting task",
                "loading task state",
                "invalid digit found in string"
            ]
        );
        let rendered = report.to_string();
        assert!(rendered.starts_with(
            "ERROR REPORT\n  [MD001]: outer message; inner message\n  AFFECTED OBJECT: schema=ape_dts, table=resume_position\n\n  CAUSED BY:"
        ));
        assert!(rendered.contains("    0: starting task"));
        assert!(rendered.contains("    2: invalid digit found in string"));
        for omitted_label in ["MESSAGE", "HINT", "STAGE", "TASK", "ENDPOINT"] {
            assert!(!rendered.contains(omitted_label));
        }
    }

    #[test]
    fn provider_code_owns_identity_while_context_supplies_scope() {
        let error = anyhow::Error::new(sqlx::Error::PoolTimedOut)
            .code(ErrorCode::StatementFailed)
            .stage(Stage::Sinker)
            .endpoint(EndpointRole::Destination)
            .context("pipeline.start failed");

        let report = ErrorReport::from_anyhow(&error);
        assert_eq!(report.code, ErrorCode::ConnectionTimeout);
        assert_eq!(report.stage, Stage::Sinker);
        assert_eq!(report.endpoint, Some(EndpointRole::Destination));
        assert_eq!(
            report.messages,
            [ErrorCode::ConnectionTimeout.default_message()]
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
    fn explicit_code_is_fallback_for_unclassified_provider_error() {
        let error = sqlx::Error::RowNotFound.code(ErrorCode::StatementFailed);

        let report = ErrorReport::from_anyhow(&error);

        assert_eq!(report.code, ErrorCode::StatementFailed);
        assert_eq!(
            report.details,
            ["sqlx: no rows returned by a query that expected to return at least one row"]
        );
    }

    #[test]
    fn sqlx_wrapper_and_source_are_distinct_details() {
        let error = anyhow::Error::new(sqlx::Error::Io(io::Error::other(
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
        let source = io::Error::other("invalid port");
        let error = anyhow::Error::new(source)
            .context(DtError::InvalidConfig("invalid port".to_string()))
            .context("loading source config");
        let report = ErrorReport::from_anyhow(&error);
        assert_eq!(report.code, ErrorCode::InvalidConfig);
        assert!(error.downcast_ref::<io::Error>().is_some());

        let error = error.code(ErrorCode::InvariantViolated);
        assert_eq!(
            ErrorReport::from_anyhow(&error).code,
            ErrorCode::InvalidConfig
        );

        let redis_error = anyhow::Error::new(DtError::RedisRdbError(
            "invalid snapshot payload".to_string(),
        ));
        let report = ErrorReport::from_anyhow(&redis_error);
        assert_eq!(report.code, ErrorCode::StatementFailed);
    }

    #[test]
    fn json_includes_internal_fields_while_text_stays_minimal() {
        let error = anyhow!("password=secret, sql=INSERT, row_data=private")
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
        assert!(rendered.contains("  CAUSED BY:\n    0: internal worker context"));
        assert!(rendered.contains("    0: internal worker context"));
        assert!(rendered.contains("sql=INSERT"));
        assert!(rendered.contains("row_data=private"));
        assert!(!rendered.contains("password=secret"));
        assert!(!rendered.contains("abc123"));
        assert!(rendered.contains("  BACKTRACE:\n       0: internal frame\n       1: caller frame"));
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
        let report = ErrorReport::from_anyhow(&anyhow!("failed"));
        let timestamp = DateTime::parse_from_rfc3339(&report.timestamp).unwrap();

        assert_eq!(timestamp.offset().local_minus_utc(), 0);
    }

    #[test]
    fn unsupported_raw_errors_use_unclassified_fallback() {
        let url_error = anyhow::Error::new(Url::parse("://bad").unwrap_err());
        assert_eq!(
            ErrorReport::from_anyhow(&url_error).code,
            ErrorCode::Unclassified
        );
    }

    #[test]
    fn scalar_directions_are_field_specific_and_arrays_remove_duplicates() {
        let error = anyhow!("same detail")
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
        assert_eq!(report.code, ErrorCode::ConnectionTimeout);
        assert_eq!(report.messages, ["same message"]);
        assert_eq!(report.stage, Stage::Extractor);
        assert_eq!(report.task_id.as_deref(), Some("outer-task"));
        assert_eq!(report.endpoint, Some(EndpointRole::Source));
        assert_eq!(report.details, ["same detail"]);
        let rendered = report.to_string();
        assert!(rendered.starts_with(
            "ERROR REPORT\n  [CN002]: same message\n\n  CAUSED BY:\n    0: same detail"
        ));
        assert!(!rendered.contains("MESSAGE"));
        assert!(!rendered.contains("HINT"));
    }

    #[test]
    fn innermost_stage_wins_over_pipeline_and_task() {
        let error = anyhow!("sink failed")
            .stage(Stage::Sinker)
            .stage(Stage::Pipeline)
            .stage(Stage::Task);

        let report = ErrorReport::from_anyhow(&error);

        assert_eq!(report.stage, Stage::Sinker);
    }

    #[test]
    fn affected_objects_remain_independent_array_items() {
        let error = anyhow!("object failure")
            .object(ErrorObject {
                schema: Some("archive".to_string()),
                table: Some("orders".to_string()),
                ..Default::default()
            })
            .object(ErrorObject {
                schema: Some("sales".to_string()),
                table: Some("orders".to_string()),
                constraint: Some("orders_pkey".to_string()),
                ..Default::default()
            })
            .message("Data violates a destination constraint")
            .message("The destination rejected the row")
            .context("insert operation failed");

        let mut report = ErrorReport::from_anyhow(&error);
        assert_eq!(report.objects.len(), 2);
        assert_eq!(report.objects[0].schema.as_deref(), Some("sales"));
        assert_eq!(report.objects[1].schema.as_deref(), Some("archive"));
        report.backtrace = None;
        assert_eq!(
            report.to_string(),
            "ERROR REPORT\n  [IN999]: The destination rejected the row; Data violates a destination constraint\n  AFFECTED OBJECT: schema=sales, table=orders, constraint=orders_pkey; schema=archive, table=orders\n\n  CAUSED BY:\n    0: insert operation failed\n    1: object failure"
        );
    }
}
