use std::{error::Error as StdError, fmt};

use serde::Serialize;

use super::{
    classify_mongodb_error, classify_redis_error, classify_sqlx_error, DtError, EndpointRole,
    ErrorCode, ErrorObject, OriginError, SqlxProvider, Stage,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(skip_serializing)]
    pub stage: Stage,
    #[serde(skip_serializing)]
    pub operation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<EndpointRole>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<ErrorObject>,
    #[serde(skip_serializing)]
    pub origin: Option<OriginError>,
    #[serde(skip_serializing)]
    pub contexts: Vec<String>,
    #[serde(skip_serializing)]
    pub diagnostic_message: Option<String>,
    #[serde(skip_serializing)]
    pub location: Option<String>,
}

impl ErrorReport {
    pub fn from_anyhow(error: &anyhow::Error) -> Self {
        if let Some(dt_error) = error.downcast_ref::<DtError>() {
            return Self::from_dt_error(dt_error, contexts_before::<DtError>(error));
        }

        if let Some(sqlx_error) = error.downcast_ref::<sqlx::Error>() {
            let classification = classify_sqlx_error(
                sqlx_error,
                SqlxProvider::Unknown,
                ErrorCode::StatementFailed,
            );
            return Self::new(
                classification.code,
                Stage::Unknown,
                sqlx_error.to_string(),
                contexts_before::<sqlx::Error>(error),
            )
            .with_origin(classification.origin)
            .with_object(classification.object);
        }

        if let Some(redis_error) = error.downcast_ref::<redis::RedisError>() {
            let classification = classify_redis_error(redis_error, ErrorCode::StatementFailed);
            return Self::new(
                classification.code,
                Stage::Unknown,
                redis_error.to_string(),
                contexts_before::<redis::RedisError>(error),
            )
            .with_origin(classification.origin);
        }

        if let Some(mongodb_error) = error.downcast_ref::<mongodb::error::Error>() {
            let classification = classify_mongodb_error(mongodb_error, ErrorCode::StatementFailed);
            return Self::new(
                classification.code,
                Stage::Unknown,
                mongodb_error.to_string(),
                contexts_before::<mongodb::error::Error>(error),
            )
            .with_origin(classification.origin);
        }

        if let Some(join_error) = error.downcast_ref::<tokio::task::JoinError>() {
            return Self::new(
                ErrorCode::WorkerFailed,
                Stage::Task,
                join_error.to_string(),
                contexts_before::<tokio::task::JoinError>(error),
            );
        }

        if let Some(url_error) = error.downcast_ref::<url::ParseError>() {
            return Self::new(
                ErrorCode::InvalidConfig,
                Stage::Bootstrap,
                url_error.to_string(),
                contexts_before::<url::ParseError>(error),
            );
        }

        if let Some(yaml_error) = error.downcast_ref::<serde_yaml::Error>() {
            return Self::new(
                ErrorCode::InvalidConfig,
                Stage::Bootstrap,
                yaml_error.to_string(),
                contexts_before::<serde_yaml::Error>(error),
            );
        }

        if let Some(io_error) = error.downcast_ref::<std::io::Error>() {
            return Self::new(
                ErrorCode::IoFailed,
                Stage::Unknown,
                io_error.to_string(),
                contexts_before::<std::io::Error>(error),
            );
        }

        let mut chain: Vec<_> = error.chain().map(ToString::to_string).collect();
        let diagnostic_message = chain.pop().unwrap_or_else(|| error.to_string());
        Self::new(
            ErrorCode::Unclassified,
            Stage::Unknown,
            diagnostic_message,
            chain,
        )
    }

    fn from_dt_error(error: &DtError, contexts: Vec<String>) -> Self {
        let stage = error.root_stage().unwrap_or(Stage::Unknown);
        Self {
            schema_version: ERROR_REPORT_SCHEMA_VERSION,
            code: error.code(),
            message: sanitize_user_text(&error.message),
            detail: error.detail.as_deref().map(sanitize_user_text),
            hint: Some(sanitize_user_text(
                error
                    .hint
                    .as_deref()
                    .unwrap_or_else(|| error.code().default_hint()),
            )),
            phase: stage.user_description().map(str::to_string),
            stage,
            operation: error.root_operation().map(str::to_string),
            task_id: error.task_id.clone(),
            endpoint: error.endpoint,
            object: error.object.clone(),
            origin: error.origin_error().cloned(),
            contexts,
            diagnostic_message: StdError::source(error).map(ToString::to_string),
            location: Some(format!(
                "{}:{}:{}",
                error.location.file(),
                error.location.line(),
                error.location.column()
            )),
        }
    }

    fn new(
        code: ErrorCode,
        stage: Stage,
        diagnostic_message: String,
        contexts: Vec<String>,
    ) -> Self {
        Self {
            schema_version: ERROR_REPORT_SCHEMA_VERSION,
            code,
            message: code.default_message().to_string(),
            detail: None,
            hint: Some(code.default_hint().to_string()),
            phase: stage.user_description().map(str::to_string),
            stage,
            operation: None,
            task_id: None,
            endpoint: None,
            object: None,
            origin: None,
            contexts,
            diagnostic_message: Some(diagnostic_message),
            location: None,
        }
    }

    fn with_origin(mut self, origin: OriginError) -> Self {
        self.origin = Some(origin);
        self
    }

    fn with_object(mut self, object: Option<ErrorObject>) -> Self {
        self.object = object;
        self
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

fn contexts_before<T>(error: &anyhow::Error) -> Vec<String>
where
    T: std::error::Error + Send + Sync + 'static,
{
    error
        .chain()
        .take_while(|cause| cause.downcast_ref::<T>().is_none())
        .map(ToString::to_string)
        .collect()
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
        if let Some(phase) = &self.phase {
            write!(f, "\nPHASE: {phase}")?;
        }
        if let Some(detail) = &self.detail {
            write!(f, "\nDETAIL: {detail}")?;
        }
        if let Some(hint) = &self.hint {
            write!(f, "\nHINT: {hint}")?;
        }
        write!(f, "\nDIAGNOSTIC [{}]", self.code)?;
        if let Some(location) = &self.location {
            write!(f, "\nLOCATION: {location}")?;
        }
        write!(f, "\nSTAGE: {}", self.stage.diagnostic_name())?;
        if let Some(operation) = &self.operation {
            write!(f, "\nOPERATION: {operation}")?;
        }
        if let Some(endpoint) = self.endpoint {
            write!(f, "\nENDPOINT: {}", endpoint.user_description())?;
        }
        if let Some(origin) = &self.origin {
            write!(f, "\nORIGIN: {}", origin.system)?;
            if let Some(code) = &origin.code {
                write!(f, "/{code}")?;
            }
        }
        for context in &self.contexts {
            write!(f, "\nCONTEXT: {}", sanitize_user_text(context))?;
        }
        if let Some(message) = &self.diagnostic_message {
            write!(f, "\nCAUSE: {}", sanitize_user_text(message))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_structured_error_through_anyhow_context() {
        let error = anyhow::Error::new(
            DtError::new(ErrorCode::ObjectNotFound)
                .message("resume table does not exist")
                .stage(Stage::Resumer)
                .operation("load_checkpoint")
                .object(ErrorObject {
                    schema: Some("ape_dts".to_string()),
                    table: Some("resume_position".to_string()),
                    ..Default::default()
                })
                .origin(OriginError::new("postgres", Some("42P01"))),
        )
        .context("loading task state")
        .context("starting task");

        let report = ErrorReport::from_anyhow(&error);
        assert_eq!(report.code, ErrorCode::ObjectNotFound);
        assert_eq!(report.stage, Stage::Resumer);
        assert_eq!(
            report.phase.as_deref(),
            Some("restoring saved task progress")
        );
        assert_eq!(report.operation.as_deref(), Some("load_checkpoint"));
        assert_eq!(
            report.origin.as_ref().unwrap().code.as_deref(),
            Some("42P01")
        );
        assert_eq!(
            report.object.as_ref().unwrap().table.as_deref(),
            Some("resume_position")
        );
        assert_eq!(report.contexts, ["starting task", "loading task state"]);
        let rendered = report.to_string();
        assert!(rendered.contains("DIAGNOSTIC [MD001]"));
        assert!(rendered.contains("LOCATION:"));
        assert!(rendered.contains("STAGE: resumer"));
        assert!(rendered.contains("OPERATION: load_checkpoint"));
        assert!(rendered.contains("ORIGIN: postgres/42P01"));
        assert!(rendered.contains("CONTEXT: starting task"));
    }

    #[test]
    fn maps_unknown_errors() {
        let unknown = anyhow::anyhow!("plain error");
        assert_eq!(
            ErrorReport::from_anyhow(&unknown).code,
            ErrorCode::Unclassified
        );
    }

    #[test]
    fn renders_pg_style_report() {
        let error = anyhow::Error::new(
            DtError::new(ErrorCode::InvalidConfig)
                .stage(Stage::Bootstrap)
                .detail("config [runtime].worker_threads is invalid")
                .hint("use a positive integer"),
        );
        let rendered = ErrorReport::from_anyhow(&error).to_string();
        assert!(rendered.starts_with("ERROR [CF002]:"));
        assert!(rendered.contains("\nPHASE: loading task configuration"));
        assert!(rendered.contains("\nDETAIL:"));
        assert!(rendered.contains("\nHINT:"));
    }

    #[test]
    fn renders_user_known_task_endpoint_and_object() {
        let error = anyhow::Error::new(
            DtError::new(ErrorCode::ObjectNotFound)
                .stage(Stage::Sinker)
                .task_id("task-42")
                .endpoint(EndpointRole::Destination)
                .object(ErrorObject {
                    schema: Some("sales".to_string()),
                    table: Some("orders".to_string()),
                    ..Default::default()
                })
                .origin(OriginError::new("postgres", Some("42P01"))),
        );

        let rendered = ErrorReport::from_anyhow(&error).to_string();
        assert!(rendered.contains("\nTASK: task-42"));
        assert!(rendered.contains("\nAFFECTED: destination postgres sales.orders"));
        assert!(rendered.contains("\nPHASE: writing to the destination"));
        assert!(rendered.contains("\nORIGIN: postgres/42P01"));
    }

    #[test]
    fn text_includes_diagnostics_while_json_excludes_them() {
        let error = anyhow::anyhow!("password=secret, sql=INSERT, row_data=private")
            .context("internal worker context");
        let report = ErrorReport::from_anyhow(&error);
        let rendered = report.to_string();
        let json = serde_json::to_string(&report).unwrap();

        assert_eq!(report.message, ErrorCode::Unclassified.default_message());
        assert!(rendered.contains("DIAGNOSTIC [IN999]"));
        assert!(rendered.contains("CONTEXT: internal worker context"));
        assert!(rendered.contains("sql=INSERT"));
        assert!(rendered.contains("row_data=private"));
        assert!(!rendered.contains("password=secret"));
        assert!(!json.contains("internal worker context"));
        assert!(!json.contains("sql=INSERT"));
        assert!(!json.contains("row_data=private"));
        assert!(!json.contains("password=secret"));
        for diagnostic_field in [
            "stage",
            "operation",
            "origin",
            "contexts",
            "diagnostic_message",
        ] {
            assert!(!json.contains(diagnostic_field));
        }
        assert!(report.diagnostic_message.is_some());
        assert!(!report.contexts.is_empty());
    }

    #[test]
    fn serializes_schema_version_and_redacts_user_text() {
        let error = anyhow::Error::new(
            DtError::new(ErrorCode::InvalidConfig)
                .message("password=secret is invalid")
                .detail("endpoint=mysql://user:hunter2@localhost:3306/db")
                .hint("set token=abc123 before retrying"),
        );
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
    fn full_text_redacts_diagnostic_credentials() {
        let error = anyhow::anyhow!(
            "failed to connect to mysql://user:hunter2@localhost:3306/db with password=secret"
        )
        .context("token=abc123");
        let rendered = ErrorReport::from_anyhow(&error).to_string();

        for secret in ["hunter2", "password=secret", "token=abc123"] {
            assert!(!rendered.contains(secret), "{rendered}");
        }
        assert!(rendered.contains("[redacted]"), "{rendered}");
    }

    #[test]
    fn captures_the_structured_error_creation_location() {
        let error = anyhow::Error::new(DtError::new(ErrorCode::InvariantViolated));
        let report = ErrorReport::from_anyhow(&error);
        let location = report.location.unwrap();
        assert!(location.contains("error/report.rs"), "{location}");
    }

    #[tokio::test]
    async fn maps_worker_join_errors() {
        let handle = tokio::spawn(async { panic!("worker panic for test") });
        let error = anyhow::Error::new(handle.await.unwrap_err());
        let report = ErrorReport::from_anyhow(&error);
        assert_eq!(report.code, ErrorCode::WorkerFailed);
        assert_eq!(report.stage, Stage::Task);
        let rendered = report.to_string();
        assert!(rendered.contains("DIAGNOSTIC [RT001]"));
        assert!(rendered.contains("worker panic for test"));
    }

    #[test]
    fn maps_url_and_yaml_errors_to_invalid_config() {
        let url_error = anyhow::Error::new(url::Url::parse("://bad").unwrap_err());
        assert_eq!(
            ErrorReport::from_anyhow(&url_error).code,
            ErrorCode::InvalidConfig
        );

        let yaml_error =
            anyhow::Error::new(serde_yaml::from_str::<serde_yaml::Value>("key: [").unwrap_err());
        assert_eq!(
            ErrorReport::from_anyhow(&yaml_error).code,
            ErrorCode::InvalidConfig
        );
    }

    #[test]
    fn separates_anyhow_context_from_typed_root_cause() {
        let error = anyhow::Error::new(std::io::Error::other("root I/O failure"))
            .context("opening checkpoint file");
        let report = ErrorReport::from_anyhow(&error);

        assert_eq!(report.contexts, ["opening checkpoint file"]);
        assert_eq!(
            report.diagnostic_message.as_deref(),
            Some("root I/O failure")
        );

        let rendered = report.to_string();
        assert!(rendered.contains("CONTEXT: opening checkpoint file"));
        assert!(rendered.contains("CAUSE: root I/O failure"));
        assert!(!rendered.contains("CAUSE: opening checkpoint file"));
    }
}
