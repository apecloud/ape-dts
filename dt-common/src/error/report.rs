use std::{error::Error as StdError, fmt};

use serde::Serialize;

use super::{
    classify_mongodb_error, classify_redis_error, classify_sqlx_error, DtError, EndpointRole,
    Error, ErrorCode, ErrorObject, OriginError, SqlxProvider, Stage,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ErrorReport {
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
    #[serde(skip_serializing_if = "Option::is_none")]
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

pub struct DiagnosticReport<'a> {
    report: &'a ErrorReport,
}

impl ErrorReport {
    pub fn from_anyhow(error: &anyhow::Error) -> Self {
        if let Some(dt_error) = error.downcast_ref::<DtError>() {
            return Self::from_dt_error(dt_error, contexts_before::<DtError>(error));
        }

        if let Some(legacy) = error.downcast_ref::<Error>() {
            return Self::from_legacy_error(legacy, contexts_before::<Error>(error));
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
                error.to_string(),
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
                error.to_string(),
                contexts_before::<redis::RedisError>(error),
            )
            .with_origin(classification.origin);
        }

        if let Some(mongodb_error) = error.downcast_ref::<mongodb::error::Error>() {
            let classification = classify_mongodb_error(mongodb_error, ErrorCode::StatementFailed);
            return Self::new(
                classification.code,
                Stage::Unknown,
                error.to_string(),
                contexts_before::<mongodb::error::Error>(error),
            )
            .with_origin(classification.origin);
        }

        if error.downcast_ref::<tokio::task::JoinError>().is_some() {
            return Self::new(
                ErrorCode::WorkerFailed,
                Stage::Task,
                error.to_string(),
                contexts_before::<tokio::task::JoinError>(error),
            );
        }

        if error.downcast_ref::<url::ParseError>().is_some()
            || error.downcast_ref::<serde_yaml::Error>().is_some()
        {
            return Self::new(
                ErrorCode::InvalidConfig,
                Stage::Bootstrap,
                error.to_string(),
                error.chain().skip(1).map(ToString::to_string).collect(),
            );
        }

        if error.downcast_ref::<std::io::Error>().is_some() {
            return Self::new(
                ErrorCode::IoFailed,
                Stage::Unknown,
                error.to_string(),
                contexts_before::<std::io::Error>(error),
            );
        }

        Self::new(
            ErrorCode::Unclassified,
            Stage::Unknown,
            error.to_string(),
            error.chain().skip(1).map(ToString::to_string).collect(),
        )
    }

    fn from_dt_error(error: &DtError, contexts: Vec<String>) -> Self {
        let stage = error.stage.unwrap_or(Stage::Unknown);
        Self {
            code: error.code,
            message: error.message.clone(),
            detail: error.detail.clone(),
            hint: Some(
                error
                    .hint
                    .clone()
                    .unwrap_or_else(|| error.code.default_hint().to_string()),
            ),
            phase: stage.user_description().map(str::to_string),
            stage,
            operation: error.operation.map(str::to_string),
            task_id: error.task_id.clone(),
            endpoint: error.endpoint,
            object: error.object.clone(),
            origin: error.origin.clone(),
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

    fn from_legacy_error(error: &Error, contexts: Vec<String>) -> Self {
        if let Error::SqlxError(sqlx_error) = error {
            let classification = classify_sqlx_error(
                sqlx_error,
                SqlxProvider::Unknown,
                ErrorCode::StatementFailed,
            );
            return Self::new(
                classification.code,
                error.stage(),
                error.to_string(),
                contexts,
            )
            .with_origin(classification.origin)
            .with_object(classification.object);
        }

        if let Error::MongodbError(mongodb_error) = error {
            let classification = classify_mongodb_error(mongodb_error, ErrorCode::StatementFailed);
            return Self::new(
                classification.code,
                error.stage(),
                error.to_string(),
                contexts,
            )
            .with_origin(classification.origin);
        }

        Self::new(error.code(), error.stage(), error.to_string(), contexts)
    }

    fn new(
        code: ErrorCode,
        stage: Stage,
        diagnostic_message: String,
        contexts: Vec<String>,
    ) -> Self {
        Self {
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

    pub fn diagnostic(&self) -> DiagnosticReport<'_> {
        DiagnosticReport { report: self }
    }
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
        Ok(())
    }
}

impl fmt::Display for DiagnosticReport<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let report = self.report;
        write!(f, "DIAGNOSTIC [{}]", report.code)?;
        if let Some(location) = &report.location {
            write!(f, "\nLOCATION: {location}")?;
        }
        write!(f, "\nSTAGE: {}", report.stage.diagnostic_name())?;
        if let Some(operation) = &report.operation {
            write!(f, "\nOPERATION: {operation}")?;
        }
        if let Some(endpoint) = report.endpoint {
            write!(f, "\nENDPOINT: {}", endpoint.user_description())?;
        }
        if let Some(origin) = &report.origin {
            write!(f, "\nORIGIN: {}", origin.system)?;
            if let Some(code) = &origin.code {
                write!(f, "/{code}")?;
            }
        }
        for context in &report.contexts {
            write!(f, "\nCONTEXT: {context}")?;
        }
        if let Some(message) = &report.diagnostic_message {
            write!(f, "\nCAUSE: {message}")?;
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
        let diagnostic = report.diagnostic().to_string();
        assert!(diagnostic.contains("DIAGNOSTIC [MD001]"));
        assert!(diagnostic.contains("LOCATION:"));
        assert!(diagnostic.contains("STAGE: resumer"));
        assert!(diagnostic.contains("OPERATION: load_checkpoint"));
        assert!(diagnostic.contains("ORIGIN: postgres/42P01"));
        assert!(diagnostic.contains("CONTEXT: starting task"));
    }

    #[test]
    fn maps_legacy_and_unknown_errors() {
        let legacy = anyhow::Error::new(Error::MetadataError("table metadata missing".into()));
        assert_eq!(
            ErrorReport::from_anyhow(&legacy).code,
            ErrorCode::MetadataFailed
        );

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
        assert!(!rendered.contains("42P01"));
    }

    #[test]
    fn default_views_hide_diagnostic_context_and_unknown_messages() {
        let error = anyhow::anyhow!("password=secret, sql=INSERT, row_data=private")
            .context("internal worker context");
        let report = ErrorReport::from_anyhow(&error);
        let rendered = report.to_string();
        let json = serde_json::to_string(&report).unwrap();

        assert_eq!(report.message, ErrorCode::Unclassified.default_message());
        for secret in [
            "password=secret",
            "sql=INSERT",
            "row_data=private",
            "internal worker",
        ] {
            assert!(!rendered.contains(secret));
            assert!(!json.contains(secret));
        }
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
        assert!(!report.to_string().contains("worker panic for test"));
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
}
