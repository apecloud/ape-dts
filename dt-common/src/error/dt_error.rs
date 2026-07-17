use std::{error::Error as StdError, fmt, panic::Location};

use super::{EndpointRole, ErrorCode, ErrorObject, OriginError, Stage};

pub type BoxError = Box<dyn StdError + Send + Sync + 'static>;

#[derive(Debug)]
pub struct DtError {
    code: ErrorCode,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    stage: Option<Stage>,
    operation: Option<&'static str>,
    pub task_id: Option<String>,
    pub endpoint: Option<EndpointRole>,
    pub object: Option<ErrorObject>,
    origin: Option<OriginError>,
    pub location: &'static Location<'static>,
    source: Option<BoxError>,
}

impl DtError {
    #[track_caller]
    pub fn new(code: ErrorCode) -> Self {
        Self {
            code,
            message: code.default_message().to_string(),
            detail: None,
            hint: None,
            stage: None,
            operation: None,
            task_id: None,
            endpoint: None,
            object: None,
            origin: None,
            location: Location::caller(),
            source: None,
        }
    }

    pub fn message(mut self, message: impl Into<String>) -> Self {
        self.message = message.into();
        self
    }

    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    pub const fn root_stage(&self) -> Option<Stage> {
        self.stage
    }

    pub const fn root_operation(&self) -> Option<&'static str> {
        self.operation
    }

    pub fn origin_error(&self) -> Option<&OriginError> {
        self.origin.as_ref()
    }

    pub fn detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    pub fn hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    pub fn stage(mut self, stage: Stage) -> Self {
        if self.stage.is_none() {
            self.stage = Some(stage);
        }
        self
    }

    pub fn operation(mut self, operation: &'static str) -> Self {
        if self.operation.is_none() {
            self.operation = Some(operation);
        }
        self
    }

    pub fn task_id(mut self, task_id: impl Into<String>) -> Self {
        if self.task_id.is_none() {
            self.task_id = Some(task_id.into());
        }
        self
    }

    pub fn endpoint(mut self, endpoint: EndpointRole) -> Self {
        if self.endpoint.is_none() {
            self.endpoint = Some(endpoint);
        }
        self
    }

    pub fn object(mut self, object: ErrorObject) -> Self {
        if self.object.is_none() {
            self.object = Some(object);
        }
        self
    }

    pub fn origin(mut self, origin: OriginError) -> Self {
        if self.origin.is_none() {
            self.origin = Some(origin);
        }
        self
    }

    pub fn source(mut self, source: impl Into<BoxError>) -> Self {
        if self.source.is_none() {
            self.source = Some(source.into());
        }
        self
    }
}

impl fmt::Display for DtError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

impl StdError for DtError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source.as_deref().map(|source| source as _)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_is_set_only_once() {
        let error = DtError::new(ErrorCode::StatementFailed)
            .stage(Stage::Sinker)
            .stage(Stage::Task);
        assert_eq!(error.root_stage(), Some(Stage::Sinker));
    }

    #[test]
    fn root_context_is_set_only_once() {
        let error = DtError::new(ErrorCode::StatementFailed)
            .stage(Stage::Extractor)
            .operation("read_source")
            .stage(Stage::Task)
            .operation("run_task")
            .origin(OriginError::new("mysql", Some("1146")))
            .origin(OriginError::new("postgres", Some("42P01")));

        assert_eq!(error.root_stage(), Some(Stage::Extractor));
        assert_eq!(error.root_operation(), Some("read_source"));
        assert_eq!(
            error.origin_error(),
            Some(&OriginError::new("mysql", Some("1146")))
        );
    }
}
