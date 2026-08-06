use std::fmt;

use super::{DtError, EndpointRole, ErrorCode, ErrorObject, Stage};

pub(crate) const DT_ERROR_CONTEXT_MARKER: &str = "__APE_DTS_ERROR_CONTEXT__";

#[derive(Clone, Debug, Default)]
pub struct DtErrorContext {
    pub(crate) code: Option<ErrorCode>,
    pub(crate) message: Option<String>,
    pub(crate) detail: Option<String>,
    pub(crate) hint: Option<String>,
    pub(crate) stage: Option<Stage>,
    pub(crate) task_id: Option<String>,
    pub(crate) endpoint: Option<EndpointRole>,
    pub(crate) object: Option<ErrorObject>,
}

impl DtErrorContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_code(mut self, code: ErrorCode) -> Self {
        self.code = Some(code);
        self
    }

    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    pub fn with_stage(mut self, stage: Stage) -> Self {
        self.stage = Some(stage);
        self
    }

    pub fn with_task_id(mut self, task_id: impl Into<String>) -> Self {
        self.task_id = Some(task_id.into());
        self
    }

    pub fn with_endpoint(mut self, endpoint: EndpointRole) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn with_object(mut self, object: ErrorObject) -> Self {
        self.object = Some(object);
        self
    }

    pub fn error_code(&self) -> Option<ErrorCode> {
        self.code
    }
}

#[derive(Debug)]
pub(crate) struct DtErrorContexts {
    contexts: Vec<DtErrorContext>,
}

impl DtErrorContexts {
    fn new(context: DtErrorContext) -> Self {
        Self {
            contexts: vec![context],
        }
    }

    fn push(&mut self, context: DtErrorContext) {
        self.contexts.push(context);
    }

    pub(crate) fn iter_outer_to_inner(&self) -> impl Iterator<Item = &DtErrorContext> {
        self.contexts.iter().rev()
    }
}

impl fmt::Display for DtErrorContexts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(DT_ERROR_CONTEXT_MARKER)
    }
}

pub trait DtErrorContextExt: Sized {
    fn dt_context(self, context: DtErrorContext) -> anyhow::Error;

    fn code(self, code: ErrorCode) -> anyhow::Error {
        self.dt_context(DtErrorContext::new().with_code(code))
    }

    fn message(self, message: impl Into<String>) -> anyhow::Error {
        self.dt_context(DtErrorContext::new().with_message(message))
    }

    fn hint(self, hint: impl Into<String>) -> anyhow::Error {
        self.dt_context(DtErrorContext::new().with_hint(hint))
    }

    fn stage(self, stage: Stage) -> anyhow::Error {
        self.dt_context(DtErrorContext::new().with_stage(stage))
    }

    fn task_id(self, task_id: impl Into<String>) -> anyhow::Error {
        self.dt_context(DtErrorContext::new().with_task_id(task_id))
    }

    fn endpoint(self, endpoint: EndpointRole) -> anyhow::Error {
        self.dt_context(DtErrorContext::new().with_endpoint(endpoint))
    }

    fn object(self, object: ErrorObject) -> anyhow::Error {
        self.dt_context(DtErrorContext::new().with_object(object))
    }
}

impl<E> DtErrorContextExt for E
where
    E: Into<anyhow::Error>,
{
    fn dt_context(self, context: DtErrorContext) -> anyhow::Error {
        let mut error = self.into();
        if let Some(contexts) = error.downcast_mut::<DtErrorContexts>() {
            contexts.push(context);
            error
        } else {
            error.context(DtErrorContexts::new(context))
        }
    }
}

pub trait DtOptionExt<T>: Sized {
    fn or_dt_error(self, error: DtError) -> anyhow::Result<T>;
}

impl<T> DtOptionExt<T> for Option<T> {
    fn or_dt_error(self, error: DtError) -> anyhow::Result<T> {
        match self {
            Some(value) => Ok(value),
            None => Err(error.into()),
        }
    }
}

pub trait DtResultExt<T>: Sized {
    fn dt_error(self, error: DtError) -> anyhow::Result<T>;

    fn dt_context<F>(self, make_context: F) -> anyhow::Result<T>
    where
        F: FnOnce() -> DtErrorContext;

    fn code(self, code: ErrorCode) -> anyhow::Result<T> {
        self.dt_context(|| DtErrorContext::new().with_code(code))
    }

    fn message(self, message: impl Into<String>) -> anyhow::Result<T> {
        self.dt_context(|| DtErrorContext::new().with_message(message))
    }

    fn hint(self, hint: impl Into<String>) -> anyhow::Result<T> {
        self.dt_context(|| DtErrorContext::new().with_hint(hint))
    }

    fn stage(self, stage: Stage) -> anyhow::Result<T> {
        self.dt_context(|| DtErrorContext::new().with_stage(stage))
    }

    fn task_id(self, task_id: impl Into<String>) -> anyhow::Result<T> {
        self.dt_context(|| DtErrorContext::new().with_task_id(task_id))
    }

    fn endpoint(self, endpoint: EndpointRole) -> anyhow::Result<T> {
        self.dt_context(|| DtErrorContext::new().with_endpoint(endpoint))
    }

    fn object(self, object: ErrorObject) -> anyhow::Result<T> {
        self.dt_context(|| DtErrorContext::new().with_object(object))
    }
}

impl<T, E> DtResultExt<T> for Result<T, E>
where
    E: Into<anyhow::Error>,
{
    fn dt_error(self, error: DtError) -> anyhow::Result<T> {
        self.map_err(|e| e.into().context(error))
    }

    fn dt_context<F>(self, make_context: F) -> anyhow::Result<T>
    where
        F: FnOnce() -> DtErrorContext,
    {
        self.map_err(|error| {
            let error: anyhow::Error = error.into();
            error.dt_context(make_context())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorReport;
    use anyhow::Context;
    use std::io::{Error, ErrorKind};

    #[test]
    fn option_or_dt_error_returns_value() {
        let result = Some(42).or_dt_error(DtError::ObjectNotFound("missing value".to_string()));

        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn option_or_dt_error_creates_classified_error() {
        let result = None::<()>
            .or_dt_error(DtError::ObjectNotFound("missing value".to_string()))
            .stage(Stage::Extractor)
            .endpoint(EndpointRole::Source);

        let error = result.unwrap_err();
        assert!(error.downcast_ref::<DtError>().is_some());
        let report = ErrorReport::from_anyhow(&error);
        assert_eq!(report.code, ErrorCode::ObjectNotFound);
        assert_eq!(report.stage, Stage::Extractor);
        assert_eq!(report.endpoint, Some(EndpointRole::Source));
    }

    #[test]
    fn result_dt_error_preserves_source_and_metadata() {
        let result: Result<(), Error> = Err(Error::new(ErrorKind::Other, "test error"));
        let anyhow_result = result
            .dt_error(DtError::Unclassified("e1".into()))
            .dt_error(DtError::Unclassified("e2".into()))
            .stage(Stage::Bootstrap)
            .endpoint(EndpointRole::Source);
        let err = anyhow_result.unwrap_err();
        assert!(err.downcast_ref::<DtError>().is_some());
        assert!(err.downcast_ref::<Error>().is_some());
        let report = ErrorReport::from_anyhow(&err);
        assert_eq!(report.stage, Stage::Bootstrap);
        assert_eq!(report.endpoint, Some(EndpointRole::Source));
    }

    #[test]
    fn anyhow_result_dt_error_preserves_existing_chain() {
        let result: Result<(), anyhow::Error> = Err(anyhow::Error::from(Error::new(
            ErrorKind::Other,
            "test error",
        )));
        let anyhow_result = result
            .dt_error(DtError::Unclassified("e1".into()))
            .dt_error(DtError::Unclassified("e2".into()))
            .context(DtError::Unclassified("e3".into()))
            .stage(Stage::Bootstrap)
            .endpoint(EndpointRole::Source);
        let err = anyhow_result.unwrap_err();
        assert!(err.downcast_ref::<DtError>().is_some());
        assert!(err.downcast_ref::<Error>().is_some());
        assert!(err.downcast_ref::<DtErrorContexts>().is_some());
        let chain: Vec<_> = err.chain().map(ToString::to_string).collect();
        assert_eq!(
            chain,
            [DT_ERROR_CONTEXT_MARKER, "e3", "e2", "e1", "test error"]
        );
    }
}
