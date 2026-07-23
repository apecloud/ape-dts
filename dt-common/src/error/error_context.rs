use std::{error::Error as StdError, fmt, sync::Arc};

use super::{
    provider::classify_raw_error, DtError, EndpointRole, ErrorCode, ErrorObject, OriginError, Stage,
};

pub(crate) const DT_ERROR_CONTEXT_MARKER: &str = "__APE_DTS_ERROR_CONTEXT__";

#[derive(Clone, Debug, Default)]
pub struct DtErrorContext {
    code: Option<ErrorCode>,
    message: Option<String>,
    hint: Option<String>,
    stage: Option<Stage>,
    task_id: Option<String>,
    endpoint: Option<EndpointRole>,
    object: Option<ErrorObject>,
    origin: Option<OriginError>,
    inner: Option<Arc<Self>>,
}

impl DtErrorContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn code(mut self, code: ErrorCode) -> Self {
        self.code = Some(code);
        self
    }

    pub fn message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    pub fn hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    pub fn stage(mut self, stage: Stage) -> Self {
        self.stage = Some(stage);
        self
    }

    pub fn task_id(mut self, task_id: impl Into<String>) -> Self {
        self.task_id = Some(task_id.into());
        self
    }

    pub fn endpoint(mut self, endpoint: EndpointRole) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn object(mut self, object: ErrorObject) -> Self {
        self.object = Some(object);
        self
    }

    pub fn origin(mut self, origin: OriginError) -> Self {
        self.origin = Some(origin);
        self
    }

    pub fn attach<E>(self, source: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        anyhow::Error::new(source).context(self)
    }

    pub fn error_code(&self) -> Option<ErrorCode> {
        self.code
            .or_else(|| self.inner.as_deref().and_then(Self::error_code))
    }

    pub fn message_text(&self) -> Option<&str> {
        self.message
            .as_deref()
            .or_else(|| self.inner.as_deref().and_then(Self::message_text))
    }

    pub fn hint_text(&self) -> Option<&str> {
        self.hint
            .as_deref()
            .or_else(|| self.inner.as_deref().and_then(Self::hint_text))
    }

    pub fn stage_value(&self) -> Option<Stage> {
        self.inner
            .as_deref()
            .and_then(Self::stage_value)
            .or(self.stage)
    }

    pub fn task_id_value(&self) -> Option<&str> {
        self.task_id
            .as_deref()
            .or_else(|| self.inner.as_deref().and_then(Self::task_id_value))
    }

    pub fn endpoint_role(&self) -> Option<EndpointRole> {
        self.inner
            .as_deref()
            .and_then(Self::endpoint_role)
            .or(self.endpoint)
    }

    pub fn error_object(&self) -> Option<ErrorObject> {
        let inner = self.inner.as_deref().and_then(Self::error_object);
        match (inner, self.object.as_ref()) {
            (Some(mut inner), Some(outer)) => {
                inner.fill_missing_from(outer);
                Some(inner)
            }
            (Some(inner), None) => Some(inner),
            (None, Some(outer)) => Some(outer.clone()),
            (None, None) => None,
        }
    }

    pub fn origin_error(&self) -> Option<&OriginError> {
        self.inner
            .as_deref()
            .and_then(Self::origin_error)
            .or(self.origin.as_ref())
    }

    fn with_inner(mut self, inner: Option<&Self>) -> Self {
        self.inner = inner.cloned().map(Arc::new);
        self
    }
}

impl fmt::Display for DtErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(DT_ERROR_CONTEXT_MARKER)
    }
}

pub trait ClassifyError {
    fn classify(&self) -> DtErrorContext;
}

pub trait AnyhowErrorExt {
    fn dt_context(&self) -> Option<&DtErrorContext>;
    fn error_code(&self) -> Option<ErrorCode>;
}

impl AnyhowErrorExt for anyhow::Error {
    fn dt_context(&self) -> Option<&DtErrorContext> {
        self.downcast_ref::<DtErrorContext>()
    }

    fn error_code(&self) -> Option<ErrorCode> {
        resolve_dt_context(self).error_code()
    }
}

pub(crate) fn resolve_dt_context(error: &anyhow::Error) -> DtErrorContext {
    let explicit = error.downcast_ref::<DtErrorContext>();
    let project = error.downcast_ref::<DtError>().map(ClassifyError::classify);
    let raw = classify_raw_error(error);
    let code = explicit
        .and_then(DtErrorContext::error_code)
        .or_else(|| project.as_ref().and_then(DtErrorContext::error_code))
        .or_else(|| raw.as_ref().and_then(DtErrorContext::error_code));
    let object = merge_error_objects(
        merge_error_objects(
            raw.as_ref().and_then(DtErrorContext::error_object),
            project.as_ref().and_then(DtErrorContext::error_object),
        ),
        explicit.and_then(DtErrorContext::error_object),
    );
    let explicit_origin = explicit.and_then(DtErrorContext::origin_error);
    let project_origin = project.as_ref().and_then(DtErrorContext::origin_error);
    let raw_origin = raw.as_ref().and_then(DtErrorContext::origin_error);
    let origin = [raw_origin, project_origin, explicit_origin]
        .into_iter()
        .flatten()
        .find(|origin| origin.code.is_some())
        .or(project_origin)
        .or(explicit_origin)
        .or(raw_origin)
        .cloned();

    DtErrorContext {
        code,
        message: explicit
            .and_then(DtErrorContext::message_text)
            .or_else(|| project.as_ref().and_then(DtErrorContext::message_text))
            .or_else(|| raw.as_ref().and_then(DtErrorContext::message_text))
            .map(str::to_string),
        hint: explicit
            .and_then(DtErrorContext::hint_text)
            .or_else(|| project.as_ref().and_then(DtErrorContext::hint_text))
            .or_else(|| raw.as_ref().and_then(DtErrorContext::hint_text))
            .map(str::to_string),
        stage: raw
            .as_ref()
            .and_then(DtErrorContext::stage_value)
            .or_else(|| project.as_ref().and_then(DtErrorContext::stage_value))
            .or_else(|| explicit.and_then(DtErrorContext::stage_value)),
        task_id: explicit
            .and_then(DtErrorContext::task_id_value)
            .or_else(|| project.as_ref().and_then(DtErrorContext::task_id_value))
            .or_else(|| raw.as_ref().and_then(DtErrorContext::task_id_value))
            .map(str::to_string),
        endpoint: raw
            .as_ref()
            .and_then(DtErrorContext::endpoint_role)
            .or_else(|| project.as_ref().and_then(DtErrorContext::endpoint_role))
            .or_else(|| explicit.and_then(DtErrorContext::endpoint_role)),
        object,
        origin,
        inner: None,
    }
}

fn merge_error_objects(
    inner: Option<ErrorObject>,
    outer: Option<ErrorObject>,
) -> Option<ErrorObject> {
    match (inner, outer) {
        (Some(mut inner), Some(outer)) => {
            inner.fill_missing_from(&outer);
            Some(inner)
        }
        (Some(inner), None) => Some(inner),
        (None, Some(outer)) => Some(outer),
        (None, None) => None,
    }
}

pub trait DtErrorContextExt: Sized {
    fn with_context(self, context: DtErrorContext) -> anyhow::Error;

    fn with_code(self, code: ErrorCode) -> anyhow::Error {
        self.with_context(DtErrorContext::new().code(code))
    }

    fn with_message(self, message: impl Into<String>) -> anyhow::Error {
        self.with_context(DtErrorContext::new().message(message))
    }

    fn with_hint(self, hint: impl Into<String>) -> anyhow::Error {
        self.with_context(DtErrorContext::new().hint(hint))
    }

    fn with_stage(self, stage: Stage) -> anyhow::Error {
        self.with_context(DtErrorContext::new().stage(stage))
    }

    fn with_task_id(self, task_id: impl Into<String>) -> anyhow::Error {
        self.with_context(DtErrorContext::new().task_id(task_id))
    }

    fn with_endpoint(self, endpoint: EndpointRole) -> anyhow::Error {
        self.with_context(DtErrorContext::new().endpoint(endpoint))
    }

    fn with_object(self, object: ErrorObject) -> anyhow::Error {
        self.with_context(DtErrorContext::new().object(object))
    }

    fn with_origin(self, origin: OriginError) -> anyhow::Error {
        self.with_context(DtErrorContext::new().origin(origin))
    }
}

impl DtErrorContextExt for anyhow::Error {
    fn with_context(self, context: DtErrorContext) -> anyhow::Error {
        let context = context.with_inner(self.dt_context());
        self.context(context)
    }
}

macro_rules! impl_dt_error_context_ext {
    ($($error:ty),+ $(,)?) => {
        $(
            impl DtErrorContextExt for $error {
                fn with_context(self, context: DtErrorContext) -> anyhow::Error {
                    context.attach(self)
                }
            }
        )+
    };
}

impl_dt_error_context_ext!(
    DtError,
    sqlx::Error,
    tokio_postgres::Error,
    mongodb::error::Error,
    redis::RedisError,
    reqwest::Error,
    rdkafka::error::KafkaError,
    kafka::Error,
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_fields_use_semantic_precedence() {
        let error = DtError::ConnectionFailed("inner application error".to_string())
            .with_message("inner message")
            .with_hint("inner hint")
            .with_stage(Stage::Sinker)
            .with_task_id("inner-task")
            .with_endpoint(EndpointRole::Destination)
            .with_object(ErrorObject {
                table: Some("inner_table".to_string()),
                ..Default::default()
            })
            .with_origin(OriginError::new("postgres", Some("42P01")))
            .with_code(ErrorCode::ConnectionFailed)
            .with_message("outer message")
            .with_hint("outer hint")
            .with_stage(Stage::Pipeline)
            .with_endpoint(EndpointRole::Source)
            .with_task_id("outer-task")
            .with_object(ErrorObject {
                schema: Some("outer_schema".to_string()),
                table: Some("outer_table".to_string()),
                ..Default::default()
            })
            .with_origin(OriginError::new("pipeline", None::<String>))
            .with_code(ErrorCode::StatementFailed);
        let context = error.dt_context().unwrap();

        assert_eq!(context.error_code(), Some(ErrorCode::StatementFailed));
        assert_eq!(context.message_text(), Some("outer message"));
        assert_eq!(context.hint_text(), Some("outer hint"));
        assert_eq!(context.stage_value(), Some(Stage::Sinker));
        assert_eq!(context.endpoint_role(), Some(EndpointRole::Destination));
        assert_eq!(context.task_id_value(), Some("outer-task"));
        let object = context.error_object().unwrap();
        assert_eq!(object.schema.as_deref(), Some("outer_schema"));
        assert_eq!(object.table.as_deref(), Some("inner_table"));
        assert_eq!(
            context.origin_error(),
            Some(&OriginError::new("postgres", Some("42P01")))
        );
    }
}
