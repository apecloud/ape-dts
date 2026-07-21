use std::{error::Error as StdError, fmt, sync::Arc};

use super::{DtError, EndpointRole, ErrorCode, ErrorObject, OriginError, Stage};

pub(crate) const DT_ERROR_CONTEXT_MARKER: &str = "__APE_DTS_ERROR_CONTEXT__";

#[derive(Clone, Debug, Default)]
pub struct DtErrorContext {
    code: Option<ErrorCode>,
    message: Option<String>,
    detail: Option<String>,
    hint: Option<String>,
    stage: Option<Stage>,
    task_id: Option<String>,
    endpoint: Option<EndpointRole>,
    object: Option<ErrorObject>,
    origin: Option<OriginError>,
    parent: Option<Arc<Self>>,
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

    pub fn detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
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

    pub fn inherit(mut self, parent: Self) -> Self {
        self.parent = Some(Arc::new(parent));
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
            .or_else(|| self.parent.as_deref().and_then(Self::error_code))
    }

    pub fn message_text(&self) -> Option<&str> {
        self.message
            .as_deref()
            .or_else(|| self.parent.as_deref().and_then(Self::message_text))
    }

    pub fn detail_text(&self) -> Option<&str> {
        self.detail
            .as_deref()
            .or_else(|| self.parent.as_deref().and_then(Self::detail_text))
    }

    pub fn hint_text(&self) -> Option<&str> {
        self.hint
            .as_deref()
            .or_else(|| self.parent.as_deref().and_then(Self::hint_text))
    }

    pub fn stage_value(&self) -> Option<Stage> {
        self.stage
            .or_else(|| self.parent.as_deref().and_then(Self::stage_value))
    }

    pub fn task_id_value(&self) -> Option<&str> {
        self.task_id
            .as_deref()
            .or_else(|| self.parent.as_deref().and_then(Self::task_id_value))
    }

    pub fn endpoint_role(&self) -> Option<EndpointRole> {
        self.endpoint
            .or_else(|| self.parent.as_deref().and_then(Self::endpoint_role))
    }

    pub fn error_object(&self) -> Option<&ErrorObject> {
        self.object
            .as_ref()
            .or_else(|| self.parent.as_deref().and_then(Self::error_object))
    }

    pub fn origin_error(&self) -> Option<&OriginError> {
        self.origin
            .as_ref()
            .or_else(|| self.parent.as_deref().and_then(Self::origin_error))
    }

    fn with_parent(mut self, parent: Option<&Self>) -> Self {
        self.parent = parent.cloned().map(Arc::new);
        self
    }
}

impl fmt::Display for DtErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(DT_ERROR_CONTEXT_MARKER)
    }
}

pub trait AnyhowErrorExt {
    fn dt_context(&self) -> Option<&DtErrorContext>;
}

impl AnyhowErrorExt for anyhow::Error {
    fn dt_context(&self) -> Option<&DtErrorContext> {
        self.downcast_ref::<DtErrorContext>()
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

    fn with_detail(self, detail: impl Into<String>) -> anyhow::Error {
        self.with_context(DtErrorContext::new().detail(detail))
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
        let context = context.with_parent(self.dt_context());
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
    use std::io;

    use super::*;

    #[test]
    fn project_error_has_a_typed_root_cause() {
        let error = DtError::Unexpected("queue state is invalid".to_string())
            .with_code(ErrorCode::InvariantViolated);

        assert_eq!(
            error.dt_context().and_then(DtErrorContext::error_code),
            Some(ErrorCode::InvariantViolated)
        );
        assert!(error.downcast_ref::<DtError>().is_some());
    }

    #[test]
    fn typed_context_preserves_provider_error() {
        let error = DtErrorContext::new()
            .code(ErrorCode::IoFailed)
            .attach(io::Error::other("disk failure"))
            .context("writing checkpoint");

        assert_eq!(
            error.dt_context().and_then(DtErrorContext::error_code),
            Some(ErrorCode::IoFailed)
        );
        assert_eq!(
            error.downcast_ref::<io::Error>().map(io::Error::kind),
            Some(io::ErrorKind::Other)
        );
    }

    #[test]
    fn outer_context_fields_override_inner_fields() {
        let error = DtError::Unexpected("inner application error".to_string())
            .with_code(ErrorCode::ConnectionFailed)
            .with_message("inner message")
            .with_detail("inner detail")
            .with_hint("inner hint")
            .with_stage(Stage::Extractor)
            .with_task_id("inner-task")
            .with_endpoint(EndpointRole::Source)
            .with_object(ErrorObject {
                table: Some("inner_table".to_string()),
                ..Default::default()
            })
            .with_origin(OriginError::new("mysql", Some("1146")))
            .with_code(ErrorCode::StatementFailed)
            .with_message("outer message")
            .with_detail("outer detail")
            .with_hint("outer hint")
            .with_stage(Stage::Sinker)
            .with_endpoint(EndpointRole::Destination)
            .with_task_id("outer-task")
            .with_object(ErrorObject {
                table: Some("outer_table".to_string()),
                ..Default::default()
            })
            .with_origin(OriginError::new("postgres", Some("42P01")));
        let context = error.dt_context().unwrap();

        assert_eq!(context.error_code(), Some(ErrorCode::StatementFailed));
        assert_eq!(context.message_text(), Some("outer message"));
        assert_eq!(context.detail_text(), Some("outer detail"));
        assert_eq!(context.hint_text(), Some("outer hint"));
        assert_eq!(context.stage_value(), Some(Stage::Sinker));
        assert_eq!(context.endpoint_role(), Some(EndpointRole::Destination));
        assert_eq!(context.task_id_value(), Some("outer-task"));
        assert_eq!(
            context
                .error_object()
                .and_then(|object| object.table.as_deref()),
            Some("outer_table")
        );
        assert_eq!(
            context.origin_error(),
            Some(&OriginError::new("postgres", Some("42P01")))
        );
    }

    #[test]
    fn project_error_variant_can_be_the_root_cause() {
        let error = DtError::ConfigError("worker_threads is invalid".to_string())
            .with_code(ErrorCode::InvalidConfig);

        assert!(matches!(
            error.downcast_ref::<DtError>(),
            Some(DtError::ConfigError(message)) if message == "worker_threads is invalid"
        ));
    }

    #[test]
    fn known_provider_error_supports_context_extensions() {
        let error = sqlx::Error::PoolTimedOut
            .with_code(ErrorCode::ConnectionTimeout)
            .with_stage(Stage::Sinker);

        assert!(matches!(
            error.downcast_ref::<sqlx::Error>(),
            Some(sqlx::Error::PoolTimedOut)
        ));
        assert_eq!(
            error.dt_context().and_then(DtErrorContext::error_code),
            Some(ErrorCode::ConnectionTimeout)
        );
        assert_eq!(
            error.dt_context().and_then(DtErrorContext::stage_value),
            Some(Stage::Sinker)
        );
    }
}
