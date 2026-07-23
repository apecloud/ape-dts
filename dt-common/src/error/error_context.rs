use std::{error::Error as StdError, fmt};

use super::{DtError, EndpointRole, ErrorCode, ErrorObject, OriginError, Stage};

pub(crate) const DT_ERROR_CONTEXT_MARKER: &str = "__APE_DTS_ERROR_CONTEXT__";

#[derive(Clone, Debug, Default)]
pub struct DtErrorContext {
    pub(crate) code: Option<ErrorCode>,
    pub(crate) message: Option<String>,
    pub(crate) hint: Option<String>,
    pub(crate) stage: Option<Stage>,
    pub(crate) task_id: Option<String>,
    pub(crate) endpoint: Option<EndpointRole>,
    pub(crate) object: Option<ErrorObject>,
    pub(crate) origin: Option<OriginError>,
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

    pub fn with_origin(mut self, origin: OriginError) -> Self {
        self.origin = Some(origin);
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

pub trait ClassifyError {
    fn classify(&self) -> DtErrorContext;
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

    fn origin(self, origin: OriginError) -> anyhow::Error {
        self.dt_context(DtErrorContext::new().with_origin(origin))
    }
}

impl DtErrorContextExt for anyhow::Error {
    fn dt_context(mut self, context: DtErrorContext) -> anyhow::Error {
        if let Some(contexts) = self.downcast_mut::<DtErrorContexts>() {
            contexts.push(context);
            self
        } else {
            self.context(DtErrorContexts::new(context))
        }
    }
}

fn attach_dt_context<E>(error: E, context: DtErrorContext) -> anyhow::Error
where
    E: StdError + Send + Sync + 'static,
{
    anyhow::Error::new(error).context(DtErrorContexts::new(context))
}

macro_rules! impl_dt_error_context_ext {
    ($($error:ty),+ $(,)?) => {
        $(
            impl DtErrorContextExt for $error {
                #[inline(always)]
                fn dt_context(self, context: DtErrorContext) -> anyhow::Error {
                    attach_dt_context(self, context)
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
    std::io::Error,
    tokio::task::JoinError,
    openssl::error::ErrorStack,
    mysql_binlog_connector_rust::binlog_error::BinlogError,
);

pub trait DtResultExt<T>: Sized {
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

    fn origin(self, origin: OriginError) -> anyhow::Result<T> {
        self.dt_context(|| DtErrorContext::new().with_origin(origin))
    }
}

impl<T, E> DtResultExt<T> for Result<T, E>
where
    E: Into<anyhow::Error>,
{
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
