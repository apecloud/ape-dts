use dt_common::error::{
    DtError, DtErrorContext, DtErrorContextExt, EndpointRole, ErrorCode, Stage,
};

fn endpoint(is_source: bool) -> EndpointRole {
    if is_source {
        EndpointRole::Source
    } else {
        EndpointRole::Destination
    }
}

fn scoped(context: DtErrorContext, endpoint: EndpointRole) -> DtErrorContext {
    DtErrorContext::new()
        .stage(Stage::Precheck)
        .endpoint(endpoint)
        .inherit(context)
}

pub(crate) fn precheck_failure(
    code: ErrorCode,
    detail: impl Into<String>,
    is_source: bool,
) -> anyhow::Error {
    DtError::Unexpected(detail.into())
        .with_code(code)
        .with_stage(Stage::Precheck)
        .with_endpoint(endpoint(is_source))
}

pub(crate) mod report {
    use dt_common::error::{ErrorCode, ErrorReport};

    pub(crate) fn classify(
        error: Option<&anyhow::Error>,
        fallback: ErrorCode,
    ) -> (Option<ErrorCode>, String) {
        let Some(error) = error else {
            return (None, String::new());
        };
        let report = ErrorReport::from_anyhow(error);
        let code = if report.code == ErrorCode::Unclassified {
            fallback
        } else {
            report.code
        };
        let message = if report.code == ErrorCode::Unclassified {
            code.default_message().to_string()
        } else {
            report.message
        };
        (Some(code), message)
    }
}

pub(crate) mod mysql {
    use dt_common::error::{
        classify_sqlx_error, DtErrorContextExt, EndpointRole, ErrorCode, SqlxProvider, Stage,
    };
    pub(crate) fn mysql_precheck_error(
        error: sqlx::Error,
        endpoint: EndpointRole,
    ) -> anyhow::Error {
        let context = classify_sqlx_error(&error, SqlxProvider::MySql).into_context();
        error
            .with_code(ErrorCode::StatementFailed)
            .with_context(context)
            .with_stage(Stage::Precheck)
            .with_endpoint(endpoint)
    }
}

pub(crate) mod postgres {
    use dt_common::error::{
        classify_sqlx_error, DtErrorContextExt, EndpointRole, ErrorCode, SqlxProvider, Stage,
    };
    pub(crate) fn postgres_precheck_error(
        error: sqlx::Error,
        endpoint: EndpointRole,
    ) -> anyhow::Error {
        let context = classify_sqlx_error(&error, SqlxProvider::Postgres).into_context();
        error
            .with_code(ErrorCode::StatementFailed)
            .with_context(context)
            .with_stage(Stage::Precheck)
            .with_endpoint(endpoint)
    }
}

pub(crate) mod mongodb {
    use dt_common::error::{classify_mongodb_error, DtError, DtErrorContextExt, ErrorCode, Stage};

    use super::endpoint;
    pub(crate) fn mongo_precheck_state_error(is_source: bool) -> anyhow::Error {
        DtError::Unexpected("the MongoDB precheck client is not initialized".to_string())
            .with_code(ErrorCode::InvariantViolated)
            .with_stage(Stage::Precheck)
            .with_endpoint(endpoint(is_source))
    }
    pub(crate) fn mongo_precheck_provider_error(
        error: mongodb::error::Error,
        is_source: bool,
    ) -> anyhow::Error {
        let context = classify_mongodb_error(&error).into_context();
        error
            .with_code(ErrorCode::StatementFailed)
            .with_context(context)
            .with_stage(Stage::Precheck)
            .with_endpoint(endpoint(is_source))
    }
}

pub(crate) mod redis {
    use std::error::Error as StdError;

    use dt_common::error::{DtErrorContext, EndpointRole, ErrorCode, OriginError};
    pub(crate) fn redis_source<E>(
        error: E,
        code: ErrorCode,
        detail: impl Into<String>,
        endpoint: EndpointRole,
    ) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        super::scoped(
            DtErrorContext::new()
                .code(code)
                .detail(detail)
                .origin(OriginError::new("redis", None::<String>)),
            endpoint,
        )
        .attach(error)
    }
}
