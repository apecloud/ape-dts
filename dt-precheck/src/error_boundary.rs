use dt_common::error::{DtError, DtErrorContextExt, ErrorCode};

pub(crate) fn precheck_failure(code: ErrorCode, detail: impl Into<String>) -> anyhow::Error {
    DtError::General(detail.into()).with_code(code)
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
    use dt_common::error::{classify_sqlx_error, DtErrorContextExt, ErrorCode, SqlxProvider};
    pub(crate) fn mysql_precheck_error(error: sqlx::Error) -> anyhow::Error {
        let context = classify_sqlx_error(&error, SqlxProvider::MySql).into_context();
        error
            .with_code(ErrorCode::StatementFailed)
            .with_context(context)
    }
}

pub(crate) mod postgres {
    use dt_common::error::{classify_sqlx_error, DtErrorContextExt, ErrorCode, SqlxProvider};
    pub(crate) fn postgres_precheck_error(error: sqlx::Error) -> anyhow::Error {
        let context = classify_sqlx_error(&error, SqlxProvider::Postgres).into_context();
        error
            .with_code(ErrorCode::StatementFailed)
            .with_context(context)
    }
}

pub(crate) mod mongodb {
    use dt_common::error::{classify_mongodb_error, DtError, DtErrorContextExt, ErrorCode};

    pub(crate) fn mongo_precheck_state_error() -> anyhow::Error {
        DtError::General("the MongoDB precheck client is not initialized".to_string())
            .with_code(ErrorCode::InvariantViolated)
    }
    pub(crate) fn mongo_precheck_provider_error(error: mongodb::error::Error) -> anyhow::Error {
        let context = classify_mongodb_error(&error).into_context();
        error
            .with_code(ErrorCode::StatementFailed)
            .with_context(context)
    }
}

pub(crate) mod redis {
    use std::error::Error as StdError;

    use dt_common::error::{DtErrorContext, ErrorCode, OriginError};
    pub(crate) fn redis_source<E>(
        error: E,
        code: ErrorCode,
        detail: impl Into<String>,
    ) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        DtErrorContext::new()
            .code(code)
            .origin(OriginError::new("redis", None::<String>))
            .attach(error)
            .context(detail.into())
    }
}
