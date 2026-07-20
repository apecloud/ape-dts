use dt_common::error::{DtError, EndpointRole, ErrorCode, Stage};

fn endpoint(is_source: bool) -> EndpointRole {
    if is_source {
        EndpointRole::Source
    } else {
        EndpointRole::Destination
    }
}

#[track_caller]
pub(crate) fn failure(
    code: ErrorCode,
    detail: impl Into<String>,
    is_source: bool,
    operation: &'static str,
) -> anyhow::Error {
    DtError::new(code)
        .detail(detail)
        .stage(Stage::Precheck)
        .operation(operation)
        .endpoint(endpoint(is_source))
        .into()
}

pub(crate) mod mysql {
    use dt_common::error::{
        dt_error_from_sqlx, DtError, EndpointRole, ErrorCode, SqlxProvider, Stage,
    };

    #[track_caller]
    pub(crate) fn provider(
        error: sqlx::Error,
        endpoint: EndpointRole,
        operation: &'static str,
    ) -> DtError {
        dt_error_from_sqlx(error, SqlxProvider::MySql, ErrorCode::StatementFailed)
            .stage(Stage::Precheck)
            .operation(operation)
            .endpoint(endpoint)
    }
}

pub(crate) mod postgres {
    use dt_common::error::{
        dt_error_from_sqlx, DtError, EndpointRole, ErrorCode, SqlxProvider, Stage,
    };

    #[track_caller]
    pub(crate) fn provider(
        error: sqlx::Error,
        endpoint: EndpointRole,
        operation: &'static str,
    ) -> DtError {
        dt_error_from_sqlx(error, SqlxProvider::Postgres, ErrorCode::StatementFailed)
            .stage(Stage::Precheck)
            .operation(operation)
            .endpoint(endpoint)
    }
}

pub(crate) mod mongodb {
    use dt_common::error::{dt_error_from_mongodb, DtError, ErrorCode, Stage};

    use super::endpoint;

    #[track_caller]
    pub(crate) fn state(is_source: bool, operation: &'static str) -> DtError {
        DtError::new(ErrorCode::InvariantViolated)
            .detail("the MongoDB precheck client is not initialized")
            .stage(Stage::Precheck)
            .operation(operation)
            .endpoint(endpoint(is_source))
    }

    #[track_caller]
    pub(crate) fn provider(
        error: mongodb::error::Error,
        is_source: bool,
        operation: &'static str,
    ) -> DtError {
        dt_error_from_mongodb(error, ErrorCode::StatementFailed)
            .stage(Stage::Precheck)
            .operation(operation)
            .endpoint(endpoint(is_source))
    }
}
