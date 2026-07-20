pub(crate) mod connection {
    use dt_common::error::{
        dt_error_from_sqlx, DtError, EndpointRole, ErrorCode, SqlxProvider, Stage,
    };

    #[track_caller]
    pub(crate) fn metadata(
        error: sqlx::Error,
        provider: SqlxProvider,
        operation: &'static str,
    ) -> DtError {
        dt_error_from_sqlx(error, provider, ErrorCode::MetadataFailed)
            .stage(Stage::Task)
            .operation(operation)
    }

    #[track_caller]
    pub(crate) fn missing(expected: &'static str, operation: &'static str) -> DtError {
        DtError::new(ErrorCode::InvariantViolated)
            .detail(format!("expected {expected} connection client is missing"))
            .stage(Stage::Task)
            .operation(operation)
    }

    #[track_caller]
    pub(crate) fn invalid_config(detail: impl Into<String>) -> DtError {
        DtError::new(ErrorCode::InvalidConfig)
            .detail(detail)
            .stage(Stage::Bootstrap)
    }

    pub(crate) fn attach_endpoint(
        mut error: anyhow::Error,
        endpoint: EndpointRole,
    ) -> anyhow::Error {
        if let Some(error) = error.downcast_mut::<DtError>() {
            if error.endpoint.is_none() {
                error.endpoint = Some(endpoint);
            }
        }
        error
    }
}

pub(crate) mod extractor {
    use dt_common::error::{DtError, ErrorCode, Stage};

    #[track_caller]
    pub(crate) fn missing_client() -> DtError {
        DtError::new(ErrorCode::InvariantViolated)
            .detail("the configured source connection client is missing")
            .stage(Stage::Task)
            .operation("build_extractor")
    }

    #[track_caller]
    pub(crate) fn invalid_config(detail: impl Into<String>, operation: &'static str) -> DtError {
        DtError::new(ErrorCode::InvalidConfig)
            .detail(detail)
            .stage(Stage::Bootstrap)
            .operation(operation)
    }
}

pub(crate) mod sinker {
    use dt_common::error::{DtError, ErrorCode, Stage};

    #[track_caller]
    pub(crate) fn missing_client() -> DtError {
        DtError::new(ErrorCode::InvariantViolated)
            .detail("the configured destination connection client is missing")
            .stage(Stage::Task)
            .operation("build_sinker")
    }
}

pub(crate) mod runner {
    use dt_common::error::{DtError, ErrorCode, Stage};

    #[track_caller]
    pub(crate) fn invalid_config(detail: impl Into<String>) -> DtError {
        DtError::new(ErrorCode::InvalidConfig)
            .detail(detail)
            .stage(Stage::Bootstrap)
    }
}
