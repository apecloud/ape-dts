pub(crate) mod extractor {
    use dt_common::error::{
        dt_error_from_rdkafka, dt_error_from_sqlx, dt_error_from_tokio_postgres, DtError,
        EndpointRole, ErrorCode, OriginError, SqlxProvider, Stage,
    };
    use rdkafka::error::KafkaError;

    fn enrich(error: DtError, operation: &'static str) -> DtError {
        error
            .stage(Stage::Extractor)
            .operation(operation)
            .endpoint(EndpointRole::Source)
    }

    #[track_caller]
    pub(crate) fn mysql_sqlx(
        error: sqlx::Error,
        fallback: ErrorCode,
        operation: &'static str,
    ) -> DtError {
        enrich(
            dt_error_from_sqlx(error, SqlxProvider::MySql, fallback),
            operation,
        )
    }

    #[track_caller]
    pub(crate) fn postgres_sqlx(
        error: sqlx::Error,
        fallback: ErrorCode,
        operation: &'static str,
    ) -> DtError {
        enrich(
            dt_error_from_sqlx(error, SqlxProvider::Postgres, fallback),
            operation,
        )
    }

    #[track_caller]
    pub(crate) fn postgres(
        error: tokio_postgres::Error,
        fallback: ErrorCode,
        operation: &'static str,
    ) -> DtError {
        enrich(dt_error_from_tokio_postgres(error, fallback), operation)
    }

    #[track_caller]
    pub(crate) fn kafka(
        error: KafkaError,
        fallback: ErrorCode,
        operation: &'static str,
    ) -> DtError {
        enrich(dt_error_from_rdkafka(error, fallback), operation)
    }

    #[track_caller]
    pub(crate) fn redis_source(
        code: ErrorCode,
        detail: impl Into<String>,
        operation: &'static str,
    ) -> DtError {
        enrich(DtError::new(code), operation)
            .detail(detail)
            .origin(OriginError::new("redis", None::<String>))
    }

    #[track_caller]
    pub(crate) fn redis_invalid_config(
        detail: impl Into<String>,
        operation: &'static str,
    ) -> DtError {
        DtError::new(ErrorCode::InvalidConfig)
            .detail(detail)
            .stage(Stage::Bootstrap)
            .operation(operation)
            .endpoint(EndpointRole::Source)
            .origin(OriginError::new("redis", None::<String>))
    }

    #[track_caller]
    pub(crate) fn redis_io(error: std::io::Error, operation: &'static str) -> DtError {
        let code = if matches!(
            error.kind(),
            std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
        ) {
            ErrorCode::ConnectionTimeout
        } else {
            ErrorCode::ConnectionFailed
        };
        enrich(DtError::new(code), operation)
            .origin(OriginError::new("redis", None::<String>))
            .source(error)
    }

    #[track_caller]
    pub(crate) fn redis_rdb(detail: impl Into<String>) -> DtError {
        redis_source(ErrorCode::StatementFailed, detail, "parse_redis_rdb")
    }

    #[track_caller]
    pub(crate) fn redis_reshard_metadata(operation: &'static str) -> DtError {
        enrich(DtError::new(ErrorCode::MetadataFailed), operation)
            .origin(OriginError::new("redis", None::<String>))
    }

    #[track_caller]
    pub(crate) fn redis_snapshot_file(error: std::io::Error, operation: &'static str) -> DtError {
        DtError::new(ErrorCode::IoFailed)
            .detail("failed to read the configured Redis snapshot file")
            .stage(Stage::Extractor)
            .operation(operation)
            .origin(OriginError::new("redis", None::<String>))
            .source(error)
    }

    #[track_caller]
    pub(crate) fn mongodb_oplog(detail: impl Into<String>) -> DtError {
        enrich(
            DtError::new(ErrorCode::StatementFailed),
            "parse_mongodb_oplog",
        )
        .detail(detail)
        .origin(OriginError::new("mongodb", None::<String>))
    }

    #[track_caller]
    pub(crate) fn mysql_binlog_metadata(operation: &'static str) -> DtError {
        enrich(DtError::new(ErrorCode::MetadataFailed), operation)
            .origin(OriginError::new("mysql", None::<String>))
    }

    #[track_caller]
    pub(crate) fn mysql_binlog(
        error: mysql_binlog_connector_rust::binlog_error::BinlogError,
        code: ErrorCode,
        operation: &'static str,
    ) -> DtError {
        enrich(DtError::new(code), operation)
            .origin(OriginError::new("mysql", None::<String>))
            .source(error)
    }

    #[track_caller]
    pub(crate) fn postgres_tls(
        error: openssl::error::ErrorStack,
        operation: &'static str,
    ) -> DtError {
        enrich(DtError::new(ErrorCode::TlsFailed), operation)
            .origin(OriginError::new("postgres", None::<String>))
            .source(error)
    }

    #[track_caller]
    pub(crate) fn resumer_config(detail: impl Into<String>, operation: &'static str) -> DtError {
        DtError::new(ErrorCode::InvalidConfig)
            .detail(detail)
            .stage(Stage::Bootstrap)
            .operation(operation)
    }

    #[track_caller]
    pub(crate) fn checkpoint_sqlx(
        error: sqlx::Error,
        provider: SqlxProvider,
        task_id: &str,
        schema: &str,
        table: &str,
    ) -> DtError {
        let mut error = dt_error_from_sqlx(error, provider, ErrorCode::CheckpointReadFailed)
            .message("failed to query resume position from database")
            .stage(Stage::Resumer)
            .operation("load_checkpoint")
            .task_id(task_id)
            .endpoint(EndpointRole::Metadata);
        let object = error.object.get_or_insert_with(Default::default);
        object.schema.get_or_insert_with(|| schema.to_string());
        object.table.get_or_insert_with(|| table.to_string());
        error
    }
}

pub mod sinker {
    use dt_common::error::{
        dt_error_from_kafka, dt_error_from_mongodb, dt_error_from_rdkafka, dt_error_from_redis,
        dt_error_from_reqwest, dt_error_from_sqlx, try_dt_error_from_anyhow_sqlx, DtError,
        EndpointRole, ErrorCode, OriginError, SqlxProvider, Stage,
    };
    use rdkafka::error::KafkaError;

    fn enrich(error: DtError, operation: &'static str) -> DtError {
        error
            .stage(Stage::Sinker)
            .operation(operation)
            .endpoint(EndpointRole::Destination)
    }

    #[track_caller]
    pub(crate) fn mysql(
        error: sqlx::Error,
        fallback: ErrorCode,
        operation: &'static str,
    ) -> DtError {
        enrich(
            dt_error_from_sqlx(error, SqlxProvider::MySql, fallback),
            operation,
        )
    }

    #[track_caller]
    pub(crate) fn postgres(
        error: sqlx::Error,
        fallback: ErrorCode,
        operation: &'static str,
    ) -> DtError {
        enrich(
            dt_error_from_sqlx(error, SqlxProvider::Postgres, fallback),
            operation,
        )
    }

    #[track_caller]
    pub(crate) fn mysql_from_anyhow(
        error: anyhow::Error,
        fallback: ErrorCode,
        operation: &'static str,
    ) -> anyhow::Error {
        from_anyhow(error, SqlxProvider::MySql, fallback, operation)
    }

    #[track_caller]
    pub(crate) fn postgres_from_anyhow(
        error: anyhow::Error,
        fallback: ErrorCode,
        operation: &'static str,
    ) -> anyhow::Error {
        from_anyhow(error, SqlxProvider::Postgres, fallback, operation)
    }

    #[track_caller]
    fn from_anyhow(
        error: anyhow::Error,
        provider: SqlxProvider,
        fallback: ErrorCode,
        operation: &'static str,
    ) -> anyhow::Error {
        match try_dt_error_from_anyhow_sqlx(error, provider, fallback) {
            Ok(error) => enrich(error, operation).into(),
            Err(error) => error,
        }
    }

    #[track_caller]
    pub(crate) fn reqwest(error: reqwest::Error, operation: &'static str) -> DtError {
        enrich(
            dt_error_from_reqwest(error, ErrorCode::StatementFailed),
            operation,
        )
    }

    #[track_caller]
    pub(crate) fn http_status(status: reqwest::StatusCode, operation: &'static str) -> DtError {
        enrich(DtError::new(ErrorCode::StatementFailed), operation)
            .origin(OriginError::new("http", Some(status.as_u16().to_string())))
            .detail(format!(
                "the destination rejected the request with HTTP status {}",
                status.as_u16()
            ))
    }

    #[track_caller]
    pub(crate) fn http_rejected(status: reqwest::StatusCode, operation: &'static str) -> DtError {
        enrich(DtError::new(ErrorCode::StatementFailed), operation)
            .origin(OriginError::new("http", Some(status.as_u16().to_string())))
            .detail("the destination rejected the data load request")
    }

    #[track_caller]
    pub(crate) fn http_invalid_response(
        error: serde_json::Error,
        operation: &'static str,
    ) -> DtError {
        enrich(DtError::new(ErrorCode::StatementFailed), operation)
            .origin(OriginError::new("http", None::<String>))
            .source(error)
    }

    #[track_caller]
    pub(crate) fn redis_destination(
        code: ErrorCode,
        detail: impl Into<String>,
        operation: &'static str,
    ) -> DtError {
        enrich(DtError::new(code), operation)
            .detail(detail)
            .origin(OriginError::new("redis", None::<String>))
    }

    #[track_caller]
    pub(crate) fn redis(
        error: redis::RedisError,
        fallback: ErrorCode,
        operation: &'static str,
    ) -> DtError {
        enrich(dt_error_from_redis(error, fallback), operation)
    }

    #[track_caller]
    pub(crate) fn redis_metadata(operation: &'static str) -> DtError {
        redis_destination(
            ErrorCode::MetadataFailed,
            "Redis cluster slot metadata is missing",
            operation,
        )
    }

    #[track_caller]
    pub(crate) fn mongodb_struct(error: mongodb::error::Error, operation: &'static str) -> DtError {
        enrich(
            dt_error_from_mongodb(error, ErrorCode::StatementFailed),
            operation,
        )
    }

    #[track_caller]
    pub(crate) fn clickhouse_metadata() -> DtError {
        enrich(
            DtError::new(ErrorCode::MetadataFailed),
            "build_clickhouse_table",
        )
        .origin(OriginError::new("clickhouse", None::<String>))
    }

    #[track_caller]
    pub(crate) fn rdkafka(
        error: KafkaError,
        fallback: ErrorCode,
        operation: &'static str,
    ) -> DtError {
        enrich(dt_error_from_rdkafka(error, fallback), operation)
    }

    #[track_caller]
    pub fn kafka(error: ::kafka::Error, fallback: ErrorCode, operation: &'static str) -> DtError {
        enrich(dt_error_from_kafka(error, fallback), operation)
    }
}

pub(crate) mod checker {
    use dt_common::error::{DtError, ErrorCode, Stage};

    #[track_caller]
    pub(crate) fn state_config(detail: impl Into<String>) -> DtError {
        DtError::new(ErrorCode::InvalidConfig)
            .detail(detail)
            .stage(Stage::Bootstrap)
            .operation("build_checker_state_store")
    }
}

pub(crate) mod router {
    use dt_common::error::{DtError, ErrorCode, Stage};

    #[track_caller]
    pub(crate) fn invalid_config(detail: impl Into<String>) -> DtError {
        DtError::new(ErrorCode::InvalidConfig)
            .detail(detail)
            .stage(Stage::Bootstrap)
            .operation("parse_router_config")
    }
}

#[cfg(test)]
mod tests {
    use dt_common::error::{EndpointRole, ErrorCode, ErrorReport, OriginError, Stage};
    use rdkafka::error::KafkaError;

    use super::{extractor, sinker};

    #[test]
    fn component_wrappers_attach_fixed_context() {
        let extractor_error = extractor::kafka(
            KafkaError::ClientCreation("invalid client".to_string()),
            ErrorCode::ConnectionFailed,
            "create_consumer",
        );
        assert_eq!(extractor_error.root_stage(), Some(Stage::Extractor));
        assert_eq!(extractor_error.endpoint, Some(EndpointRole::Source));
        assert_eq!(extractor_error.root_operation(), Some("create_consumer"));

        let sinker_error = sinker::kafka(
            ::kafka::Error::NoHostReachable,
            ErrorCode::StatementFailed,
            "send_message",
        );
        assert_eq!(sinker_error.root_stage(), Some(Stage::Sinker));
        assert_eq!(sinker_error.endpoint, Some(EndpointRole::Destination));
        assert_eq!(sinker_error.root_operation(), Some("send_message"));
    }

    #[test]
    fn sqlx_wrappers_attach_component_context() {
        let caller_line = line!() + 1;
        let extractor_error = extractor::mysql_sqlx(
            sqlx::Error::PoolTimedOut,
            ErrorCode::MetadataFailed,
            "list_binary_logs",
        );
        assert_eq!(extractor_error.code(), ErrorCode::ConnectionTimeout);
        assert_eq!(extractor_error.root_stage(), Some(Stage::Extractor));
        assert_eq!(extractor_error.endpoint, Some(EndpointRole::Source));
        assert_eq!(extractor_error.location.file(), file!());
        assert_eq!(extractor_error.location.line(), caller_line);

        let sinker_error = sinker::mysql(
            sqlx::Error::PoolClosed,
            ErrorCode::StatementFailed,
            "sink_dml",
        );
        assert_eq!(sinker_error.code(), ErrorCode::ConnectionFailed);
        assert_eq!(sinker_error.root_stage(), Some(Stage::Sinker));
        assert_eq!(sinker_error.endpoint, Some(EndpointRole::Destination));
        assert_eq!(
            sinker_error.origin_error(),
            Some(&OriginError::new("mysql", None::<String>))
        );
    }

    #[test]
    fn anyhow_wrapper_preserves_existing_structured_error() {
        let original = dt_common::error::DtError::new(ErrorCode::WorkerFailed)
            .stage(Stage::Task)
            .operation("join_worker");
        let error = sinker::mysql_from_anyhow(
            anyhow::Error::new(original),
            ErrorCode::StatementFailed,
            "sink_struct",
        );
        let report = ErrorReport::from_anyhow(&error);

        assert_eq!(report.code, ErrorCode::WorkerFailed);
        assert_eq!(report.stage, Stage::Task);
        assert_eq!(report.operation.as_deref(), Some("join_worker"));
        assert_eq!(report.endpoint, None);
    }
}
