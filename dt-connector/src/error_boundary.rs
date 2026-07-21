pub(crate) mod extractor_error {
    use std::error::Error as StdError;

    use dt_common::error::{
        classify_rdkafka_error, classify_sqlx_error, classify_tokio_postgres_error, DtError,
        DtErrorContext, DtErrorContextExt, EndpointRole, ErrorCode, OriginError, SqlxProvider,
        Stage,
    };
    use rdkafka::error::KafkaError;

    fn enrich(error: DtErrorContext) -> DtErrorContext {
        DtErrorContext::new()
            .stage(Stage::Extractor)
            .endpoint(EndpointRole::Source)
            .inherit(error)
    }
    pub(crate) fn mysql_sqlx(error: sqlx::Error, default_code: ErrorCode) -> anyhow::Error {
        let context = classify_sqlx_error(&error, SqlxProvider::MySql).into_context();
        error
            .with_code(default_code)
            .with_context(context)
            .with_stage(Stage::Extractor)
            .with_endpoint(EndpointRole::Source)
    }
    pub(crate) fn postgres_sqlx(error: sqlx::Error, default_code: ErrorCode) -> anyhow::Error {
        let context = classify_sqlx_error(&error, SqlxProvider::Postgres).into_context();
        error
            .with_code(default_code)
            .with_context(context)
            .with_stage(Stage::Extractor)
            .with_endpoint(EndpointRole::Source)
    }
    pub(crate) fn postgres(error: tokio_postgres::Error, default_code: ErrorCode) -> anyhow::Error {
        let context = classify_tokio_postgres_error(&error).into_context();
        error
            .with_code(default_code)
            .with_context(context)
            .with_stage(Stage::Extractor)
            .with_endpoint(EndpointRole::Source)
    }
    pub(crate) fn kafka(error: KafkaError, default_code: ErrorCode) -> anyhow::Error {
        let context = classify_rdkafka_error(&error).into_context();
        error
            .with_code(default_code)
            .with_context(context)
            .with_stage(Stage::Extractor)
            .with_endpoint(EndpointRole::Source)
    }
    pub(crate) fn worker(error: tokio::task::JoinError) -> anyhow::Error {
        enrich(DtErrorContext::new().code(ErrorCode::WorkerFailed)).attach(error)
    }
    pub(crate) fn provider_source<E>(
        error: E,
        code: ErrorCode,
        provider: &'static str,
    ) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        enrich(DtErrorContext::new().code(code))
            .origin(OriginError::new(provider, None::<String>))
            .attach(error)
    }
    pub(crate) fn provider_anyhow(
        error: anyhow::Error,
        code: ErrorCode,
        detail: impl Into<String>,
        provider: &'static str,
    ) -> anyhow::Error {
        error
            .with_code(code)
            .with_detail(detail)
            .with_origin(OriginError::new(provider, None::<String>))
            .with_stage(Stage::Extractor)
            .with_endpoint(EndpointRole::Source)
    }
    pub(crate) fn redis_source_error(code: ErrorCode, detail: impl Into<String>) -> anyhow::Error {
        DtError::RedisResultError(detail.into())
            .with_code(code)
            .with_origin(OriginError::new("redis", None::<String>))
            .with_stage(Stage::Extractor)
            .with_endpoint(EndpointRole::Source)
    }
    pub(crate) fn redis_source_error_with_cause<E>(
        code: ErrorCode,
        detail: impl Into<String>,
        error: E,
    ) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        enrich(DtErrorContext::new().code(code))
            .detail(detail)
            .origin(OriginError::new("redis", None::<String>))
            .attach(error)
    }
    pub(crate) fn redis_source_error_with_anyhow(
        code: ErrorCode,
        detail: impl Into<String>,
        error: anyhow::Error,
    ) -> anyhow::Error {
        error.with_context(
            enrich(DtErrorContext::new().code(code))
                .detail(detail)
                .origin(OriginError::new("redis", None::<String>)),
        )
    }
    pub(crate) fn invalid_redis_config(detail: impl Into<String>) -> anyhow::Error {
        DtError::ConfigError(detail.into())
            .with_code(ErrorCode::InvalidConfig)
            .with_stage(Stage::Bootstrap)
            .with_endpoint(EndpointRole::Source)
            .with_origin(OriginError::new("redis", None::<String>))
    }
    pub(crate) fn redis_invalid_config_source<E>(
        detail: impl Into<String>,
        error: E,
    ) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        DtErrorContext::new()
            .code(ErrorCode::InvalidConfig)
            .detail(detail)
            .stage(Stage::Bootstrap)
            .endpoint(EndpointRole::Source)
            .origin(OriginError::new("redis", None::<String>))
            .attach(error)
    }
    pub(crate) fn redis_io(error: std::io::Error) -> anyhow::Error {
        let code = if matches!(
            error.kind(),
            std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
        ) {
            ErrorCode::ConnectionTimeout
        } else {
            ErrorCode::ConnectionFailed
        };
        enrich(DtErrorContext::new().code(code))
            .origin(OriginError::new("redis", None::<String>))
            .attach(error)
    }
    pub(crate) fn rdb_error(detail: impl Into<String>) -> anyhow::Error {
        DtError::RedisRdbError(detail.into())
            .with_code(ErrorCode::StatementFailed)
            .with_origin(OriginError::new("redis", None::<String>))
            .with_stage(Stage::Extractor)
            .with_endpoint(EndpointRole::Source)
    }
    pub(crate) fn redis_rdb_source<E>(detail: impl Into<String>, error: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        redis_source_error_with_cause(ErrorCode::StatementFailed, detail, error)
    }
    pub(crate) fn redis_reshard_topology(detail: impl Into<String>) -> anyhow::Error {
        let detail = detail.into();
        DtError::RedisResultError(detail.clone())
            .with_code(ErrorCode::PrerequisiteNotMet)
            .with_message("The Redis cluster topology is incomplete or changed")
            .with_detail(detail)
            .with_hint(
                "Ensure every Redis cluster slot has a stable master owner, then restart the task.",
            )
            .with_origin(OriginError::new("redis", None::<String>))
            .with_stage(Stage::Extractor)
            .with_endpoint(EndpointRole::Source)
    }
    pub(crate) fn redis_snapshot_file(error: std::io::Error) -> anyhow::Error {
        DtErrorContext::new()
            .code(ErrorCode::IoFailed)
            .detail("failed to read the configured Redis snapshot file")
            .stage(Stage::Extractor)
            .origin(OriginError::new("redis", None::<String>))
            .attach(error)
    }
    pub(crate) fn mongodb_oplog(detail: impl Into<String>) -> anyhow::Error {
        DtError::ExtractorError(detail.into())
            .with_code(ErrorCode::StatementFailed)
            .with_origin(OriginError::new("mongodb", None::<String>))
            .with_stage(Stage::Extractor)
            .with_endpoint(EndpointRole::Source)
    }
    pub(crate) fn mysql_binlog_table_map_missing(
        table_id: u64,
        event_type: &'static str,
    ) -> anyhow::Error {
        let detail = format!(
            "the {event_type} event for source table ID {table_id} arrived before Ape-DTS received its table definition"
        );
        DtError::ExtractorError(detail.clone())
            .with_code(ErrorCode::StatementFailed)
            .with_message("A MySQL row event could not be decoded")
            .with_detail(detail)
            .with_hint(
                "Restart from an earlier binlog position so Ape-DTS can reload the table definition. If it repeats, check binlog retention and the source database logs.",
            )
            .with_origin(OriginError::new("mysql", None::<String>))
            .with_stage(Stage::Extractor)
            .with_endpoint(EndpointRole::Source)
    }
    pub(crate) fn mysql_binlog(
        error: mysql_binlog_connector_rust::binlog_error::BinlogError,
        code: ErrorCode,
    ) -> anyhow::Error {
        enrich(DtErrorContext::new().code(code))
            .origin(OriginError::new("mysql", None::<String>))
            .attach(error)
    }
    pub(crate) fn postgres_tls(error: openssl::error::ErrorStack) -> anyhow::Error {
        enrich(DtErrorContext::new().code(ErrorCode::TlsFailed))
            .origin(OriginError::new("postgres", None::<String>))
            .attach(error)
    }
    pub(crate) fn invalid_postgres_lsn() -> anyhow::Error {
        DtError::ExtractorError("a PostgreSQL replication position is invalid".to_string())
            .with_code(ErrorCode::CheckpointReadFailed)
            .with_origin(OriginError::new("postgres", None::<String>))
            .with_stage(Stage::Extractor)
            .with_endpoint(EndpointRole::Source)
    }
    pub(crate) fn invalid_resumer_config(detail: impl Into<String>) -> anyhow::Error {
        DtError::ConfigError(detail.into())
            .with_code(ErrorCode::InvalidConfig)
            .with_stage(Stage::Bootstrap)
    }
    pub(crate) fn resumer_config_source<E>(detail: impl Into<String>, error: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        DtErrorContext::new()
            .code(ErrorCode::InvalidConfig)
            .detail(detail)
            .stage(Stage::Bootstrap)
            .attach(error)
    }
    pub(crate) fn checkpoint_sqlx(
        error: sqlx::Error,
        provider: SqlxProvider,
        task_id: &str,
        schema: &str,
        table: &str,
    ) -> anyhow::Error {
        let context = classify_sqlx_error(&error, provider)
            .into_context()
            .message("failed to query resume position from database");
        let mut object = context.error_object().cloned().unwrap_or_default();
        object.schema.get_or_insert_with(|| schema.to_string());
        object.table.get_or_insert_with(|| table.to_string());
        error
            .with_code(ErrorCode::CheckpointReadFailed)
            .with_context(context.object(object))
            .with_stage(Stage::Resumer)
            .with_task_id(task_id)
            .with_endpoint(EndpointRole::Metadata)
    }
}

pub mod sinker_error {
    use dt_common::error::{
        classify_kafka_error, classify_mongodb_error, classify_rdkafka_error, classify_redis_error,
        classify_reqwest_error, classify_sqlx_error, DtError, DtErrorContext, DtErrorContextExt,
        EndpointRole, ErrorCode, OriginError, SqlxProvider, Stage,
    };
    use rdkafka::error::KafkaError;

    fn enrich(error: DtErrorContext) -> DtErrorContext {
        DtErrorContext::new()
            .stage(Stage::Sinker)
            .endpoint(EndpointRole::Destination)
            .inherit(error)
    }
    pub(crate) fn mysql(error: sqlx::Error, default_code: ErrorCode) -> anyhow::Error {
        let context = classify_sqlx_error(&error, SqlxProvider::MySql).into_context();
        error
            .with_code(default_code)
            .with_context(context)
            .with_stage(Stage::Sinker)
            .with_endpoint(EndpointRole::Destination)
    }
    pub(crate) fn postgres(error: sqlx::Error, default_code: ErrorCode) -> anyhow::Error {
        let context = classify_sqlx_error(&error, SqlxProvider::Postgres).into_context();
        error
            .with_code(default_code)
            .with_context(context)
            .with_stage(Stage::Sinker)
            .with_endpoint(EndpointRole::Destination)
    }
    pub(crate) fn reqwest(error: reqwest::Error) -> anyhow::Error {
        let context = classify_reqwest_error(&error).into_context();
        error
            .with_code(ErrorCode::StatementFailed)
            .with_context(context)
            .with_stage(Stage::Sinker)
            .with_endpoint(EndpointRole::Destination)
    }
    pub(crate) fn http_status(status: reqwest::StatusCode) -> anyhow::Error {
        let detail = format!(
            "the destination rejected the request with HTTP status {}",
            status.as_u16()
        );
        DtError::HttpError(detail)
            .with_code(ErrorCode::StatementFailed)
            .with_origin(OriginError::new("http", Some(status.as_u16().to_string())))
            .with_stage(Stage::Sinker)
            .with_endpoint(EndpointRole::Destination)
    }
    pub(crate) fn http_rejected(status: reqwest::StatusCode) -> anyhow::Error {
        DtError::HttpError("the destination rejected the data load request".to_string())
            .with_code(ErrorCode::StatementFailed)
            .with_origin(OriginError::new("http", Some(status.as_u16().to_string())))
            .with_stage(Stage::Sinker)
            .with_endpoint(EndpointRole::Destination)
    }
    pub(crate) fn http_invalid_response(error: serde_json::Error) -> anyhow::Error {
        enrich(DtErrorContext::new().code(ErrorCode::StatementFailed))
            .origin(OriginError::new("http", None::<String>))
            .attach(error)
    }
    pub(crate) fn redis_destination_error(
        code: ErrorCode,
        detail: impl Into<String>,
    ) -> anyhow::Error {
        DtError::RedisResultError(detail.into())
            .with_code(code)
            .with_origin(OriginError::new("redis", None::<String>))
            .with_stage(Stage::Sinker)
            .with_endpoint(EndpointRole::Destination)
    }
    pub(crate) fn redis_driver_error(
        error: redis::RedisError,
        default_code: ErrorCode,
    ) -> anyhow::Error {
        let context = classify_redis_error(&error).into_context();
        error
            .with_code(default_code)
            .with_context(context)
            .with_stage(Stage::Sinker)
            .with_endpoint(EndpointRole::Destination)
    }
    pub(crate) fn redis_slot_topology() -> anyhow::Error {
        let detail = "A required Redis cluster slot has no master owner in the loaded topology";
        DtError::RedisResultError(detail.to_string())
            .with_code(ErrorCode::PrerequisiteNotMet)
            .with_detail(detail)
            .with_origin(OriginError::new("redis", None::<String>))
            .with_message("The Redis cluster slot map is incomplete or changed")
            .with_hint(
                "Ensure all Redis cluster slots are assigned and stable, then restart the task.",
            )
            .with_stage(Stage::Sinker)
            .with_endpoint(EndpointRole::Destination)
    }
    pub(crate) fn mongodb_struct(error: mongodb::error::Error) -> anyhow::Error {
        let context = classify_mongodb_error(&error).into_context();
        error
            .with_code(ErrorCode::StatementFailed)
            .with_context(context)
            .with_stage(Stage::Sinker)
            .with_endpoint(EndpointRole::Destination)
    }
    pub(crate) fn clickhouse_source_metadata_missing() -> anyhow::Error {
        let detail = "Ape-DTS could not determine the source table definition needed to build the ClickHouse table";
        DtError::SinkerError(detail.to_string())
            .with_code(ErrorCode::ObjectNotFound)
            .with_message(
                "Source table metadata is unavailable for ClickHouse structure migration",
            )
            .with_detail(detail)
            .with_hint(
                "Verify that the source table still exists and rerun structure migration. If it repeats, contact support with the task ID and error code.",
            )
            .with_origin(OriginError::new("clickhouse", None::<String>))
            .with_stage(Stage::Sinker)
            .with_endpoint(EndpointRole::Destination)
    }
    pub(crate) fn rdkafka(error: KafkaError, default_code: ErrorCode) -> anyhow::Error {
        let context = classify_rdkafka_error(&error).into_context();
        error
            .with_code(default_code)
            .with_context(context)
            .with_stage(Stage::Sinker)
            .with_endpoint(EndpointRole::Destination)
    }
    pub fn kafka(error: ::kafka::Error, default_code: ErrorCode) -> anyhow::Error {
        let context = classify_kafka_error(&error).into_context();
        error
            .with_code(default_code)
            .with_context(context)
            .with_stage(Stage::Sinker)
            .with_endpoint(EndpointRole::Destination)
    }
}

pub(crate) mod checker {
    use dt_common::error::{DtError, DtErrorContextExt, ErrorCode, Stage};
    pub(crate) fn state_config(detail: impl Into<String>) -> anyhow::Error {
        DtError::ConfigError(detail.into())
            .with_code(ErrorCode::InvalidConfig)
            .with_stage(Stage::Bootstrap)
    }
    pub(crate) fn parse_source(error: anyhow::Error, detail: impl Into<String>) -> anyhow::Error {
        error
            .with_code(ErrorCode::StatementFailed)
            .with_detail(detail)
            .with_stage(Stage::Checker)
    }
}

pub(crate) mod router {
    use std::error::Error as StdError;

    use dt_common::error::{DtErrorContext, ErrorCode, Stage};
    pub(crate) fn invalid_config_source<E>(detail: impl Into<String>, error: E) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        DtErrorContext::new()
            .code(ErrorCode::InvalidConfig)
            .detail(detail)
            .stage(Stage::Bootstrap)
            .attach(error)
    }
}

#[cfg(test)]
mod tests {
    use dt_common::error::{
        AnyhowErrorExt, DtError, DtErrorContextExt, EndpointRole, ErrorCode, ErrorReport,
        OriginError, Stage,
    };
    use rdkafka::error::KafkaError;

    use super::{extractor_error, sinker_error};

    #[test]
    fn component_wrappers_attach_fixed_context() {
        let extractor_error = extractor_error::kafka(
            KafkaError::ClientCreation("invalid client".to_string()),
            ErrorCode::ConnectionFailed,
        );
        let context = extractor_error.dt_context().unwrap();
        assert_eq!(context.stage_value(), Some(Stage::Extractor));
        assert_eq!(context.endpoint_role(), Some(EndpointRole::Source));
        assert!(extractor_error.downcast_ref::<KafkaError>().is_some());

        let sinker_error =
            sinker_error::kafka(::kafka::Error::NoHostReachable, ErrorCode::StatementFailed);
        let context = sinker_error.dt_context().unwrap();
        assert_eq!(context.stage_value(), Some(Stage::Sinker));
        assert_eq!(context.endpoint_role(), Some(EndpointRole::Destination));
        assert!(sinker_error.downcast_ref::<::kafka::Error>().is_some());
    }

    #[test]
    fn sqlx_wrappers_attach_component_context() {
        let extractor_error =
            extractor_error::mysql_sqlx(sqlx::Error::PoolTimedOut, ErrorCode::MetadataReadFailed);
        let context = extractor_error.dt_context().unwrap();
        assert_eq!(context.error_code(), Some(ErrorCode::ConnectionTimeout));
        assert_eq!(context.stage_value(), Some(Stage::Extractor));
        assert_eq!(context.endpoint_role(), Some(EndpointRole::Source));
        assert!(extractor_error.downcast_ref::<sqlx::Error>().is_some());

        let sinker_error = sinker_error::mysql(sqlx::Error::PoolClosed, ErrorCode::StatementFailed);
        let context = sinker_error.dt_context().unwrap();
        assert_eq!(context.error_code(), Some(ErrorCode::ConnectionFailed));
        assert_eq!(context.stage_value(), Some(Stage::Sinker));
        assert_eq!(context.endpoint_role(), Some(EndpointRole::Destination));
        assert_eq!(
            context.origin_error(),
            Some(&OriginError::new("mysql", None::<String>))
        );
        assert!(sinker_error.downcast_ref::<sqlx::Error>().is_some());

        let unclassified_provider_error =
            sinker_error::postgres(sqlx::Error::RowNotFound, ErrorCode::StatementFailed);
        assert_eq!(
            unclassified_provider_error
                .dt_context()
                .and_then(|context| context.error_code()),
            Some(ErrorCode::StatementFailed)
        );
    }

    #[test]
    fn ordinary_context_preserves_existing_structured_error() {
        let original = DtError::Unexpected("worker failed".to_string())
            .with_code(ErrorCode::WorkerFailed)
            .with_stage(Stage::Task);
        let error = original.context("sink_struct");
        let report = ErrorReport::from_anyhow(&error);

        assert_eq!(report.code, ErrorCode::WorkerFailed);
        assert_eq!(report.stage, Stage::Task);
        assert!(!report.to_string().contains("OPERATION:"));
        assert_eq!(report.endpoint, None);
    }

    #[test]
    fn component_errors_explain_the_problem_and_next_action() {
        let redis_reshard =
            extractor_error::redis_reshard_topology("Redis slot 42 has no master owner");
        let report = ErrorReport::from_anyhow(&redis_reshard);
        assert_eq!(report.code, ErrorCode::PrerequisiteNotMet);
        assert_eq!(
            report.message,
            "The Redis cluster topology is incomplete or changed"
        );
        assert_eq!(
            report.detail.as_deref(),
            Some("Redis slot 42 has no master owner")
        );
        assert!(report
            .hint
            .as_deref()
            .is_some_and(|hint| hint.contains("stable master owner")));

        let mysql_binlog = extractor_error::mysql_binlog_table_map_missing(73, "write rows");
        let report = ErrorReport::from_anyhow(&mysql_binlog);
        assert_eq!(report.code, ErrorCode::StatementFailed);
        assert_eq!(report.message, "A MySQL row event could not be decoded");
        assert!(report
            .detail
            .as_deref()
            .is_some_and(|detail| detail.contains("table ID 73")));
        assert!(report
            .hint
            .as_deref()
            .is_some_and(|hint| hint.contains("earlier binlog position")));

        let redis_sink = sinker_error::redis_slot_topology();
        let report = ErrorReport::from_anyhow(&redis_sink);
        assert_eq!(report.code, ErrorCode::PrerequisiteNotMet);
        assert_eq!(
            report.message,
            "The Redis cluster slot map is incomplete or changed"
        );

        let clickhouse = sinker_error::clickhouse_source_metadata_missing();
        let report = ErrorReport::from_anyhow(&clickhouse);
        assert_eq!(report.code, ErrorCode::ObjectNotFound);
        assert!(report.message.contains("ClickHouse structure migration"));
        assert!(report
            .hint
            .as_deref()
            .is_some_and(|hint| hint.contains("rerun structure migration")));
    }
}
