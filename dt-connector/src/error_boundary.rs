pub(crate) mod extractor_error {
    use std::error::Error as StdError;

    use dt_common::error::{
        classify_rdkafka_error, classify_sqlx_error, classify_tokio_postgres_error, DtError,
        DtErrorContext, DtErrorContextExt, EndpointRole, ErrorCode, OriginError, SqlxProvider,
        Stage,
    };
    use mysql_binlog_connector_rust::binlog_error::BinlogError;
    use rdkafka::error::KafkaError;

    pub(crate) fn mysql_sqlx(error: sqlx::Error, default_code: ErrorCode) -> anyhow::Error {
        let context = classify_sqlx_error(&error, SqlxProvider::MySql).into_context();
        error.with_code(default_code).with_context(context)
    }
    pub(crate) fn postgres_sqlx(error: sqlx::Error, default_code: ErrorCode) -> anyhow::Error {
        let context = classify_sqlx_error(&error, SqlxProvider::Postgres).into_context();
        error.with_code(default_code).with_context(context)
    }
    pub(crate) fn postgres(error: tokio_postgres::Error, default_code: ErrorCode) -> anyhow::Error {
        let context = classify_tokio_postgres_error(&error).into_context();
        error.with_code(default_code).with_context(context)
    }
    pub(crate) fn kafka(error: KafkaError, default_code: ErrorCode) -> anyhow::Error {
        let context = classify_rdkafka_error(&error).into_context();
        error.with_code(default_code).with_context(context)
    }
    pub(crate) fn worker(error: tokio::task::JoinError) -> anyhow::Error {
        DtErrorContext::new()
            .code(ErrorCode::WorkerFailed)
            .attach(error)
    }
    pub(crate) fn provider_source<E>(
        error: E,
        code: ErrorCode,
        provider: &'static str,
    ) -> anyhow::Error
    where
        E: StdError + Send + Sync + 'static,
    {
        DtErrorContext::new()
            .code(code)
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
            .with_origin(OriginError::new(provider, None::<String>))
            .context(detail.into())
    }
    pub(crate) fn redis_source_error(code: ErrorCode, detail: impl Into<String>) -> anyhow::Error {
        DtError::RedisResultError(detail.into())
            .with_code(code)
            .with_origin(OriginError::new("redis", None::<String>))
    }
    pub(crate) fn redis_source_error_with_cause<E>(
        code: ErrorCode,
        detail: impl Into<String>,
        error: E,
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
    pub(crate) fn redis_source_error_with_anyhow(
        code: ErrorCode,
        detail: impl Into<String>,
        error: anyhow::Error,
    ) -> anyhow::Error {
        error
            .with_context(
                DtErrorContext::new()
                    .code(code)
                    .origin(OriginError::new("redis", None::<String>)),
            )
            .context(detail.into())
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
            .stage(Stage::Bootstrap)
            .endpoint(EndpointRole::Source)
            .origin(OriginError::new("redis", None::<String>))
            .attach(error)
            .context(detail.into())
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
        DtErrorContext::new()
            .code(code)
            .origin(OriginError::new("redis", None::<String>))
            .attach(error)
    }
    pub(crate) fn rdb_error(detail: impl Into<String>) -> anyhow::Error {
        DtError::RedisRdbError(detail.into())
            .with_code(ErrorCode::StatementFailed)
            .with_origin(OriginError::new("redis", None::<String>))
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
            .with_hint(
                "Ensure every Redis cluster slot has a stable master owner, then restart the task.",
            )
            .with_origin(OriginError::new("redis", None::<String>))
    }
    pub(crate) fn redis_snapshot_file(error: std::io::Error) -> anyhow::Error {
        DtErrorContext::new()
            .code(ErrorCode::IoFailed)
            .origin(OriginError::new("redis", None::<String>))
            .attach(error)
            .context("failed to read the configured Redis snapshot file")
    }
    pub(crate) fn mongodb_oplog(detail: impl Into<String>) -> anyhow::Error {
        DtError::ExtractorError(detail.into())
            .with_code(ErrorCode::StatementFailed)
            .with_origin(OriginError::new("mongodb", None::<String>))
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
            .with_hint(
                "Restart from an earlier binlog position so Ape-DTS can reload the table definition. If it repeats, check binlog retention and the source database logs.",
            )
            .with_origin(OriginError::new("mysql", None::<String>))
    }
    pub(crate) fn mysql_binlog(error: BinlogError, code: ErrorCode) -> anyhow::Error {
        DtErrorContext::new()
            .code(code)
            .origin(OriginError::new("mysql", None::<String>))
            .attach(error)
    }
    pub(crate) fn mysql_binlog_read(error: BinlogError) -> anyhow::Error {
        let context = match &error {
            BinlogError::IoError(io_error) => {
                let code = if matches!(
                    io_error.kind(),
                    std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                ) {
                    ErrorCode::ConnectionTimeout
                } else {
                    ErrorCode::ConnectionFailed
                };
                DtErrorContext::new()
                    .code(code)
                    .origin(OriginError::new("mysql", None::<String>))
            }
            BinlogError::ConnectError(message) if mysql_binlog_is_unavailable(message) => {
                DtErrorContext::new()
                    .code(ErrorCode::CheckpointReadFailed)
                    .message("The requested MySQL binlog is no longer available")
                    .hint(
                        "Start from a retained binlog position or take a new snapshot, then increase the source binlog retention period.",
                    )
                    .origin(OriginError::new("mysql", Some("1236")))
            }
            BinlogError::ConnectError(_) => DtErrorContext::new()
                .code(ErrorCode::ConnectionFailed)
                .origin(OriginError::new("mysql", None::<String>)),
            BinlogError::InvalidGtid(_) => DtErrorContext::new()
                .code(ErrorCode::InvalidConfig)
                .origin(OriginError::new("mysql", None::<String>)),
            _ => DtErrorContext::new()
                .code(ErrorCode::StatementFailed)
                .origin(OriginError::new("mysql", None::<String>)),
        };
        context.attach(error)
    }
    fn mysql_binlog_is_unavailable(message: &str) -> bool {
        // v0.3.4 discards ErrorPacket.error_code and exposes MySQL 1236 as ConnectError(String).
        let message = message.to_ascii_lowercase();
        message.contains("fatal error 1236")
            || message.contains("could not find first log file name")
            || message.contains("binlog has been purged")
            || message.contains("not in binlog index")
            || message.contains("start replication from impossible position")
    }
    pub(crate) fn postgres_tls(error: openssl::error::ErrorStack) -> anyhow::Error {
        DtErrorContext::new()
            .code(ErrorCode::TlsFailed)
            .origin(OriginError::new("postgres", None::<String>))
            .attach(error)
    }
    pub(crate) fn invalid_postgres_lsn() -> anyhow::Error {
        DtError::ExtractorError("a PostgreSQL replication position is invalid".to_string())
            .with_code(ErrorCode::CheckpointReadFailed)
            .with_origin(OriginError::new("postgres", None::<String>))
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
            .stage(Stage::Bootstrap)
            .attach(error)
            .context(detail.into())
    }
    pub(crate) fn checkpoint_sqlx(
        error: sqlx::Error,
        provider: SqlxProvider,
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
    }
}

pub(crate) mod sinker_error {
    use dt_common::error::{
        classify_kafka_error, classify_mongodb_error, classify_rdkafka_error, classify_redis_error,
        classify_reqwest_error, classify_sqlx_error, DtError, DtErrorContext, DtErrorContextExt,
        EndpointRole, ErrorCode, OriginError, SqlxProvider, Stage,
    };
    use rdkafka::error::KafkaError;

    pub(crate) fn scope(error: anyhow::Error) -> anyhow::Error {
        error
            .with_stage(Stage::Sinker)
            .with_endpoint(EndpointRole::Destination)
    }

    pub(crate) fn mysql(error: sqlx::Error, default_code: ErrorCode) -> anyhow::Error {
        let context = classify_sqlx_error(&error, SqlxProvider::MySql).into_context();
        error.with_code(default_code).with_context(context)
    }
    pub(crate) fn postgres(error: sqlx::Error, default_code: ErrorCode) -> anyhow::Error {
        let context = classify_sqlx_error(&error, SqlxProvider::Postgres).into_context();
        error.with_code(default_code).with_context(context)
    }
    pub(crate) fn reqwest(error: reqwest::Error) -> anyhow::Error {
        let context = classify_reqwest_error(&error).into_context();
        error
            .with_code(ErrorCode::StatementFailed)
            .with_context(context)
    }
    pub(crate) fn http_status(status: reqwest::StatusCode) -> anyhow::Error {
        let detail = format!(
            "the destination rejected the request with HTTP status {}",
            status.as_u16()
        );
        DtError::HttpError(detail)
            .with_code(ErrorCode::StatementFailed)
            .with_origin(OriginError::new("http", Some(status.as_u16().to_string())))
    }
    pub(crate) fn http_rejected(status: reqwest::StatusCode) -> anyhow::Error {
        DtError::HttpError("the destination rejected the data load request".to_string())
            .with_code(ErrorCode::StatementFailed)
            .with_origin(OriginError::new("http", Some(status.as_u16().to_string())))
    }
    pub(crate) fn http_invalid_response(error: serde_json::Error) -> anyhow::Error {
        DtErrorContext::new()
            .code(ErrorCode::StatementFailed)
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
    }
    pub(crate) fn redis_driver_error(
        error: redis::RedisError,
        default_code: ErrorCode,
    ) -> anyhow::Error {
        let context = classify_redis_error(&error).into_context();
        error.with_code(default_code).with_context(context)
    }
    pub(crate) fn redis_slot_topology() -> anyhow::Error {
        let detail = "A required Redis cluster slot has no master owner in the loaded topology";
        DtError::RedisResultError(detail.to_string())
            .with_code(ErrorCode::PrerequisiteNotMet)
            .with_origin(OriginError::new("redis", None::<String>))
            .with_message("The Redis cluster slot map is incomplete or changed")
            .with_hint(
                "Ensure all Redis cluster slots are assigned and stable, then restart the task.",
            )
    }
    pub(crate) fn mongodb_struct(error: mongodb::error::Error) -> anyhow::Error {
        let context = classify_mongodb_error(&error).into_context();
        error
            .with_code(ErrorCode::StatementFailed)
            .with_context(context)
    }
    pub(crate) fn clickhouse_source_metadata_missing() -> anyhow::Error {
        let detail = "Ape-DTS could not determine the source table definition needed to build the ClickHouse table";
        DtError::SinkerError(detail.to_string())
            .with_code(ErrorCode::ObjectNotFound)
            .with_message(
                "Source table metadata is unavailable for ClickHouse structure migration",
            )
            .with_hint(
                "Verify that the source table still exists and rerun structure migration. If it repeats, contact support with the task ID and error code.",
            )
            .with_origin(OriginError::new("clickhouse", None::<String>))
    }
    pub(crate) fn rdkafka(error: KafkaError, default_code: ErrorCode) -> anyhow::Error {
        let context = classify_rdkafka_error(&error).into_context();
        error.with_code(default_code).with_context(context)
    }
    pub(crate) fn kafka(error: ::kafka::Error, default_code: ErrorCode) -> anyhow::Error {
        let context = classify_kafka_error(&error).into_context();
        error.with_code(default_code).with_context(context)
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
            .with_stage(Stage::Checker)
            .context(detail.into())
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
            .stage(Stage::Bootstrap)
            .attach(error)
            .context(detail.into())
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use dt_common::error::{
        AnyhowErrorExt, DtErrorContextExt, EndpointRole, ErrorCode, SqlxProvider, Stage,
    };
    use mysql_binlog_connector_rust::binlog_error::BinlogError;

    use super::extractor_error;

    #[test]
    fn checkpoint_provider_defers_scope_to_recovery_and_task_boundaries() {
        let error = extractor_error::checkpoint_sqlx(
            sqlx::Error::PoolTimedOut,
            SqlxProvider::Postgres,
            "apecloud_metadata",
            "apedts_task_position",
        );
        let context = error.dt_context().unwrap();
        assert_eq!(context.error_code(), Some(ErrorCode::ConnectionTimeout));
        assert_eq!(context.stage_value(), None);
        assert_eq!(context.endpoint_role(), None);
        assert_eq!(context.task_id_value(), None);

        let error = error
            .with_stage(Stage::Resumer)
            .with_endpoint(EndpointRole::Metadata)
            .with_task_id("task-42");
        let context = error.dt_context().unwrap();
        assert_eq!(context.stage_value(), Some(Stage::Resumer));
        assert_eq!(context.endpoint_role(), Some(EndpointRole::Metadata));
        assert_eq!(context.task_id_value(), Some("task-42"));
    }

    #[test]
    fn mysql_binlog_read_classifies_transport_config_and_purged_errors() {
        let timeout = extractor_error::mysql_binlog_read(BinlogError::IoError(io::Error::new(
            io::ErrorKind::TimedOut,
            "read timed out",
        )));
        assert_eq!(
            timeout
                .dt_context()
                .and_then(|context| context.error_code()),
            Some(ErrorCode::ConnectionTimeout)
        );
        assert!(matches!(
            timeout.downcast_ref::<BinlogError>(),
            Some(BinlogError::IoError(_))
        ));

        let reset = extractor_error::mysql_binlog_read(BinlogError::IoError(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "connection reset",
        )));
        assert_eq!(
            reset.dt_context().and_then(|context| context.error_code()),
            Some(ErrorCode::ConnectionFailed)
        );

        let connect = extractor_error::mysql_binlog_read(BinlogError::ConnectError(
            "connection closed".to_string(),
        ));
        assert_eq!(
            connect
                .dt_context()
                .and_then(|context| context.error_code()),
            Some(ErrorCode::ConnectionFailed)
        );

        let invalid_gtid =
            extractor_error::mysql_binlog_read(BinlogError::InvalidGtid("bad-gtid".to_string()));
        assert_eq!(
            invalid_gtid
                .dt_context()
                .and_then(|context| context.error_code()),
            Some(ErrorCode::InvalidConfig)
        );

        let decode = extractor_error::mysql_binlog_read(BinlogError::UnexpectedData(
            "invalid event payload".to_string(),
        ));
        assert_eq!(
            decode.dt_context().and_then(|context| context.error_code()),
            Some(ErrorCode::StatementFailed)
        );

        let purged = extractor_error::mysql_binlog_read(BinlogError::ConnectError(
            "connect mysql failed: Could not find first log file name in binary log index file"
                .to_string(),
        ));
        let context = purged.dt_context().expect("structured error context");
        assert_eq!(context.error_code(), Some(ErrorCode::CheckpointReadFailed));
        assert_eq!(
            context
                .origin_error()
                .and_then(|origin| origin.code.as_deref()),
            Some("1236")
        );
        assert!(context
            .hint_text()
            .is_some_and(|hint| hint.contains("new snapshot")));
    }
}
