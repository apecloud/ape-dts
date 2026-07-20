mod code;
mod context;
mod dt_error;
pub mod provider;
mod report;

pub use code::ErrorCode;
pub use context::{EndpointRole, ErrorObject, OriginError, Stage};
pub use dt_error::{BoxError, DtError};
pub use provider::{
    classify_kafka_error, classify_mongodb_error, classify_rdkafka_error, classify_redis_error,
    classify_reqwest_error, classify_sqlx_error, classify_tokio_postgres_error,
    dt_error_from_kafka, dt_error_from_mongodb, dt_error_from_rdkafka, dt_error_from_redis,
    dt_error_from_reqwest, dt_error_from_sqlx, dt_error_from_tokio_postgres,
    try_dt_error_from_anyhow_sqlx, ProviderErrorClassification, SqlxProvider,
};
pub use report::{ErrorReport, ERROR_REPORT_SCHEMA_VERSION};
