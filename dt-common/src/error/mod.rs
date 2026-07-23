mod code;
mod context;
mod dt_error;
mod error_context;
pub mod provider;
mod report;

pub use code::ErrorCode;
pub use context::{EndpointRole, ErrorObject, OriginError, Stage};
pub use dt_error::{classify_dt_error, DtError};
pub use error_context::{AnyhowErrorExt, DtErrorContext, DtErrorContextExt};
pub use provider::{
    classify_kafka_error, classify_mongodb_error, classify_rdkafka_error, classify_redis_error,
    classify_reqwest_error, classify_sqlx_error, classify_tokio_postgres_error,
    ProviderErrorClassification, SqlxProvider,
};
pub use report::{ErrorReport, ERROR_REPORT_SCHEMA_VERSION};
