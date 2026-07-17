mod classification;
mod http;
mod kafka;
mod mongodb;
mod postgres;
mod redis;
mod sqlx;

pub use classification::ProviderErrorClassification;
pub use http::{classify_reqwest_error, dt_error_from_reqwest};
pub use kafka::{
    classify_kafka_error, classify_rdkafka_error, dt_error_from_kafka, dt_error_from_rdkafka,
};
pub use mongodb::{classify_mongodb_error, dt_error_from_mongodb};
pub use postgres::{classify_tokio_postgres_error, dt_error_from_tokio_postgres};
pub use redis::{classify_redis_error, dt_error_from_redis};
pub use sqlx::{
    classify_sqlx_error, dt_error_from_sqlx, try_dt_error_from_anyhow_sqlx, SqlxProvider,
};

pub type ExternalErrorClassification = ProviderErrorClassification;
pub type SqlxErrorClassification = ProviderErrorClassification;
