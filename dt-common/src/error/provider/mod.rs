mod classification;
mod http;
mod kafka;
mod mongodb;
mod postgres;
mod redis;
mod sqlx;

pub use classification::ProviderErrorClassification;
pub use http::classify_reqwest_error;
pub use kafka::{classify_kafka_error, classify_rdkafka_error};
pub use mongodb::classify_mongodb_error;
pub use postgres::classify_tokio_postgres_error;
pub use redis::classify_redis_error;
pub use sqlx::{classify_sqlx_error, SqlxProvider};
