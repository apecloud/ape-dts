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
    try_dt_error_from_anyhow_sqlx, ExternalErrorClassification, ProviderErrorClassification,
    SqlxErrorClassification, SqlxProvider,
};
pub use report::{DiagnosticReport, ErrorReport, ERROR_REPORT_SCHEMA_VERSION};

use thiserror::Error as ThisError;

/// Legacy application error. New error sites should create [`DtError`] instead.
#[derive(ThisError, Debug)]
pub enum Error {
    #[error("config error: {0}")]
    ConfigError(String),

    #[error("extractor error: {0}")]
    ExtractorError(String),

    #[error("pipeline error: {0}")]
    PipelineError(String),

    #[error("sinker error: {0}")]
    SinkerError(String),

    #[error("heartbeat error: {0}")]
    HeartbeatError(String),

    #[error("pull mysql binlog error: {0}")]
    BinlogError(#[from] mysql_binlog_connector_rust::binlog_error::BinlogError),

    #[error("sqlx error: {0}")]
    SqlxError(#[from] sqlx::Error),

    #[error("unexpected error: {0}")]
    Unexpected(String),

    #[error("parse redis rdb error: {0}")]
    RedisRdbError(String),

    #[error("parse redis cmd error: {0}")]
    RedisCmdError(String),

    #[error("parse redis result error: {0}")]
    RedisResultError(String),

    #[error("metadata error: {0}")]
    MetadataError(String),

    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("yaml error: {0}")]
    YamlError(#[from] serde_yaml::Error),

    #[error("from utf8 error: {0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),

    #[error("mongodb error: {0}")]
    MongodbError(#[from] mongodb::error::Error),

    #[error("struct error: {0}")]
    StructError(String),

    #[error("avro encode error: {0}")]
    AvroEncodeError(#[from] apache_avro::Error),

    #[error("enum parse error: {0}")]
    EnumParseError(#[from] strum::ParseError),

    #[error("http request error: {0}")]
    HttpError(String),

    #[error("data marker error: {0}")]
    DataMarkerError(String),

    #[error("mlua error: {0}")]
    MluaError(#[from] mlua::Error),
}

impl Error {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::ConfigError(_) => ErrorCode::InvalidConfig,
            Self::MetadataError(_) => ErrorCode::MetadataFailed,
            Self::IoError(_) => ErrorCode::IoFailed,
            Self::YamlError(_) => ErrorCode::InvalidConfig,
            Self::MongodbError(error) => {
                classify_mongodb_error(error, ErrorCode::StatementFailed).code
            }
            Self::StructError(_) => ErrorCode::MetadataFailed,
            Self::HttpError(_) => ErrorCode::StatementFailed,
            Self::Unexpected(_) => ErrorCode::InvariantViolated,
            Self::SqlxError(error) => {
                classify_sqlx_error(error, SqlxProvider::Unknown, ErrorCode::StatementFailed).code
            }
            _ => ErrorCode::Unclassified,
        }
    }

    pub fn stage(&self) -> Stage {
        match self {
            Self::ConfigError(_) | Self::YamlError(_) => Stage::Bootstrap,
            Self::ExtractorError(_) | Self::BinlogError(_) => Stage::Extractor,
            Self::PipelineError(_) => Stage::Pipeline,
            Self::SinkerError(_) | Self::HttpError(_) => Stage::Sinker,
            _ => Stage::Unknown,
        }
    }
}
