#[derive(Debug, thiserror::Error)]
pub enum DtError {
    #[error("config error: {0}")]
    ConfigError(String),

    #[error("extractor error: {0}")]
    ExtractorError(String),

    #[error("sinker error: {0}")]
    SinkerError(String),

    #[error("{0}")]
    General(String),

    #[error("parse Redis RDB error: {0}")]
    RedisRdbError(String),

    #[error("parse Redis command error: {0}")]
    RedisCmdError(String),

    #[error("parse Redis result error: {0}")]
    RedisResultError(String),

    #[error("metadata error: {0}")]
    MetadataError(String),

    #[error("table structure error: {0}")]
    StructError(String),

    #[error("HTTP error: {0}")]
    HttpError(String),
}
