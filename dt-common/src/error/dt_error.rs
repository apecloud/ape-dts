#[derive(Debug, thiserror::Error)]
pub enum DtError {
    #[error("config error: {0}")]
    ConfigError(String),

    #[error("extractor error: {0}")]
    ExtractorError(String),

    #[error("sinker error: {0}")]
    SinkerError(String),

    #[error("unexpected internal error: {0}")]
    Unexpected(String),

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

impl DtError {
    pub(crate) fn detail(&self) -> &str {
        match self {
            Self::ConfigError(detail)
            | Self::ExtractorError(detail)
            | Self::SinkerError(detail)
            | Self::Unexpected(detail)
            | Self::RedisRdbError(detail)
            | Self::RedisCmdError(detail)
            | Self::RedisResultError(detail)
            | Self::MetadataError(detail)
            | Self::StructError(detail)
            | Self::HttpError(detail) => detail,
        }
    }
}
