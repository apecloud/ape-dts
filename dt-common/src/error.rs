use thiserror::Error;

#[derive(Error, Debug)]
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
