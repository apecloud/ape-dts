mod classification;
mod http;
mod kafka;
mod mongodb;
mod mysql_binlog;
mod postgres;
mod redis;
mod sqlx;
mod system;

pub use sqlx::classify_sqlx_error;

use std::error::Error as StdError;

use super::{ClassifyError, DtErrorContext};

type RawClassifier = fn(&(dyn StdError + 'static)) -> Option<DtErrorContext>;

const PROVIDER_CLASSIFIERS: &[RawClassifier] = &[
    classify_raw::<::sqlx::Error>,
    classify_raw::<tokio_postgres::Error>,
    classify_raw::<::mongodb::error::Error>,
    classify_raw::<::redis::RedisError>,
    classify_raw::<reqwest::Error>,
    classify_raw::<rdkafka::error::KafkaError>,
    classify_raw::<::kafka::Error>,
    classify_raw::<mysql_binlog_connector_rust::binlog_error::BinlogError>,
];

const SYSTEM_CLASSIFIERS: &[RawClassifier] = &[
    classify_raw::<std::io::Error>,
    classify_raw::<tokio::task::JoinError>,
];

fn classify_raw<E>(error: &(dyn StdError + 'static)) -> Option<DtErrorContext>
where
    E: ClassifyError + StdError + 'static,
{
    error.downcast_ref::<E>().map(ClassifyError::classify)
}

pub(crate) fn classify_raw_errors(
    error: &anyhow::Error,
) -> (Vec<DtErrorContext>, Vec<DtErrorContext>) {
    let mut providers = Vec::new();
    let mut system = Vec::new();
    for cause in error.chain() {
        if let Some(context) = PROVIDER_CLASSIFIERS
            .iter()
            .find_map(|classify| classify(cause))
        {
            providers.push(context);
        } else if let Some(context) = SYSTEM_CLASSIFIERS
            .iter()
            .find_map(|classify| classify(cause))
        {
            system.push(context);
        }
    }
    (providers, system)
}

pub(crate) fn provider_error_detail(error: &(dyn StdError + 'static)) -> Option<String> {
    if let Some(error) = error.downcast_ref::<::sqlx::Error>() {
        return Some(sqlx_detail(error));
    }
    if let Some(error) = error.downcast_ref::<tokio_postgres::Error>() {
        return Some(match error.as_db_error() {
            Some(database_error) => {
                format!("postgres/{}", database_error.code().code())
            }
            None => "postgres".to_string(),
        });
    }
    if let Some(error) = error.downcast_ref::<::mongodb::error::Error>() {
        let code = match error.kind.as_ref() {
            ::mongodb::error::ErrorKind::Command(command) => Some(command.code.to_string()),
            _ => None,
        };
        return Some(provider_and_code("mongodb", code));
    }
    if let Some(error) = error.downcast_ref::<::redis::RedisError>() {
        return Some(provider_and_code("redis", error.code().map(str::to_string)));
    }
    if let Some(error) = error.downcast_ref::<reqwest::Error>() {
        return Some(provider_and_code(
            "http",
            error.status().map(|status| status.as_u16().to_string()),
        ));
    }
    if let Some(error) = error.downcast_ref::<rdkafka::error::KafkaError>() {
        return Some(provider_and_code(
            "kafka",
            error.rdkafka_error_code().map(|code| format!("{code:?}")),
        ));
    }
    if let Some(error) = error.downcast_ref::<::kafka::Error>() {
        return Some(kafka_detail(error));
    }
    if let Some(error) =
        error.downcast_ref::<mysql_binlog_connector_rust::binlog_error::BinlogError>()
    {
        return Some(provider_and_code(
            "mysql",
            mysql_binlog::diagnostic_code(error).map(str::to_string),
        ));
    }
    None
}

fn sqlx_detail(error: &::sqlx::Error) -> String {
    let ::sqlx::Error::Database(database_error) = error else {
        return "sqlx".to_string();
    };
    if database_error
        .try_downcast_ref::<::sqlx::postgres::PgDatabaseError>()
        .is_some()
    {
        provider_and_code(
            "postgres",
            database_error.code().map(|code| code.into_owned()),
        )
    } else if let Some(mysql_error) =
        database_error.try_downcast_ref::<::sqlx::mysql::MySqlDatabaseError>()
    {
        provider_and_code("mysql", Some(mysql_error.number().to_string()))
    } else {
        provider_and_code("sqlx", database_error.code().map(|code| code.into_owned()))
    }
}

fn kafka_detail(error: &::kafka::Error) -> String {
    match error {
        ::kafka::Error::Kafka(code) => provider_and_code("kafka", Some(format!("{code:?}"))),
        ::kafka::Error::TopicPartitionError { error_code, .. } => {
            provider_and_code("kafka", Some(format!("{error_code:?}")))
        }
        ::kafka::Error::ArcSelf(error) => kafka_detail(error),
        _ => "kafka".to_string(),
    }
}

fn provider_and_code(provider: &str, code: Option<String>) -> String {
    match code {
        Some(code) => format!("{provider}/{code}"),
        None => provider.to_string(),
    }
}
