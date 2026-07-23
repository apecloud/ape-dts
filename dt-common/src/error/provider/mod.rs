mod classification;
mod http;
mod kafka;
mod mongodb;
mod postgres;
mod redis;
mod sqlx;

pub use sqlx::{classify_sqlx_error, SqlxProvider};

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
];

fn classify_raw<E>(error: &(dyn StdError + 'static)) -> Option<DtErrorContext>
where
    E: ClassifyError + StdError + 'static,
{
    error.downcast_ref::<E>().map(ClassifyError::classify)
}

pub(crate) fn classify_raw_error(error: &anyhow::Error) -> Option<DtErrorContext> {
    error.chain().find_map(|cause| {
        PROVIDER_CLASSIFIERS
            .iter()
            .find_map(|classify| classify(cause))
    })
}
