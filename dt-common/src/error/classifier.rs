use std::error::Error as StdError;
use std::io::Error;

use mysql_binlog_connector_rust::binlog_error::BinlogError;
use redis::RedisError;
use tokio::task::JoinError;

use super::{
    error_context::{DtErrorContexts, DT_ERROR_CONTEXT_MARKER},
    DtError, DtErrorContext, ErrorCode,
};

pub trait ClassifyError {
    fn classify(&self) -> DtErrorContext;
}

type ChainClassifier = fn(&(dyn StdError + 'static)) -> Option<DtErrorContext>;

const CHAIN_CLASSIFIERS: &[ChainClassifier] = &[
    classify_chain::<sqlx::Error>,
    classify_chain::<tokio_postgres::Error>,
    classify_chain::<mongodb::error::Error>,
    classify_chain::<RedisError>,
    classify_chain::<reqwest::Error>,
    classify_chain::<rdkafka::error::KafkaError>,
    classify_chain::<kafka::Error>,
    classify_chain::<BinlogError>,
    classify_chain::<Error>,
    classify_chain::<JoinError>,
];

fn classify_chain<E>(error: &(dyn StdError + 'static)) -> Option<DtErrorContext>
where
    E: ClassifyError + StdError + 'static,
{
    error.downcast_ref::<E>().map(ClassifyError::classify)
}

fn classify_chain_error(error: &(dyn StdError + 'static)) -> Option<DtErrorContext> {
    CHAIN_CLASSIFIERS
        .iter()
        .find_map(|classify| classify(error))
}

pub(crate) fn collect_contexts(error: &anyhow::Error) -> Vec<DtErrorContext> {
    let mut result = Vec::new();
    let mut concrete_code_found = false;

    if let Some(contexts) = error.downcast_ref::<DtErrorContexts>() {
        result.extend(contexts.iter_outer_to_inner().cloned());
    }

    if let Some(error) = error.downcast_ref::<DtError>() {
        let mut context = error.classify();
        if context.code == Some(ErrorCode::Unclassified) {
            context.code = None;
        } else if context.code.is_some() {
            concrete_code_found = true;
        }
        result.push(context);
    }

    for cause in error.chain() {
        let detail = cause.to_string();
        if detail == DT_ERROR_CONTEXT_MARKER {
            continue;
        }
        if let Some(mut context) = classify_chain_error(cause) {
            // A classified wrapper owns the condition; nested sources still enrich diagnostics.
            if context.code == Some(ErrorCode::Unclassified) || concrete_code_found {
                context.code = None;
            } else if context.code.is_some() {
                concrete_code_found = true;
            }
            result.push(context);
        } else if !result
            .iter()
            .any(|context| context.detail.as_deref() == Some(detail.as_str()))
        {
            result.push(DtErrorContext::new().with_detail(detail));
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    #[derive(Debug, thiserror::Error)]
    #[error("outer source wrapper")]
    struct SourceWrapper {
        #[source]
        source: Error,
    }

    #[test]
    fn collects_typed_dt_error_and_its_source_chain() {
        let error = anyhow::Error::new(Error::other("source failure"))
            .context(DtError::InvalidConfig("invalid endpoint".to_string()));

        assert!(error.downcast_ref::<DtError>().is_some());
        let contexts = collect_contexts(&error);
        let codes: Vec<_> = contexts.iter().filter_map(|context| context.code).collect();
        let details: Vec<_> = contexts
            .iter()
            .filter_map(|context| context.detail.as_deref())
            .collect();

        assert_eq!(codes, [ErrorCode::InvalidConfig]);
        assert_eq!(details, ["invalid endpoint", "source failure"]);
    }

    #[test]
    fn provider_wrapper_owns_code_while_source_enriches_detail() {
        let error = anyhow::Error::new(sqlx::Error::Io(Error::other("connection reset")));

        let contexts = collect_contexts(&error);
        let codes: Vec<_> = contexts.iter().filter_map(|context| context.code).collect();
        let details: Vec<_> = contexts
            .iter()
            .filter_map(|context| context.detail.as_deref())
            .collect();

        assert_eq!(codes, [ErrorCode::ConnectionFailed]);
        assert_eq!(
            details,
            [
                "sqlx: error communicating with database",
                "connection reset"
            ]
        );
    }

    #[test]
    fn traverses_sources_that_anyhow_downcast_does_not_search() {
        let error = anyhow::Error::new(SourceWrapper {
            source: Error::other("source failure"),
        });

        assert!(error.downcast_ref::<Error>().is_none());
        let contexts = collect_contexts(&error);
        let codes: Vec<_> = contexts.iter().filter_map(|context| context.code).collect();
        let details: Vec<_> = contexts
            .iter()
            .filter_map(|context| context.detail.as_deref())
            .collect();

        assert_eq!(codes, [ErrorCode::IoFailed]);
        assert_eq!(details, ["outer source wrapper", "source failure"]);
    }
}
