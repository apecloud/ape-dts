use dt_common::error::{DtError, EndpointRole, ErrorCode, OriginError, Stage};
use rdkafka::error::{KafkaError as RdKafkaError, RDKafkaErrorCode};

#[track_caller]
pub fn rdkafka(
    error: RdKafkaError,
    fallback: ErrorCode,
    stage: Stage,
    endpoint: EndpointRole,
    operation: &'static str,
) -> DtError {
    let provider_code = error.rdkafka_error_code();
    let code = match &error {
        RdKafkaError::ClientConfig(..)
        | RdKafkaError::ClientCreation(_)
        | RdKafkaError::Nul(_)
        | RdKafkaError::SetPartitionOffset(_)
        | RdKafkaError::Subscription(_) => ErrorCode::InvalidConfig,
        _ => provider_code
            .map(|code| classify_rdkafka_code(code, fallback))
            .unwrap_or(fallback),
    };
    DtError::new(code)
        .stage(stage)
        .operation(operation)
        .endpoint(endpoint)
        .origin(OriginError::new(
            "kafka",
            provider_code.map(|code| format!("{code:?}")),
        ))
        .source(error)
}

#[track_caller]
pub fn kafka(
    error: ::kafka::Error,
    fallback: ErrorCode,
    stage: Stage,
    endpoint: EndpointRole,
    operation: &'static str,
) -> DtError {
    let (code, provider_code) = classify_kafka_error(&error, fallback);
    DtError::new(code)
        .stage(stage)
        .operation(operation)
        .endpoint(endpoint)
        .origin(OriginError::new("kafka", provider_code))
        .source(error)
}

fn classify_rdkafka_code(code: RDKafkaErrorCode, fallback: ErrorCode) -> ErrorCode {
    match code {
        RDKafkaErrorCode::Authentication | RDKafkaErrorCode::SaslAuthenticationFailed => {
            ErrorCode::AuthenticationFailed
        }
        RDKafkaErrorCode::TopicAuthorizationFailed
        | RDKafkaErrorCode::GroupAuthorizationFailed
        | RDKafkaErrorCode::ClusterAuthorizationFailed
        | RDKafkaErrorCode::TransactionalIdAuthorizationFailed => ErrorCode::PermissionDenied,
        RDKafkaErrorCode::UnknownTopic
        | RDKafkaErrorCode::UnknownPartition
        | RDKafkaErrorCode::UnknownTopicOrPartition => ErrorCode::ObjectNotFound,
        RDKafkaErrorCode::MessageTimedOut
        | RDKafkaErrorCode::OperationTimedOut
        | RDKafkaErrorCode::TimedOutQueue
        | RDKafkaErrorCode::RequestTimedOut => ErrorCode::ConnectionTimeout,
        RDKafkaErrorCode::BrokerDestroy
        | RDKafkaErrorCode::BrokerTransportFailure
        | RDKafkaErrorCode::Resolve
        | RDKafkaErrorCode::AllBrokersDown
        | RDKafkaErrorCode::BrokerNotAvailable
        | RDKafkaErrorCode::NetworkException => ErrorCode::ConnectionFailed,
        RDKafkaErrorCode::SSL => ErrorCode::TlsFailed,
        RDKafkaErrorCode::InvalidArgument
        | RDKafkaErrorCode::InvalidTopic
        | RDKafkaErrorCode::InvalidGroupId
        | RDKafkaErrorCode::InvalidConfig => ErrorCode::InvalidConfig,
        _ => fallback,
    }
}

fn classify_kafka_error(
    error: &::kafka::Error,
    fallback: ErrorCode,
) -> (ErrorCode, Option<String>) {
    match error {
        ::kafka::Error::Io(error) if is_timeout(error.kind()) => {
            (ErrorCode::ConnectionTimeout, None)
        }
        ::kafka::Error::Io(_) | ::kafka::Error::NoHostReachable => {
            (ErrorCode::ConnectionFailed, None)
        }
        ::kafka::Error::NoTopicsAssigned
        | ::kafka::Error::InvalidDuration
        | ::kafka::Error::UnsetOffsetStorage
        | ::kafka::Error::UnsetGroupId => (ErrorCode::InvalidConfig, None),
        ::kafka::Error::Kafka(code) => (
            classify_kafka_code(*code, fallback),
            Some(format!("{code:?}")),
        ),
        ::kafka::Error::TopicPartitionError { error_code, .. } => (
            classify_kafka_code(*error_code, fallback),
            Some(format!("{error_code:?}")),
        ),
        ::kafka::Error::ArcSelf(error) => classify_kafka_error(error, fallback),
        _ => (fallback, None),
    }
}

fn classify_kafka_code(code: ::kafka::error::KafkaCode, fallback: ErrorCode) -> ErrorCode {
    use ::kafka::error::KafkaCode;

    match code {
        KafkaCode::TopicAuthorizationFailed
        | KafkaCode::GroupAuthorizationFailed
        | KafkaCode::ClusterAuthorizationFailed => ErrorCode::PermissionDenied,
        KafkaCode::UnknownTopicOrPartition => ErrorCode::ObjectNotFound,
        KafkaCode::RequestTimedOut => ErrorCode::ConnectionTimeout,
        KafkaCode::BrokerNotAvailable | KafkaCode::NetworkException => ErrorCode::ConnectionFailed,
        KafkaCode::InvalidTopic
        | KafkaCode::InvalidGroupId
        | KafkaCode::InvalidSessionTimeout
        | KafkaCode::InvalidRequiredAcks => ErrorCode::InvalidConfig,
        _ => fallback,
    }
}

fn is_timeout(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_rdkafka_provider_codes() {
        assert_eq!(
            classify_rdkafka_code(
                RDKafkaErrorCode::SaslAuthenticationFailed,
                ErrorCode::StatementFailed,
            ),
            ErrorCode::AuthenticationFailed
        );
        assert_eq!(
            classify_rdkafka_code(
                RDKafkaErrorCode::TopicAuthorizationFailed,
                ErrorCode::StatementFailed,
            ),
            ErrorCode::PermissionDenied
        );
        assert_eq!(
            classify_rdkafka_code(
                RDKafkaErrorCode::UnknownTopicOrPartition,
                ErrorCode::StatementFailed,
            ),
            ErrorCode::ObjectNotFound
        );
    }
}
