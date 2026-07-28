use rdkafka::error::{KafkaError as RdKafkaError, RDKafkaErrorCode};

use super::{
    super::{ClassifyError, DtErrorContext, ErrorCode},
    classification::{provider_context, provider_detail},
};

impl ClassifyError for RdKafkaError {
    fn classify(&self) -> DtErrorContext {
        let provider_code = self.rdkafka_error_code();
        let code = match self {
            RdKafkaError::ClientConfig(..)
            | RdKafkaError::ClientCreation(_)
            | RdKafkaError::Nul(_)
            | RdKafkaError::SetPartitionOffset(_)
            | RdKafkaError::Subscription(_) => Some(ErrorCode::InvalidConfig),
            _ => provider_code.and_then(classify_rdkafka_code),
        };
        let detail_code = provider_code.map(|code| format!("{code:?}"));
        provider_context(code, provider_detail("kafka", detail_code, self))
    }
}

impl ClassifyError for ::kafka::Error {
    fn classify(&self) -> DtErrorContext {
        provider_context(
            classify_kafka_kind(self),
            provider_detail("kafka", kafka_provider_code(self), self),
        )
    }
}

fn kafka_provider_code(error: &::kafka::Error) -> Option<String> {
    match error {
        ::kafka::Error::Kafka(code) => Some(format!("{code:?}")),
        ::kafka::Error::TopicPartitionError { error_code, .. } => Some(format!("{error_code:?}")),
        ::kafka::Error::ArcSelf(error) => kafka_provider_code(error),
        _ => None,
    }
}

fn classify_rdkafka_code(code: RDKafkaErrorCode) -> Option<ErrorCode> {
    match code {
        RDKafkaErrorCode::Authentication | RDKafkaErrorCode::SaslAuthenticationFailed => {
            Some(ErrorCode::AuthenticationFailed)
        }
        RDKafkaErrorCode::TopicAuthorizationFailed
        | RDKafkaErrorCode::GroupAuthorizationFailed
        | RDKafkaErrorCode::ClusterAuthorizationFailed
        | RDKafkaErrorCode::TransactionalIdAuthorizationFailed => Some(ErrorCode::PermissionDenied),
        RDKafkaErrorCode::UnknownTopic
        | RDKafkaErrorCode::UnknownPartition
        | RDKafkaErrorCode::UnknownTopicOrPartition => Some(ErrorCode::ObjectNotFound),
        RDKafkaErrorCode::MessageTimedOut
        | RDKafkaErrorCode::OperationTimedOut
        | RDKafkaErrorCode::TimedOutQueue
        | RDKafkaErrorCode::RequestTimedOut => Some(ErrorCode::ConnectionTimeout),
        RDKafkaErrorCode::BrokerDestroy
        | RDKafkaErrorCode::BrokerTransportFailure
        | RDKafkaErrorCode::Resolve
        | RDKafkaErrorCode::AllBrokersDown
        | RDKafkaErrorCode::BrokerNotAvailable
        | RDKafkaErrorCode::NetworkException => Some(ErrorCode::ConnectionFailed),
        RDKafkaErrorCode::SSL => Some(ErrorCode::TlsFailed),
        RDKafkaErrorCode::InvalidArgument
        | RDKafkaErrorCode::InvalidTopic
        | RDKafkaErrorCode::InvalidGroupId
        | RDKafkaErrorCode::InvalidConfig => Some(ErrorCode::InvalidConfig),
        _ => None,
    }
}

fn classify_kafka_kind(error: &::kafka::Error) -> Option<ErrorCode> {
    match error {
        ::kafka::Error::Io(error) if is_timeout(error.kind()) => Some(ErrorCode::ConnectionTimeout),
        ::kafka::Error::Io(_) | ::kafka::Error::NoHostReachable => {
            Some(ErrorCode::ConnectionFailed)
        }
        ::kafka::Error::NoTopicsAssigned
        | ::kafka::Error::InvalidDuration
        | ::kafka::Error::UnsetOffsetStorage
        | ::kafka::Error::UnsetGroupId => Some(ErrorCode::InvalidConfig),
        ::kafka::Error::Kafka(code) => classify_kafka_code(*code),
        ::kafka::Error::TopicPartitionError { error_code, .. } => classify_kafka_code(*error_code),
        ::kafka::Error::ArcSelf(error) => classify_kafka_kind(error),
        _ => None,
    }
}

fn classify_kafka_code(code: ::kafka::error::KafkaCode) -> Option<ErrorCode> {
    use ::kafka::error::KafkaCode;

    match code {
        KafkaCode::TopicAuthorizationFailed
        | KafkaCode::GroupAuthorizationFailed
        | KafkaCode::ClusterAuthorizationFailed => Some(ErrorCode::PermissionDenied),
        KafkaCode::UnknownTopicOrPartition => Some(ErrorCode::ObjectNotFound),
        KafkaCode::RequestTimedOut => Some(ErrorCode::ConnectionTimeout),
        KafkaCode::BrokerNotAvailable | KafkaCode::NetworkException => {
            Some(ErrorCode::ConnectionFailed)
        }
        KafkaCode::InvalidTopic
        | KafkaCode::InvalidGroupId
        | KafkaCode::InvalidSessionTimeout
        | KafkaCode::InvalidRequiredAcks => Some(ErrorCode::InvalidConfig),
        _ => None,
    }
}

fn is_timeout(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    )
}
