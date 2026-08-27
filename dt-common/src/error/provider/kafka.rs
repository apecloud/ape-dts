use std::io::ErrorKind;

use ::kafka::error::KafkaCode;
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
            | RdKafkaError::Nul(_)
            | RdKafkaError::Subscription(_) => Some(ErrorCode::InvalidConfig),
            RdKafkaError::Canceled => Some(ErrorCode::OperationInterrupted),
            RdKafkaError::NoMessageReceived => Some(ErrorCode::DatabaseOperationTimeout),
            _ => provider_code.and_then(classify_rdkafka_code),
        };
        let detail_code = provider_code.map(|code| format!("{code:?}"));
        provider_context(
            Some(code.unwrap_or(ErrorCode::DatabaseOperationFailed)),
            provider_detail("kafka", detail_code, self),
        )
    }
}

impl ClassifyError for ::kafka::Error {
    fn classify(&self) -> DtErrorContext {
        provider_context(
            Some(classify_kafka_kind(self).unwrap_or(ErrorCode::DatabaseOperationFailed)),
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
        RDKafkaErrorCode::Authentication
        | RDKafkaErrorCode::SaslAuthenticationFailed
        | RDKafkaErrorCode::UnacceptableCredential
        | RDKafkaErrorCode::DelegationTokenExpired => Some(ErrorCode::AuthenticationFailed),
        RDKafkaErrorCode::TopicAuthorizationFailed
        | RDKafkaErrorCode::GroupAuthorizationFailed
        | RDKafkaErrorCode::ClusterAuthorizationFailed
        | RDKafkaErrorCode::TransactionalIdAuthorizationFailed
        | RDKafkaErrorCode::DelegationTokenOwnerMismatch
        | RDKafkaErrorCode::DelegationTokenRequestNotAllowed
        | RDKafkaErrorCode::DelegationTokenAuthorizationFailed => Some(ErrorCode::PermissionDenied),
        RDKafkaErrorCode::UnknownTopic
        | RDKafkaErrorCode::UnknownPartition
        | RDKafkaErrorCode::UnknownTopicOrPartition
        | RDKafkaErrorCode::NoEnt
        | RDKafkaErrorCode::DelegationTokenNotFound
        | RDKafkaErrorCode::GroupIdNotFound
        | RDKafkaErrorCode::ListenerNotFound
        | RDKafkaErrorCode::ResourceNotFound
        | RDKafkaErrorCode::UnknownTopicId
        | RDKafkaErrorCode::UnknownGroup
        | RDKafkaErrorCode::LogDirNotFound => Some(ErrorCode::ObjectNotFound),
        RDKafkaErrorCode::OffsetOutOfRange
        | RDKafkaErrorCode::NoOffset
        | RDKafkaErrorCode::AutoOffsetReset
        | RDKafkaErrorCode::LogTruncation => Some(ErrorCode::CheckpointReadFailed),
        RDKafkaErrorCode::MessageTimedOut
        | RDKafkaErrorCode::OperationTimedOut
        | RDKafkaErrorCode::TimedOutQueue
        | RDKafkaErrorCode::PollExceeded
        | RDKafkaErrorCode::RequestTimedOut => Some(ErrorCode::DatabaseOperationTimeout),
        RDKafkaErrorCode::BrokerDestroy
        | RDKafkaErrorCode::BrokerTransportFailure
        | RDKafkaErrorCode::Resolve
        | RDKafkaErrorCode::AllBrokersDown
        | RDKafkaErrorCode::BrokerNotAvailable
        | RDKafkaErrorCode::NetworkException
        | RDKafkaErrorCode::UnknownBroker
        | RDKafkaErrorCode::DestroyBroker
        | RDKafkaErrorCode::RebootstrapRequired
        | RDKafkaErrorCode::LeaderNotAvailable
        | RDKafkaErrorCode::NotLeaderForPartition
        | RDKafkaErrorCode::ReplicaNotAvailable
        | RDKafkaErrorCode::CoordinatorLoadInProgress
        | RDKafkaErrorCode::CoordinatorNotAvailable
        | RDKafkaErrorCode::NotCoordinator
        | RDKafkaErrorCode::WaitingForCoordinator
        | RDKafkaErrorCode::PreferredLeaderNotAvailable
        | RDKafkaErrorCode::EligibleLeadersNotAvailable => Some(ErrorCode::ConnectionFailed),
        RDKafkaErrorCode::SSL => Some(ErrorCode::TlsFailed),
        RDKafkaErrorCode::FileSystem | RDKafkaErrorCode::KafkaStorageError => {
            Some(ErrorCode::IoFailed)
        }
        RDKafkaErrorCode::ReadOnly => Some(ErrorCode::PermissionDenied),
        RDKafkaErrorCode::InvalidArgument
        | RDKafkaErrorCode::InvalidTopic
        | RDKafkaErrorCode::InvalidGroupId
        | RDKafkaErrorCode::InvalidConfig
        | RDKafkaErrorCode::InvalidRequiredAcks
        | RDKafkaErrorCode::InvalidSessionTimeout
        | RDKafkaErrorCode::InvalidPartitions
        | RDKafkaErrorCode::InvalidReplicationFactor
        | RDKafkaErrorCode::InvalidReplicaAssignment
        | RDKafkaErrorCode::InvalidTransactionTimeout
        | RDKafkaErrorCode::InvalidPrincipalType
        | RDKafkaErrorCode::NotConfigured
        | RDKafkaErrorCode::ExistingSubscription
        | RDKafkaErrorCode::UnsupportedSASLMechanism => Some(ErrorCode::InvalidConfig),
        RDKafkaErrorCode::UnknownProtocol
        | RDKafkaErrorCode::NotImplemented
        | RDKafkaErrorCode::UnsupportedFeature
        | RDKafkaErrorCode::UnsupportedVersion
        | RDKafkaErrorCode::UnsupportedForMessageFormat
        | RDKafkaErrorCode::SecurityDisabled
        | RDKafkaErrorCode::DelegationTokenAuthDisabled
        | RDKafkaErrorCode::TopicDeletionDisabled
        | RDKafkaErrorCode::UnsupportedCompressionType
        | RDKafkaErrorCode::UnsupportedAssignor => Some(ErrorCode::PrerequisiteNotMet),
        RDKafkaErrorCode::CriticalSystemResource
        | RDKafkaErrorCode::QueueFull
        | RDKafkaErrorCode::ISRInsufficient
        | RDKafkaErrorCode::NotEnoughReplicas
        | RDKafkaErrorCode::NotEnoughReplicasAfterAppend
        | RDKafkaErrorCode::GroupMaxSizeReached
        | RDKafkaErrorCode::ThrottlingQuotaExceeded => Some(ErrorCode::ResourceExhausted),
        RDKafkaErrorCode::Conflict
        | RDKafkaErrorCode::Fenced
        | RDKafkaErrorCode::InProgress
        | RDKafkaErrorCode::PreviousInProgress
        | RDKafkaErrorCode::TopicAlreadyExists
        | RDKafkaErrorCode::ReassignmentInProgress
        | RDKafkaErrorCode::RebalanceInProgress
        | RDKafkaErrorCode::OutOfOrderSequenceNumber
        | RDKafkaErrorCode::DuplicateSequenceNumber
        | RDKafkaErrorCode::InvalidProducerEpoch
        | RDKafkaErrorCode::InvalidTransactionalState
        | RDKafkaErrorCode::InvalidProducerIdMapping
        | RDKafkaErrorCode::ConcurrentTransactions
        | RDKafkaErrorCode::TransactionCoordinatorFenced
        | RDKafkaErrorCode::UnknownProducerId
        | RDKafkaErrorCode::FencedInstanceId
        | RDKafkaErrorCode::ProducerFenced
        | RDKafkaErrorCode::FencedMemberEpoch
        | RDKafkaErrorCode::UnreleasedInstanceId
        | RDKafkaErrorCode::StaleMemberEpoch => Some(ErrorCode::DatabaseOperationConflict),
        RDKafkaErrorCode::Interrupted
        | RDKafkaErrorCode::PurgeQueue
        | RDKafkaErrorCode::PurgeInflight
        | RDKafkaErrorCode::AssignmentLost => Some(ErrorCode::OperationInterrupted),
        _ => None,
    }
}

fn classify_kafka_kind(error: &::kafka::Error) -> Option<ErrorCode> {
    match error {
        ::kafka::Error::Io(error) if is_timeout(error.kind()) => Some(ErrorCode::ConnectionTimeout),
        ::kafka::Error::Io(_) | ::kafka::Error::NoHostReachable => {
            Some(ErrorCode::ConnectionFailed)
        }
        ::kafka::Error::Ssl(_) => Some(ErrorCode::TlsFailed),
        ::kafka::Error::UnexpectedEOF => Some(ErrorCode::ConnectionFailed),
        ::kafka::Error::UnsupportedProtocol | ::kafka::Error::UnsupportedCompression => {
            Some(ErrorCode::PrerequisiteNotMet)
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

fn classify_kafka_code(code: KafkaCode) -> Option<ErrorCode> {
    match code {
        KafkaCode::TopicAuthorizationFailed
        | KafkaCode::GroupAuthorizationFailed
        | KafkaCode::ClusterAuthorizationFailed => Some(ErrorCode::PermissionDenied),
        KafkaCode::UnknownTopicOrPartition => Some(ErrorCode::ObjectNotFound),
        KafkaCode::OffsetOutOfRange => Some(ErrorCode::CheckpointReadFailed),
        KafkaCode::RequestTimedOut => Some(ErrorCode::DatabaseOperationTimeout),
        KafkaCode::LeaderNotAvailable
        | KafkaCode::NotLeaderForPartition
        | KafkaCode::BrokerNotAvailable
        | KafkaCode::ReplicaNotAvailable
        | KafkaCode::NetworkException
        | KafkaCode::GroupLoadInProgress
        | KafkaCode::GroupCoordinatorNotAvailable
        | KafkaCode::NotCoordinatorForGroup => Some(ErrorCode::ConnectionFailed),
        KafkaCode::NotEnoughReplicas | KafkaCode::NotEnoughReplicasAfterAppend => {
            Some(ErrorCode::ResourceExhausted)
        }
        KafkaCode::InvalidTopic
        | KafkaCode::InvalidGroupId
        | KafkaCode::InvalidSessionTimeout
        | KafkaCode::InvalidRequiredAcks
        | KafkaCode::UnsupportedSaslMechanism => Some(ErrorCode::InvalidConfig),
        KafkaCode::UnsupportedVersion => Some(ErrorCode::PrerequisiteNotMet),
        _ => None,
    }
}

fn is_timeout(kind: ErrorKind) -> bool {
    matches!(kind, ErrorKind::TimedOut | ErrorKind::WouldBlock)
}
