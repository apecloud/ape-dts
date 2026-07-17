use dt_common::error::{
    dt_error_from_kafka, dt_error_from_rdkafka, DtError, EndpointRole, ErrorCode, Stage,
};
use rdkafka::error::KafkaError as RdKafkaError;

#[track_caller]
pub fn rdkafka(
    error: RdKafkaError,
    fallback: ErrorCode,
    stage: Stage,
    endpoint: EndpointRole,
    operation: &'static str,
) -> DtError {
    dt_error_from_rdkafka(error, fallback)
        .stage(stage)
        .operation(operation)
        .endpoint(endpoint)
}

#[track_caller]
pub fn kafka(
    error: ::kafka::Error,
    fallback: ErrorCode,
    stage: Stage,
    endpoint: EndpointRole,
    operation: &'static str,
) -> DtError {
    dt_error_from_kafka(error, fallback)
        .stage(stage)
        .operation(operation)
        .endpoint(endpoint)
}
