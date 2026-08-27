use redis::ErrorKind as RedisErrorKind;
use redis::RedisError;

use super::{
    super::{ClassifyError, DtErrorContext, ErrorCode},
    classification::{provider_context, provider_detail},
};

impl ClassifyError for RedisError {
    fn classify(&self) -> DtErrorContext {
        let code = if self.is_timeout() {
            Some(ErrorCode::ConnectionTimeout)
        } else {
            match (self.kind(), self.code()) {
                (RedisErrorKind::AuthenticationFailed, _) | (_, Some("NOAUTH" | "WRONGPASS")) => {
                    Some(ErrorCode::AuthenticationFailed)
                }
                (_, Some("NOPERM")) => Some(ErrorCode::PermissionDenied),
                (RedisErrorKind::ReadOnly, _) => Some(ErrorCode::PrerequisiteNotMet),
                (RedisErrorKind::InvalidClientConfig | RedisErrorKind::EmptySentinelList, _) => {
                    Some(ErrorCode::InvalidConfig)
                }
                (
                    RedisErrorKind::NoScriptError | RedisErrorKind::MasterNameNotFoundBySentinel,
                    _,
                ) => Some(ErrorCode::ObjectNotFound),
                (RedisErrorKind::RESP3NotSupported, _) => Some(ErrorCode::PrerequisiteNotMet),
                (
                    RedisErrorKind::IoError
                    | RedisErrorKind::Moved
                    | RedisErrorKind::Ask
                    | RedisErrorKind::ClusterDown
                    | RedisErrorKind::MasterDown
                    | RedisErrorKind::ClusterConnectionNotFound
                    | RedisErrorKind::NoValidReplicasFoundBySentinel,
                    _,
                ) => Some(ErrorCode::ConnectionFailed),
                _ => None,
            }
        };

        provider_context(
            Some(code.unwrap_or(ErrorCode::DatabaseOperationFailed)),
            provider_detail("redis", self.code().map(str::to_string), self),
        )
    }
}
