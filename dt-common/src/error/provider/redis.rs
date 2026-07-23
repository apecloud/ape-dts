use redis::ErrorKind as RedisErrorKind;

use super::{
    super::{ClassifyError, DtErrorContext, ErrorCode, OriginError},
    classification::provider_context,
};

impl ClassifyError for redis::RedisError {
    fn classify(&self) -> DtErrorContext {
        let provider_code = self.code().map(str::to_string);
        let code = if self.is_timeout() {
            Some(ErrorCode::ConnectionTimeout)
        } else {
            match (self.kind(), self.code()) {
                (RedisErrorKind::AuthenticationFailed, _) | (_, Some("NOAUTH" | "WRONGPASS")) => {
                    Some(ErrorCode::AuthenticationFailed)
                }
                (RedisErrorKind::ReadOnly, _) | (_, Some("NOPERM")) => {
                    Some(ErrorCode::PermissionDenied)
                }
                (RedisErrorKind::InvalidClientConfig, _) => Some(ErrorCode::InvalidConfig),
                (
                    RedisErrorKind::IoError
                    | RedisErrorKind::ClusterDown
                    | RedisErrorKind::MasterDown
                    | RedisErrorKind::ClusterConnectionNotFound
                    | RedisErrorKind::NoValidReplicasFoundBySentinel,
                    _,
                ) => Some(ErrorCode::ConnectionFailed),
                _ => None,
            }
        };

        provider_context(code, OriginError::new("redis", provider_code))
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    #[test]
    fn classifies_redis_errors() {
        for (error, expected) in [
            (
                redis::RedisError::from((
                    RedisErrorKind::AuthenticationFailed,
                    "authentication failed",
                )),
                ErrorCode::AuthenticationFailed,
            ),
            (
                redis::RedisError::from((
                    RedisErrorKind::InvalidClientConfig,
                    "invalid client config",
                )),
                ErrorCode::InvalidConfig,
            ),
            (
                redis::RedisError::from(io::Error::from(io::ErrorKind::TimedOut)),
                ErrorCode::ConnectionTimeout,
            ),
            (
                redis::RedisError::from(io::Error::from(io::ErrorKind::ConnectionRefused)),
                ErrorCode::ConnectionFailed,
            ),
        ] {
            assert_eq!(error.classify().error_code(), Some(expected));
        }
    }
}
