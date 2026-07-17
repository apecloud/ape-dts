use redis::ErrorKind as RedisErrorKind;

use super::{
    super::{DtError, ErrorCode, OriginError},
    ProviderErrorClassification,
};

pub fn classify_redis_error(
    error: &redis::RedisError,
    fallback: ErrorCode,
) -> ProviderErrorClassification {
    let provider_code = error.code().map(str::to_string);
    let code = if error.is_timeout() {
        ErrorCode::ConnectionTimeout
    } else {
        match (error.kind(), error.code()) {
            (RedisErrorKind::AuthenticationFailed, _) | (_, Some("NOAUTH" | "WRONGPASS")) => {
                ErrorCode::AuthenticationFailed
            }
            (RedisErrorKind::ReadOnly, _) | (_, Some("NOPERM")) => ErrorCode::PermissionDenied,
            (RedisErrorKind::InvalidClientConfig, _) => ErrorCode::InvalidConfig,
            (
                RedisErrorKind::IoError
                | RedisErrorKind::ClusterDown
                | RedisErrorKind::MasterDown
                | RedisErrorKind::ClusterConnectionNotFound
                | RedisErrorKind::NoValidReplicasFoundBySentinel,
                _,
            ) => ErrorCode::ConnectionFailed,
            _ => fallback,
        }
    };

    ProviderErrorClassification::new(code, OriginError::new("redis", provider_code))
}

#[track_caller]
pub fn dt_error_from_redis(error: redis::RedisError, fallback: ErrorCode) -> DtError {
    let classification = classify_redis_error(&error, fallback);
    DtError::new(classification.code)
        .origin(classification.origin)
        .source(error)
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    #[test]
    fn classifies_redis_errors() {
        let auth = redis::RedisError::from((
            RedisErrorKind::AuthenticationFailed,
            "authentication failed",
        ));
        assert_eq!(
            classify_redis_error(&auth, ErrorCode::StatementFailed).code,
            ErrorCode::AuthenticationFailed
        );

        let invalid =
            redis::RedisError::from((RedisErrorKind::InvalidClientConfig, "invalid client config"));
        assert_eq!(
            classify_redis_error(&invalid, ErrorCode::StatementFailed).code,
            ErrorCode::InvalidConfig
        );

        let timeout = redis::RedisError::from(io::Error::from(io::ErrorKind::TimedOut));
        assert_eq!(
            classify_redis_error(&timeout, ErrorCode::StatementFailed).code,
            ErrorCode::ConnectionTimeout
        );

        let unavailable =
            redis::RedisError::from(io::Error::from(io::ErrorKind::ConnectionRefused));
        assert_eq!(
            classify_redis_error(&unavailable, ErrorCode::StatementFailed).code,
            ErrorCode::ConnectionFailed
        );
    }
}
