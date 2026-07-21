use redis::ErrorKind as RedisErrorKind;

use super::{
    super::{ErrorCode, OriginError},
    ProviderErrorClassification,
};

pub fn classify_redis_error(error: &redis::RedisError) -> ProviderErrorClassification {
    let provider_code = error.code().map(str::to_string);
    let code = if error.is_timeout() {
        Some(ErrorCode::ConnectionTimeout)
    } else {
        match (error.kind(), error.code()) {
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

    ProviderErrorClassification::new(code, OriginError::new("redis", provider_code))
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
            classify_redis_error(&auth).code,
            Some(ErrorCode::AuthenticationFailed)
        );

        let invalid =
            redis::RedisError::from((RedisErrorKind::InvalidClientConfig, "invalid client config"));
        assert_eq!(
            classify_redis_error(&invalid).code,
            Some(ErrorCode::InvalidConfig)
        );

        let timeout = redis::RedisError::from(io::Error::from(io::ErrorKind::TimedOut));
        assert_eq!(
            classify_redis_error(&timeout).code,
            Some(ErrorCode::ConnectionTimeout)
        );

        let unavailable =
            redis::RedisError::from(io::Error::from(io::ErrorKind::ConnectionRefused));
        assert_eq!(
            classify_redis_error(&unavailable).code,
            Some(ErrorCode::ConnectionFailed)
        );
    }
}
