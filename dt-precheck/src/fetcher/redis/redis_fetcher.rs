use async_trait::async_trait;
use dt_common::{
    config::connection_auth_config::ConnectionAuthConfig,
    error::{DtError, EndpointRole, ErrorCode, OriginError, Stage},
    rdb_filter::RdbFilter,
    utils::redis_util::RedisUtil,
};

use crate::fetcher::traits::Fetcher;

pub struct RedisFetcher {
    pub url: String,
    pub connection_auth: ConnectionAuthConfig,
    pub conn: Option<redis::Connection>,
    pub is_source: bool,
    pub filter: RdbFilter,
}

#[async_trait]
impl Fetcher for RedisFetcher {
    async fn build_connection(&mut self) -> anyhow::Result<()> {
        self.conn = Some(RedisUtil::create_redis_conn(&self.url, &self.connection_auth).await?);
        Ok(())
    }

    async fn fetch_version(&mut self) -> anyhow::Result<String> {
        let conn = self.conn.as_mut().ok_or_else(|| {
            DtError::new(ErrorCode::InvariantViolated)
                .detail("the Redis precheck connection is not initialized")
                .stage(Stage::Precheck)
                .operation("fetch_redis_version")
                .endpoint(if self.is_source {
                    EndpointRole::Source
                } else {
                    EndpointRole::Destination
                })
                .origin(OriginError::new("redis", None::<String>))
        })?;
        let version = RedisUtil::get_redis_version(conn)?;
        Ok(version.to_string())
    }
}

impl RedisFetcher {}
