use std::{io::ErrorKind, net::Shutdown};

use anyhow::{bail, Context, Error};
use async_std::{io::BufReader, net::TcpStream, prelude::*};
use async_trait::async_trait;
use futures::executor::block_on;
use url::Url;

use super::{redis_resp_reader::RedisRespReader, redis_resp_types::Value, StreamReader};
use dt_common::{
    config::{config_enums::DbType, connection_auth_config::ConnectionAuthConfig},
    error::{DtError, DtOptionExt},
    meta::redis::{command::cmd_encoder::CmdEncoder, redis_object::RedisCmd},
};

pub struct RedisClient {
    pub url: String,
    pub connection_auth: ConnectionAuthConfig,
    stream: BufReader<TcpStream>,
}

#[async_trait]
impl StreamReader for RedisClient {
    async fn read_bytes(&mut self, size: usize) -> anyhow::Result<Vec<u8>> {
        block_on(self.read_bytes(size))
    }
}

impl RedisClient {
    pub async fn new(url: &str, connection_auth: &ConnectionAuthConfig) -> anyhow::Result<Self> {
        let url_info = Url::parse(url).context(DtError::DatabaseInvalidConfig(
            DbType::Redis,
            "source Redis URL is invalid".to_string(),
        ))?;
        let host = url_info
            .host_str()
            .or_dt_error(DtError::DatabaseInvalidConfig(
                DbType::Redis,
                "the source Redis URL must include a host".to_string(),
            ))?;
        let port = url_info.port().unwrap_or(6379);

        let username = Self::extract_username(connection_auth, &url_info)?;
        let password = Self::extract_password(connection_auth, &url_info)?;

        let stream = TcpStream::connect(format!("{}:{}", host, port))
            .await
            .map_err(|error| {
                let context = if matches!(error.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock)
                {
                    DtError::DatabaseConnectionTimeout(
                        DbType::Redis,
                        "failed to connect to Redis".to_string(),
                    )
                } else {
                    DtError::DatabaseConnectionFailed(
                        DbType::Redis,
                        "failed to connect to Redis".to_string(),
                    )
                };
                Error::new(error).context(context)
            })?;
        let mut me = Self {
            url: url.into(),
            connection_auth: connection_auth.clone(),
            stream: BufReader::new(stream),
        };

        if let Some(pwd) = password {
            let mut cmd = RedisCmd::new();
            cmd.add_str_arg("AUTH");
            if !username.is_empty() {
                cmd.add_str_arg(&username);
            }
            cmd.add_str_arg(&pwd);

            me.send(&cmd).await?;
            if let Ok(Value::Okay) = me.read().await {
                return Ok(me);
            }
            return Err(DtError::DatabaseAuthenticationFailed(
                DbType::Redis,
                "Redis authentication failed".to_string(),
            )
            .into());
        }

        Ok(me)
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        self.stream.get_mut().shutdown(Shutdown::Both).context(
            DtError::DatabaseConnectionFailed(
                DbType::Redis,
                "failed to close the Redis connection".to_string(),
            ),
        )?;
        Ok(())
    }

    pub async fn send_packed(&mut self, packed_cmd: &[u8]) -> anyhow::Result<()> {
        self.stream.get_mut().write_all(packed_cmd).await.context(
            DtError::DatabaseConnectionFailed(
                DbType::Redis,
                "failed to write to the Redis connection".to_string(),
            ),
        )?;
        Ok(())
    }

    pub async fn send(&mut self, cmd: &RedisCmd) -> anyhow::Result<()> {
        self.send_packed(&CmdEncoder::encode(cmd)).await
    }

    pub async fn read(&mut self) -> anyhow::Result<Value> {
        let mut resp_reader = RedisRespReader { read_len: 0 };
        resp_reader.decode(&mut self.stream).await
    }

    pub async fn read_as_string(&mut self) -> anyhow::Result<Vec<String>> {
        let value = self.read().await?;
        Self::parse_result_as_string(value)
    }

    pub async fn read_with_len(&mut self) -> anyhow::Result<(Value, usize)> {
        let mut resp_reader = RedisRespReader { read_len: 0 };
        let value = resp_reader.decode(&mut self.stream).await?;
        Ok((value, resp_reader.read_len))
    }

    pub async fn read_bytes(&mut self, length: usize) -> anyhow::Result<Vec<u8>> {
        let mut buf = vec![0; length];
        self.stream
            .read_exact(&mut buf)
            .await
            .context(DtError::DatabaseConnectionFailed(
                DbType::Redis,
                "failed to read from the Redis connection".to_string(),
            ))?;
        Ok(buf)
    }

    fn parse_result_as_string(value: Value) -> anyhow::Result<Vec<String>> {
        let mut results = Vec::new();
        match value {
            Value::Data(data) => {
                results.push(String::from_utf8_lossy(&data).to_string());
            }

            Value::Bulk(data) => {
                for i in data {
                    let sub_results = Self::parse_result_as_string(i)?;
                    results.extend_from_slice(&sub_results);
                }
            }

            Value::Int(data) => results.push(data.to_string()),

            Value::Status(data) => results.push(data),

            _ => {
                bail!(DtError::RedisResultError(
                    "Redis response cannot be converted to strings".to_string()
                ))
            }
        }
        Ok(results)
    }

    fn decode_url_component(component: &str, field_name: &str) -> anyhow::Result<String> {
        percent_encoding::percent_decode_str(component)
            .decode_utf8()
            .map(|s| s.to_string())
            .context(DtError::DatabaseInvalidConfig(
                DbType::Redis,
                format!("failed to decode Redis URL {field_name}"),
            ))
    }

    fn extract_username<'a>(
        connection_auth: &'a ConnectionAuthConfig,
        url_info: &'a Url,
    ) -> anyhow::Result<String> {
        match connection_auth {
            ConnectionAuthConfig::Basic { username, .. } => Ok(username.clone()),
            _ => {
                let usr_in_url = url_info.username();
                if usr_in_url.is_empty() {
                    Ok(String::new())
                } else {
                    Self::decode_url_component(usr_in_url, "username")
                }
            }
        }
    }

    fn extract_password(
        connection_auth: &ConnectionAuthConfig,
        url_info: &Url,
    ) -> anyhow::Result<Option<String>> {
        match connection_auth {
            ConnectionAuthConfig::Basic {
                password: Some(password),
                ..
            } => Ok(Some(password.clone())),
            _ => url_info
                .password()
                .map(|pwd| Self::decode_url_component(pwd, "password"))
                .transpose(),
        }
    }
}
