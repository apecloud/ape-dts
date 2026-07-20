use futures::executor::block_on;
use url::Url;

use anyhow::bail;
use async_std::{io::BufReader, net::TcpStream, prelude::*};
use async_trait::async_trait;

use super::{redis_resp_reader::RedisRespReader, redis_resp_types::Value, StreamReader};
use crate::error::extractor::{
    redis_invalid_config as invalid_config, redis_io, redis_source as redis_source_error,
};
use dt_common::{
    config::connection_auth_config::ConnectionAuthConfig,
    error::{DtError, EndpointRole, ErrorCode, Stage},
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
        let url_info = Url::parse(url).map_err(|error| {
            DtError::new(ErrorCode::InvalidConfig)
                .stage(Stage::Extractor)
                .operation("parse_redis_url")
                .endpoint(EndpointRole::Source)
                .source(error)
        })?;
        let host = url_info.host_str().ok_or_else(|| {
            DtError::new(ErrorCode::InvalidConfig)
                .stage(Stage::Extractor)
                .operation("parse_redis_url")
                .endpoint(EndpointRole::Source)
                .detail("the source Redis URL must include a host")
        })?;
        let port = url_info.port().unwrap_or(6379);

        let username = Self::extract_username(connection_auth, &url_info)?;
        let password = Self::extract_password(connection_auth, &url_info)?;

        let stream = TcpStream::connect(format!("{}:{}", host, port))
            .await
            .map_err(|error| redis_io(error, "connect_redis"))?;
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
            return Err(DtError::new(ErrorCode::AuthenticationFailed)
                .stage(Stage::Extractor)
                .operation("authenticate_redis")
                .endpoint(EndpointRole::Source)
                .into());
        }

        Ok(me)
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        self.stream
            .get_mut()
            .shutdown(std::net::Shutdown::Both)
            .map_err(|error| redis_io(error, "close_redis_connection"))?;
        Ok(())
    }

    pub async fn send_packed(&mut self, packed_cmd: &[u8]) -> anyhow::Result<()> {
        self.stream
            .get_mut()
            .write_all(packed_cmd)
            .await
            .map_err(|error| redis_io(error, "write_redis_command"))?;
        Ok(())
    }

    pub async fn send(&mut self, cmd: &RedisCmd) -> anyhow::Result<()> {
        self.send_packed(&CmdEncoder::encode(cmd)).await
    }

    pub async fn read(&mut self) -> anyhow::Result<Value> {
        let mut resp_reader = RedisRespReader { read_len: 0 };
        match resp_reader.decode(&mut self.stream).await {
            Ok(value) => Ok(value),
            Err(error) => bail! {redis_source_error(
                ErrorCode::StatementFailed,
                error.to_string(),
                "decode_redis_response",
            ).source(error)},
        }
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
            .map_err(|error| redis_io(error, "read_redis_response"))?;
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
                bail! {redis_source_error(
                    ErrorCode::StatementFailed,
                    "Redis response cannot be converted to strings",
                    "parse_redis_response",
                )}
            }
        }
        Ok(results)
    }

    fn decode_url_component(component: &str, field_name: &str) -> anyhow::Result<String> {
        Ok(percent_encoding::percent_decode_str(component)
            .decode_utf8()
            .map(|s| s.to_string())
            .map_err(|error| {
                invalid_config(
                    format!("failed to decode Redis URL {field_name}: {error}"),
                    "decode_redis_url_component",
                )
            })?)
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
