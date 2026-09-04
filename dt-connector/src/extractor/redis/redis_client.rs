use futures::{executor::block_on, future::Either};
use futures_rustls::{client::TlsStream, TlsConnector};
use url::Url;

use anyhow::{bail, Context};
use async_std::{io::BufReader, net::TcpStream, prelude::*};
use async_trait::async_trait;
use rustls::pki_types::ServerName;
use std::sync::Arc;

use super::{redis_resp_reader::RedisRespReader, redis_resp_types::Value, StreamReader};
use dt_common::{
    config::connection_auth_config::ConnectionAuthConfig,
    error::Error,
    meta::redis::{command::cmd_encoder::CmdEncoder, redis_object::RedisCmd},
    utils::{redis_util::RedisUtil, tls_util::build_tls_client_config},
};

pub struct RedisClient {
    pub url: String,
    pub connection_auth: ConnectionAuthConfig,
    stream: BufReader<Either<TcpStream, TlsStream<TcpStream>>>,
}

#[async_trait]
impl StreamReader for RedisClient {
    async fn read_bytes(&mut self, size: usize) -> anyhow::Result<Vec<u8>> {
        block_on(self.read_bytes(size))
    }
}

impl RedisClient {
    pub async fn new(url: &str, connection_auth: &ConnectionAuthConfig) -> anyhow::Result<Self> {
        let resolved = RedisUtil::resolve_connection_config(url, connection_auth)?;
        let url_info = Url::parse(&resolved.url)?;
        let host = url_info
            .host_str()
            .with_context(|| format!("Redis URL has no host: {}", url))?
            .to_string();
        let port = url_info.port().unwrap_or(6379);

        let username = Self::extract_username(&url_info)?;
        let password = Self::extract_password(&url_info)?;

        let tcp_stream = TcpStream::connect((host.as_str(), port)).await?;
        let stream = if !matches!(
            &resolved.ssl_config.ssl_mode,
            dt_common::config::ssl_config::SslMode::Disable
        ) {
            let server_name = ServerName::try_from(host.clone())
                .with_context(|| format!("invalid Redis TLS server name: {}", host))?;
            let connector =
                TlsConnector::from(Arc::new(build_tls_client_config(&resolved.ssl_config)?));
            Either::Right(
                connector
                    .connect(server_name, tcp_stream)
                    .await
                    .with_context(|| format!("Redis TLS handshake failed: {}", url))?,
            )
        } else {
            Either::Left(tcp_stream)
        };
        let mut me = Self {
            url: resolved.url,
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
            bail! {Error::RedisResultError(format!(
                "can't connect redis: {}",
                url
            ))}
        }

        Ok(me)
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        futures::AsyncWriteExt::close(self.stream.get_mut()).await?;
        Ok(())
    }

    pub async fn send_packed(&mut self, packed_cmd: &[u8]) -> anyhow::Result<()> {
        self.stream.get_mut().write_all(packed_cmd).await?;
        Ok(())
    }

    pub async fn send(&mut self, cmd: &RedisCmd) -> anyhow::Result<()> {
        self.send_packed(&CmdEncoder::encode(cmd)).await
    }

    pub async fn read(&mut self) -> anyhow::Result<Value> {
        let mut resp_reader = RedisRespReader { read_len: 0 };
        match resp_reader.decode(&mut self.stream).await {
            Ok(value) => Ok(value),
            Err(err) => bail! {Error::RedisResultError(err.to_string())},
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
        self.stream.read_exact(&mut buf).await?;
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
                bail! {Error::RedisResultError(
                    "redis result type can not be parsed as string".into(),
                )}
            }
        }
        Ok(results)
    }

    fn decode_url_component(component: &str, field_name: &str) -> anyhow::Result<String> {
        percent_encoding::percent_decode_str(component)
            .decode_utf8()
            .map(|s| s.to_string())
            .map_err(|e| Error::ConfigError(format!("{} parse failed: {}", field_name, e)).into())
    }

    fn extract_username(url_info: &Url) -> anyhow::Result<String> {
        let usr_in_url = url_info.username();
        if usr_in_url.is_empty() {
            Ok(String::new())
        } else {
            Self::decode_url_component(usr_in_url, "username")
        }
    }

    fn extract_password(url_info: &Url) -> anyhow::Result<Option<String>> {
        url_info
            .password()
            .map(|pwd| Self::decode_url_component(pwd, "password"))
            .transpose()
    }
}
