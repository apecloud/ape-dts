use futures::executor::block_on;
use url::Url;

use anyhow::bail;
use async_std::{io::BufReader, net::TcpStream, prelude::*};
use async_trait::async_trait;

use super::{redis_resp_reader::RedisRespReader, redis_resp_types::Value, StreamReader};
use dt_common::{
    config::connection_auth_config::ConnectionAuthConfig,
    error::Error,
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
        let url_info = Url::parse(url)?;
        let host = url_info.host_str().unwrap();
        let port = url_info.port().unwrap();

        let username = Self::extract_username(connection_auth, &url_info)?;
        let password = Self::extract_password(connection_auth, &url_info)?;

        let stream = TcpStream::connect(format!("{}:{}", host, port)).await?;
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
            bail! {Error::RedisResultError(format!(
                "can't connect redis: {}",
                url
            ))}
        }

        Ok(me)
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        self.stream.get_mut().shutdown(std::net::Shutdown::Both)?;
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
        // if length is bigger than buffer size of BufReader, the buf will be filled by 0,
        // so here we must read from inner TcpStream instead of BufReader
        // let n = self.stream.read(&mut buf).await.unwrap();
        let mut read_count = 0;
        while read_count < length {
            // use async_std::net::TcpStream instead of tokio::net::TcpStream, tokio TcpStream may stuck
            // when trying to get big data by multiple read.
            read_count += self.stream.get_mut().read(&mut buf[read_count..]).await?;
        }
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
