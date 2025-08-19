use anyhow::bail;
use async_std::io::BufReader;
use async_std::net::TcpStream;
use async_std::prelude::*;
use async_trait::async_trait;
use futures::executor::block_on;
use percent_encoding::percent_decode;
use url::Url;

use super::redis_resp_reader::RedisRespReader;
use super::redis_resp_types::Value;
use super::StreamReader;
use dt_common::error::Error;
use dt_common::meta::redis::command::cmd_encoder::CmdEncoder;
use dt_common::meta::redis::redis_object::RedisCmd;

pub struct RedisClient {
    pub url: String,
    stream: BufReader<TcpStream>,
}

#[async_trait]
impl StreamReader for RedisClient {
    async fn read_bytes(&mut self, size: usize) -> anyhow::Result<Vec<u8>> {
        block_on(self.read_bytes(size))
    }
}

impl RedisClient {
    pub async fn new(url: &str) -> anyhow::Result<Self> {
        let url_info = Url::parse(url)?;
        let host = url_info.host_str().unwrap();
        let port = url_info.port().unwrap();
        let username = url_info.username();
        let password = url_info.password();

        let stream = TcpStream::connect(format!("{}:{}", host, port)).await?;
        let mut me = Self {
            url: url.into(),
            stream: BufReader::new(stream),
        };

        if let Some(password) = password {
            let mut cmd = RedisCmd::new();
            cmd.add_str_arg("AUTH");
            if !username.is_empty() {
                let decoded_username = percent_decode(username.as_bytes()).decode_utf8()?;
                cmd.add_str_arg(&decoded_username);
            }

            let decoded_password = percent_decode(password.as_bytes()).decode_utf8()?;
            cmd.add_str_arg(&decoded_password);

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
}

