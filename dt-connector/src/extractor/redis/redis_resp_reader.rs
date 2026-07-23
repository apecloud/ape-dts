use anyhow::{bail, Context, Ok};
use async_recursion::async_recursion;
use async_std::io::BufReader;
use async_std::net::TcpStream;
use async_std::prelude::*;
use dt_common::error::DtError;

use super::redis_resp_types::Value;

pub struct RedisRespReader {
    pub read_len: usize,
}

/// up to 512 MB in length
const RESP_MAX_SIZE: i64 = 512 * 1024 * 1024;
const OK_RESPONSE: &[u8] = &[79, 75];

impl RedisRespReader {
    #[async_recursion]
    pub async fn decode(&mut self, reader: &mut BufReader<TcpStream>) -> anyhow::Result<Value> {
        let mut res: Vec<u8> = Vec::new();
        reader.read_until(b'\n', &mut res).await?;

        let len = res.len();
        self.read_len += len;

        if len <= 1 {
            return Ok(Value::Nil);
        }
        if len < 3 {
            bail!(DtError::RedisResultError(format!(
                "Redis response line is too short: {len}"
            )))
        }
        if !is_crlf(res[len - 2], res[len - 1]) {
            bail!(DtError::RedisResultError(format!(
                "Redis response has invalid CRLF: {res:?}"
            )))
        }

        let bytes = res[1..len - 2].as_ref();
        match res[0] {
            // Value::String
            b'+' => match bytes {
                OK_RESPONSE => Ok(Value::Okay),
                bytes => Ok(Value::Status(String::from_utf8(bytes.to_vec()).context(
                    DtError::RedisResultError(
                        "Redis status response is not valid UTF-8".to_string(),
                    ),
                )?)),
            },
            // Value::Error
            b'-' => {
                let message = String::from_utf8_lossy(bytes);
                bail!(DtError::RedisResultError(format!(
                    "Redis server rejected the command: {message}"
                )))
            }
            // Value::Integer
            b':' => parse_integer(bytes).map(Value::Int),
            // Value::Bulk
            b'$' => {
                let int: i64 = parse_integer(bytes)?;
                if int == -1 {
                    // Nil bulk
                    return Ok(Value::Nil);
                }
                if int < -1 || int >= RESP_MAX_SIZE {
                    bail!(DtError::RedisResultError(format!(
                        "Redis response has invalid bulk length: {int}"
                    )))
                }

                let int = int as usize;
                let mut buf: Vec<u8> = vec![0; int + 2];
                reader.read_exact(buf.as_mut_slice()).await?;
                if !is_crlf(buf[int], buf[int + 1]) {
                    bail!(DtError::RedisResultError(format!(
                        "Redis response has invalid CRLF: {buf:?}"
                    )))
                }
                self.read_len += int + 2;
                buf.truncate(int);
                Ok(Value::Data(buf))
            }
            // Value::Array
            b'*' => {
                let int = parse_integer(bytes)?;
                if int == -1 {
                    // Null array
                    return Ok(Value::Nil);
                }
                if int < -1 || int >= RESP_MAX_SIZE {
                    bail!(DtError::RedisResultError(format!(
                        "Redis response has invalid array length: {int}"
                    )))
                }

                let mut array: Vec<Value> = Vec::with_capacity(int as usize);
                for _ in 0..int {
                    let val = self.decode(reader).await?;
                    array.push(val);
                }
                Ok(Value::Bulk(array))
            }
            prefix => bail!(DtError::RedisResultError(format!(
                "invalid Redis RESP type: {prefix:?}"
            ))),
        }
    }
}

#[inline]
fn is_crlf(a: u8, b: u8) -> bool {
    a == b'\r' && b == b'\n'
}

#[inline]
fn parse_integer(bytes: &[u8]) -> anyhow::Result<i64> {
    let value = String::from_utf8(bytes.to_vec()).context(DtError::RedisResultError(
        "Redis integer response is not valid UTF-8".to_string(),
    ))?;
    value.parse::<i64>().context(DtError::RedisResultError(
        "Redis integer response is invalid".to_string(),
    ))
}
