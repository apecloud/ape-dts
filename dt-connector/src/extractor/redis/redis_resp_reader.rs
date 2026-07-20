use anyhow::{bail, Ok};
use async_recursion::async_recursion;
use async_std::io::BufReader;
use async_std::net::TcpStream;
use async_std::prelude::*;
use dt_common::error::ErrorCode;

use super::redis_resp_types::Value;
use crate::error_boundary::extractor::redis_source as redis_protocol_error;

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
            bail! {redis_protocol_error(
                ErrorCode::StatementFailed,
                format!("Redis response line is too short: {len}"),
                "decode_redis_response",
            )}
        }
        if !is_crlf(res[len - 2], res[len - 1]) {
            bail! {redis_protocol_error(
                ErrorCode::StatementFailed,
                format!("Redis response has invalid CRLF: {res:?}"),
                "decode_redis_response",
            )}
        }

        let bytes = res[1..len - 2].as_ref();
        match res[0] {
            // Value::String
            b'+' => match bytes {
                OK_RESPONSE => Ok(Value::Okay),
                bytes => Ok(Value::Status(String::from_utf8(bytes.to_vec()).map_err(
                    |error| {
                        redis_protocol_error(
                            ErrorCode::StatementFailed,
                            "Redis status response is not valid UTF-8",
                            "decode_redis_response",
                        )
                        .source(error)
                    },
                )?)),
            },
            // Value::Error
            b'-' => {
                let message = String::from_utf8_lossy(bytes);
                bail! {redis_protocol_error(
                    ErrorCode::StatementFailed,
                    format!("Redis server rejected the command: {message}"),
                    "decode_redis_response",
                )}
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
                    bail! {redis_protocol_error(
                        ErrorCode::StatementFailed,
                        format!("Redis response has invalid bulk length: {int}"),
                        "decode_redis_response",
                    )}
                }

                let int = int as usize;
                let mut buf: Vec<u8> = vec![0; int + 2];
                reader.read_exact(buf.as_mut_slice()).await?;
                if !is_crlf(buf[int], buf[int + 1]) {
                    bail! {redis_protocol_error(
                        ErrorCode::StatementFailed,
                        format!("Redis response has invalid CRLF: {buf:?}"),
                        "decode_redis_response",
                    )}
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
                    bail! {redis_protocol_error(
                        ErrorCode::StatementFailed,
                        format!("Redis response has invalid array length: {int}"),
                        "decode_redis_response",
                    )}
                }

                let mut array: Vec<Value> = Vec::with_capacity(int as usize);
                for _ in 0..int {
                    let val = self.decode(reader).await?;
                    array.push(val);
                }
                Ok(Value::Bulk(array))
            }
            prefix => bail!(redis_protocol_error(
                ErrorCode::StatementFailed,
                format!("invalid Redis RESP type: {prefix:?}"),
                "decode_redis_response",
            )),
        }
    }
}

#[inline]
fn is_crlf(a: u8, b: u8) -> bool {
    a == b'\r' && b == b'\n'
}

#[inline]
fn parse_integer(bytes: &[u8]) -> anyhow::Result<i64> {
    let value = String::from_utf8(bytes.to_vec()).map_err(|error| {
        redis_protocol_error(
            ErrorCode::StatementFailed,
            "Redis integer response is not valid UTF-8",
            "decode_redis_response",
        )
        .source(error)
    })?;
    value.parse::<i64>().map_err(|error| {
        redis_protocol_error(
            ErrorCode::StatementFailed,
            "Redis integer response is invalid",
            "decode_redis_response",
        )
        .source(error)
        .into()
    })
}
