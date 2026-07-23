use anyhow::{bail, Context};
use dt_common::error::DtError;

/// Represents a redis RESP protocol response
/// https://redis.io/topics/protocol#resp-protocol-description
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum Value {
    /// A nil response from the server.
    Nil,
    /// A status response which represents the string "OK".
    Okay,
    /// An integer response.  Note that there are a few situations
    /// in which redis actually returns a string for an integer.
    Int(i64),
    /// A simple string response.
    Status(String),
    /// An arbitrary binary data.
    Data(Vec<u8>),
    /// A bulk response of more data.  This is generally used by redis
    /// to express nested structures.
    Bulk(Vec<Value>),
}

impl Value {
    pub fn try_into<T: ParseFrom<Self>>(self) -> anyhow::Result<T> {
        T::parse_from(self)
    }
}

pub trait ParseFrom<T>: Sized {
    fn parse_from(value: T) -> anyhow::Result<Self>;
}

impl ParseFrom<Value> for () {
    fn parse_from(value: Value) -> anyhow::Result<Self> {
        match value {
            Value::Okay => Ok(()),
            v => bail!(DtError::RedisResultError(format!(
                "failed to parse Redis response: {v:?}"
            ))),
        }
    }
}

impl ParseFrom<Value> for i64 {
    fn parse_from(value: Value) -> anyhow::Result<Self> {
        match value {
            Value::Int(n) => Ok(n),
            v => bail!(DtError::RedisResultError(format!(
                "failed to parse Redis response: {v:?}"
            ))),
        }
    }
}

impl ParseFrom<Value> for Vec<u8> {
    fn parse_from(value: Value) -> anyhow::Result<Self> {
        match value {
            Value::Data(bytes) => Ok(bytes),
            v => bail!(DtError::RedisResultError(format!(
                "failed to parse Redis response: {v:?}"
            ))),
        }
    }
}

impl ParseFrom<Value> for String {
    fn parse_from(value: Value) -> anyhow::Result<Self> {
        match value {
            Value::Okay => Ok("Ok".to_owned()),
            Value::Nil => Ok(String::new()),
            Value::Int(n) => Ok(format!("{}", n)),
            Value::Status(s) => Ok(s),
            Value::Data(bytes) => String::from_utf8(bytes).context(DtError::RedisResultError(
                "Redis string response is not valid UTF-8".to_string(),
            )),
            v => bail!(DtError::RedisResultError(format!(
                "failed to parse Redis response: {v:?}"
            ))),
        }
    }
}

impl<T> ParseFrom<Value> for Vec<T>
where
    T: ParseFrom<Value>,
{
    fn parse_from(v: Value) -> anyhow::Result<Self> {
        if let Value::Bulk(array) = v {
            let mut result = Vec::with_capacity(array.len());
            for e in array {
                result.push(T::parse_from(e)?);
            }
            return Ok(result);
        }
        bail!(DtError::RedisResultError(format!(
            "failed to parse Redis response: {v:?}"
        )))
    }
}
