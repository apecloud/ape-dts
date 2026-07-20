use std::{convert::Infallible, str::FromStr};

use strum::IntoStaticStr;

#[derive(Clone, IntoStaticStr, Debug)]
pub enum RedisWriteMethod {
    #[strum(serialize = "restore")]
    Restore,

    #[strum(serialize = "rewrite")]
    Rewrite,
}

impl FromStr for RedisWriteMethod {
    type Err = Infallible;
    fn from_str(str: &str) -> Result<Self, Self::Err> {
        match str {
            "rewrite" => Ok(Self::Rewrite),
            _ => Ok(Self::Restore),
        }
    }
}
