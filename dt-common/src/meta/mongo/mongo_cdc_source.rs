use crate::error::Error;
use strum::IntoStaticStr;

#[derive(Clone, IntoStaticStr, Debug)]
pub enum MongoCdcSource {
    #[strum(serialize = "op_log")]
    OpLog,

    #[strum(serialize = "change_stream")]
    ChangeStream,
}

impl MongoCdcSource {
    pub fn parse(str: &str) -> Result<Self, Error> {
        match str.to_ascii_lowercase().as_str() {
            "op_log" => Ok(Self::OpLog),
            "change_stream" => Ok(Self::ChangeStream),
            _ => Err(Error::ConfigError(format!(
                "invalid MongoCdcSource: {}",
                str
            ))),
        }
    }
}
