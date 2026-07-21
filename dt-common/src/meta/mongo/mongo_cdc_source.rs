use crate::error::{DtError, DtErrorContextExt, ErrorCode, Stage};
use strum::IntoStaticStr;

#[derive(Clone, IntoStaticStr, Debug)]
pub enum MongoCdcSource {
    #[strum(serialize = "op_log")]
    OpLog,

    #[strum(serialize = "change_stream")]
    ChangeStream,
}

impl MongoCdcSource {
    pub fn parse(str: &str) -> anyhow::Result<Self> {
        match str.to_ascii_lowercase().as_str() {
            "op_log" => Ok(Self::OpLog),
            "change_stream" => Ok(Self::ChangeStream),
            _ => Err(
                DtError::ConfigError(format!("invalid MongoCdcSource: {}", str))
                    .with_code(ErrorCode::InvalidConfig)
                    .with_stage(Stage::Bootstrap),
            ),
        }
    }
}
