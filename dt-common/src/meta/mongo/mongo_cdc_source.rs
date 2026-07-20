use crate::error::{DtError, ErrorCode, Stage};
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
            _ => Err(DtError::new(ErrorCode::InvalidConfig)
                .detail(format!("invalid MongoCdcSource: {}", str))
                .stage(Stage::Bootstrap)
                .into()),
        }
    }
}
