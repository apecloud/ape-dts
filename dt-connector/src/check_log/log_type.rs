use serde::{Deserialize, Serialize};
use strum::{EnumString, IntoStaticStr};

#[derive(Debug, EnumString, IntoStaticStr, PartialEq, Serialize, Deserialize, Clone)]
pub enum LogType {
    #[strum(serialize = "miss")]
    Miss,
    #[strum(serialize = "diff")]
    Diff,
    #[strum(serialize = "unknown")]
    Unknown,
}
