use serde::{Deserialize, Serialize};
use strum::{Display, EnumIter, EnumString, IntoStaticStr};

#[derive(
    Debug,
    Clone,
    PartialEq,
    Display,
    EnumString,
    EnumIter,
    IntoStaticStr,
    Serialize,
    Deserialize,
    Eq,
)]
pub enum DclType {
    #[strum(serialize = "create_user")]
    CreateUser,
    #[strum(serialize = "alter_user")]
    AlterUser,
    #[strum(serialize = "create_role")]
    CreateRole,
    #[strum(serialize = "drop_user")]
    DropUser,
    #[strum(serialize = "drop_role")]
    DropRole,
    #[strum(serialize = "grant")]
    Grant,
    #[strum(serialize = "revoke")]
    Revoke,
    #[strum(serialize = "set_role")]
    SetRole,
    #[strum(serialize = "unknown")]
    Unknown,
}

impl Default for DclType {
    fn default() -> Self {
        Self::Unknown
    }
}
