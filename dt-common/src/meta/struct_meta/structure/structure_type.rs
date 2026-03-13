use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};

#[derive(Debug, Clone, PartialEq, Display, EnumString, IntoStaticStr, Serialize, Deserialize)]
pub enum StructureType {
    #[strum(serialize = "database")]
    Database,
    #[strum(serialize = "table")]
    Table,
    #[strum(serialize = "constraint")]
    Constraint,
    #[strum(serialize = "sequence")]
    Sequence,
    #[strum(serialize = "comment")]
    Comment,
    #[strum(serialize = "index")]
    Index,
    // User-Defined Function
    #[strum(serialize = "udf")]
    Udf,
    // User-Defined Type
    #[strum(serialize = "udt")]
    Udt,
    // RBAC migration requires superuser privileges in the source PostgreSQL database
    // to properly extract and migrate role-based access control settings to the target database
    #[strum(serialize = "rbac")]
    Rbac,
    #[strum(serialize = "unknown")]
    Unknown,
}
