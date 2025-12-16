use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};

#[derive(Debug, Clone, PartialEq, Display, EnumString, IntoStaticStr, Serialize, Deserialize)]
pub enum RowType {
    #[strum(serialize = "insert")]
    Insert,
    #[strum(serialize = "update")]
    Update,
    #[strum(serialize = "delete")]
    Delete,
}

#[derive(PartialEq)]
pub enum RowSqlType {
    DML,
    DDL,
    DCL,
}

#[test]
fn test_row_type() {
    assert_eq!(RowType::Insert.to_string(), "insert");
    assert_eq!(RowType::Update.to_string(), "update");
    assert_eq!(RowType::Delete.to_string(), "delete");
}
