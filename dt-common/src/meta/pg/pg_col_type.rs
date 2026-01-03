use super::pg_value_type::PgValueType;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PgColType {
    pub value_type: PgValueType,
    pub name: String,
    pub alias: String,
    pub oid: i32,
    pub parent_oid: i32,
    pub element_oid: i32,
    pub category: String,
    pub enum_values: Option<Vec<String>>,
}

impl std::fmt::Display for PgColType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

#[allow(dead_code)]
impl PgColType {
    pub fn is_enum(&self) -> bool {
        "E" == self.category
    }

    pub fn is_array(&self) -> bool {
        "A" == self.category
    }

    pub fn is_user_defined(&self) -> bool {
        "U" == self.category
    }

    pub fn is_integer(&self) -> bool {
        self.value_type.is_integer()
    }
}
