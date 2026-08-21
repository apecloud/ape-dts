use serde::{Deserialize, Serialize};
use serde_json::json;

use super::dcl_type::DclType;
use crate::config::config_enums::DbType;
use crate::meta::dcl_meta::dcl_statement::DclStatement;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DclData {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub default_db: String,
    pub default_schema: String,
    pub dcl_type: DclType,
    pub db_type: DbType,
    pub statement: DclStatement,
}

impl std::fmt::Display for DclData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

impl DclData {
    pub fn to_sql(&self) -> String {
        self.statement.to_sql(&self.db_type)
    }

    pub fn get_data_size(&self) -> u64 {
        self.to_string().len() as u64
    }

    pub fn get_malloc_size(&self) -> u64 {
        let mut size = 0;

        size += self.default_db.len() as u64;
        size += self.default_schema.len() as u64;
        size += std::mem::size_of::<DclType>() as u64;
        size += std::mem::size_of::<DbType>() as u64;
        size += self.statement.get_malloc_size();

        size
    }
}
