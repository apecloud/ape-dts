use serde::{Deserialize, Serialize};
use serde_json::json;

use super::statement::struct_statement::StructStatement;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructData {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub db: String,
    pub schema: String,
    #[serde(default)]
    pub tb: String,
    #[serde(skip)]
    pub statement: StructStatement,
}

impl std::fmt::Display for StructData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}
