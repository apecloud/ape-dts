use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ForeignKey {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub db: String,
    pub schema: String,
    pub tb: String,
    pub col: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub ref_db: String,
    pub ref_schema: String,
    pub ref_tb: String,
    pub ref_col: String,
}
