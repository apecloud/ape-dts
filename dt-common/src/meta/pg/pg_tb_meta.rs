use std::collections::HashMap;

use serde::Serialize;
use serde_json::json;

use crate::error::{DtError, DtErrorContextExt, ErrorObject, OriginError};
use crate::meta::rdb_tb_meta::RdbTbMeta;

use super::pg_col_type::PgColType;

#[derive(Debug, Clone, Default, Serialize)]
pub struct PgTbMeta {
    pub basic: RdbTbMeta,
    pub oid: i32,
    pub col_type_map: HashMap<String, PgColType>,
}

impl std::fmt::Display for PgTbMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

impl PgTbMeta {
    #[inline(always)]
    pub fn get_col_type(&self, col: &str) -> anyhow::Result<&PgColType> {
        self.col_type_map.get(col).ok_or_else(|| {
            DtError::ObjectNotFound(format!(
                "column {col} is missing from the PostgreSQL definition for {}.{}",
                self.basic.schema, self.basic.tb
            ))
                .message("A required source column was not found in the loaded table definition")
                .hint(
                    "Check configured column names and whether the source table changed, then restart the task to reload its definition.",
                )
                .object(ErrorObject {
                    schema: Some(self.basic.schema.clone()),
                    table: Some(self.basic.tb.clone()),
                    column: Some(col.to_string()),
                    ..Default::default()
                })
                .origin(OriginError::new("postgres", None::<String>))
        })
    }
}
