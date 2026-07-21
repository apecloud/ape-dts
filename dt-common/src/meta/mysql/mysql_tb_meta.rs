use std::collections::HashMap;

use serde::Serialize;
use serde_json::json;

use crate::error::{DtError, DtErrorContextExt, ErrorCode, ErrorObject, OriginError};
use crate::meta::rdb_tb_meta::RdbTbMeta;

use super::mysql_col_type::MysqlColType;

#[derive(Debug, Clone, Default, Serialize)]
pub struct MysqlTbMeta {
    pub basic: RdbTbMeta,
    pub col_type_map: HashMap<String, MysqlColType>,
}

impl std::fmt::Display for MysqlTbMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

impl MysqlTbMeta {
    #[inline(always)]
    pub fn get_col_type(&self, col: &str) -> anyhow::Result<&MysqlColType> {
        self.col_type_map.get(col).ok_or_else(|| {
            DtError::MetadataError(format!(
                "column {col} is missing from the MySQL definition for {}.{}",
                self.basic.schema, self.basic.tb
            ))
                .with_code(ErrorCode::ObjectNotFound)
                .with_message("A required source column was not found in the loaded table definition")
                .with_hint(
                    "Check configured column names and whether the source table changed, then restart the task to reload its definition.",
                )
                .with_object(ErrorObject {
                    schema: Some(self.basic.schema.clone()),
                    table: Some(self.basic.tb.clone()),
                    column: Some(col.to_string()),
                    ..Default::default()
                })
                .with_origin(OriginError::new("mysql", None::<String>))
        })
    }
}
