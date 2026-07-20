use std::collections::HashMap;

use serde::Serialize;
use serde_json::json;

use crate::error::{DtError, ErrorCode, ErrorObject, OriginError};
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
            DtError::new(ErrorCode::MetadataFailed)
                .detail(format!(
                    "column {col} is missing from PostgreSQL table metadata"
                ))
                .operation("get_postgres_column_type")
                .object(ErrorObject {
                    schema: Some(self.basic.schema.clone()),
                    table: Some(self.basic.tb.clone()),
                    column: Some(col.to_string()),
                    ..Default::default()
                })
                .origin(OriginError::new("postgres", None::<String>))
                .into()
        })
    }
}
