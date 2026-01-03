use std::collections::HashMap;

use anyhow::Context;
use serde::Serialize;
use serde_json::json;

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
        let col_type = self
            .col_type_map
            .get(col)
            .with_context(|| format!("col: [{}] not exists in tb_meta: [{}]", col, self))
            .unwrap();
        Ok(col_type)
    }
}
