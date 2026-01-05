use std::collections::HashMap;

use anyhow::Context;
use serde::Serialize;
use serde_json::json;

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
        let col_type = self
            .col_type_map
            .get(col)
            .with_context(|| format!("col: [{}] not exists in tb_meta: [{}]", col, self))
            .unwrap();
        Ok(col_type)
    }
}
