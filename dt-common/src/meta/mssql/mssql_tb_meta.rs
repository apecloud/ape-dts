use std::collections::{HashMap, HashSet};

use serde::Serialize;
use serde_json::json;

use crate::{
    config::config_enums::DbType,
    error::{DtError, DtErrorContextExt, ErrorObject},
    meta::rdb_tb_meta::RdbTbMeta,
};

use super::mssql_col_type::MssqlColType;

#[derive(Debug, Clone, Default, Serialize)]
pub struct MssqlTbMeta {
    pub basic: RdbTbMeta,
    pub object_id: i32,
    pub col_type_map: HashMap<String, MssqlColType>,
    pub identity_cols: HashSet<String>,
    pub computed_cols: HashSet<String>,
    pub rowversion_cols: HashSet<String>,
}

impl MssqlTbMeta {
    pub fn get_col_type(&self, col: &str) -> anyhow::Result<&MssqlColType> {
        self.col_type_map.get(col).ok_or_else(|| {
            DtError::DatabaseObjectNotFound(
                DbType::Mssql,
                format!(
                    "column {col} is missing from the MSSQL definition for {}.{}",
                    self.basic.schema, self.basic.tb
                ),
            )
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
        })
    }

    pub fn readable_cols(&self) -> &[String] {
        &self.basic.cols
    }

    pub fn writable_cols(&self) -> Vec<String> {
        self.basic
            .cols
            .iter()
            .filter(|col| {
                !self.computed_cols.contains(*col)
                    && !self.rowversion_cols.contains(*col)
                    && self
                        .col_type_map
                        .get(*col)
                        .is_none_or(|col_type| col_type.generated_always_type == 0)
            })
            .cloned()
            .collect()
    }
}

impl std::fmt::Display for MssqlTbMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writable_cols_exclude_server_generated_values() {
        let mut tb_meta = MssqlTbMeta {
            basic: RdbTbMeta {
                cols: vec![
                    "id".to_string(),
                    "payload".to_string(),
                    "computed_value".to_string(),
                    "row_version".to_string(),
                    "generated_value".to_string(),
                ],
                ..Default::default()
            },
            computed_cols: HashSet::from(["computed_value".to_string()]),
            rowversion_cols: HashSet::from(["row_version".to_string()]),
            ..Default::default()
        };
        tb_meta.col_type_map.insert(
            "generated_value".to_string(),
            MssqlColType {
                generated_always_type: 1,
                ..Default::default()
            },
        );

        assert_eq!(tb_meta.readable_cols(), tb_meta.basic.cols.as_slice());
        assert_eq!(tb_meta.writable_cols(), vec!["id", "payload"]);
    }
}
