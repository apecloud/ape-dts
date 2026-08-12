use std::collections::HashMap;

use serde::{ser::SerializeMap, Serialize, Serializer};
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
    #[serde(serialize_with = "serialize_col_type_map")]
    pub col_type_map: HashMap<String, MssqlColType>,
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
}

fn serialize_col_type_map<S>(
    col_type_map: &HashMap<String, MssqlColType>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut map = serializer.serialize_map(Some(col_type_map.len()))?;
    for (col, col_type) in col_type_map {
        map.serialize_entry(col, &format!("{col_type:?}"))?;
    }
    map.end()
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
    fn serializes_aliased_column_types() {
        let tb_meta = MssqlTbMeta {
            basic: RdbTbMeta {
                cols: vec!["id".to_string()],
                ..Default::default()
            },
            col_type_map: HashMap::from([("id".to_string(), MssqlColType::Int4)]),
        };

        assert_eq!(json!(tb_meta)["col_type_map"]["id"], "Int4");
    }
}
