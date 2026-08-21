use std::collections::{HashMap, HashSet};

use serde::{ser::SerializeMap, Serialize, Serializer};
use serde_json::json;

use super::mssql_col_type::MssqlColType;
use crate::{
    config::config_enums::DbType,
    error::{DtError, DtErrorContextExt, ErrorObject},
    meta::rdb_tb_meta::RdbTbMeta,
};

#[derive(Debug, Clone, Default, Serialize)]
pub struct MssqlTbMeta {
    pub basic: RdbTbMeta,
    #[serde(serialize_with = "serialize_col_type_map")]
    pub col_type_map: HashMap<String, MssqlColType>,
    pub identity_col: Option<String>,
    pub computed_cols: HashSet<String>,
    pub generated_always_type_map: HashMap<String, u8>,
    pub rowversion_cols: HashSet<String>,
}

impl MssqlTbMeta {
    pub fn has_identity_col(&self) -> bool {
        self.identity_col.is_some()
    }

    pub fn is_writable_col(&self, col: &str) -> bool {
        !self.computed_cols.contains(col)
            && self
                .generated_always_type_map
                .get(col)
                .is_none_or(|generated_always_type| *generated_always_type == 0)
            && !self.rowversion_cols.contains(col)
    }

    pub fn non_comparable_cols(&self) -> HashSet<String> {
        let mut cols = self.rowversion_cols.clone();
        cols.extend(
            self.generated_always_type_map
                .iter()
                .filter(|(_, generated_always_type)| **generated_always_type != 0)
                .map(|(col, _)| col.clone()),
        );
        cols
    }

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
    fn serializes_column_types() {
        let tb_meta = MssqlTbMeta {
            basic: RdbTbMeta {
                cols: vec!["id".to_string()],
                ..Default::default()
            },
            col_type_map: HashMap::from([("id".to_string(), MssqlColType::Int4)]),
            identity_col: Some("id".to_string()),
            computed_cols: HashSet::from(["computed_id".to_string()]),
            generated_always_type_map: HashMap::from([("valid_from".to_string(), 1)]),
            rowversion_cols: HashSet::from(["version".to_string()]),
        };

        assert_eq!(json!(tb_meta)["col_type_map"]["id"], "Int4");
        assert_eq!(json!(tb_meta)["identity_col"], "id");
        assert!(tb_meta.has_identity_col());
        assert_eq!(json!(tb_meta)["computed_cols"][0], "computed_id");
        assert_eq!(json!(tb_meta)["generated_always_type_map"]["valid_from"], 1);
        assert_eq!(json!(tb_meta)["rowversion_cols"][0], "version");
        assert_eq!(
            tb_meta.non_comparable_cols(),
            HashSet::from(["valid_from".to_string(), "version".to_string()])
        );
    }
}
