use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
};

use anyhow::Context;
use dt_common::{error::Error, meta::col_value::ColValue, utils::serialize_util::SerializeUtil};
use serde::{ser::SerializeStruct, Deserialize, Serialize, Serializer};

#[derive(Deserialize, Clone)]
pub struct CheckLog {
    pub schema: String,
    pub tb: String,
    #[serde(default)]
    pub target_schema: Option<String>,
    #[serde(default)]
    pub target_tb: Option<String>,
    pub id_col_values: HashMap<String, Option<String>>,
    #[serde(default)]
    // diff_col_values is empty means no diff, is miss
    pub diff_col_values: HashMap<String, DiffColValue>,
    #[serde(default)]
    pub src_row: Option<HashMap<String, ColValue>>,
    #[serde(default)]
    pub dst_row: Option<HashMap<String, ColValue>>,
}

fn ordered_map<V>(map: &HashMap<String, V>) -> BTreeMap<&String, &V> {
    map.iter().collect()
}

fn has_target_identity(
    schema: &str,
    tb: &str,
    target_schema: Option<&String>,
    target_tb: Option<&String>,
) -> bool {
    target_schema.is_some_and(|target_schema| target_schema != schema)
        || target_tb.is_some_and(|target_tb| target_tb != tb)
}

impl Serialize for CheckLog {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut len = 3;
        let has_target = has_target_identity(
            &self.schema,
            &self.tb,
            self.target_schema.as_ref(),
            self.target_tb.as_ref(),
        );
        if has_target {
            len += 2;
        }
        if !self.diff_col_values.is_empty() {
            len += 1;
        }
        if self.src_row.is_some() {
            len += 1;
        }
        if self.dst_row.is_some() {
            len += 1;
        }

        let mut state = serializer.serialize_struct("CheckLog", len)?;
        state.serialize_field("schema", &self.schema)?;
        state.serialize_field("tb", &self.tb)?;
        if has_target {
            let target_schema = self.target_schema.as_ref().unwrap_or(&self.schema);
            let target_tb = self.target_tb.as_ref().unwrap_or(&self.tb);
            state.serialize_field("target_schema", target_schema)?;
            state.serialize_field("target_tb", target_tb)?;
        }
        state.serialize_field("id_col_values", &ordered_map(&self.id_col_values))?;
        if !self.diff_col_values.is_empty() {
            state.serialize_field("diff_col_values", &ordered_map(&self.diff_col_values))?;
        }
        if let Some(src_row) = &self.src_row {
            state.serialize_field("src_row", &ordered_map(src_row))?;
        }
        if let Some(dst_row) = &self.dst_row {
            state.serialize_field("dst_row", &ordered_map(dst_row))?;
        }
        state.end()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DiffColValue {
    #[serde(default)]
    pub src: Option<String>,
    #[serde(default)]
    pub dst: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub src_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dst_type: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct CheckSummaryLog {
    #[serde(default)]
    pub start_time: String,
    #[serde(default)]
    pub end_time: String,
    pub is_consistent: bool,
    #[serde(default)]
    pub miss_count: usize,
    #[serde(default)]
    pub diff_count: usize,
    #[serde(default)]
    pub skip_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql_count: Option<usize>,
    #[serde(default)]
    pub tables: Vec<CheckTableSummaryLog>,
}

#[derive(Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct CheckTableSummaryLog {
    pub schema: String,
    pub tb: String,
    #[serde(default)]
    pub target_schema: Option<String>,
    #[serde(default)]
    pub target_tb: Option<String>,
    #[serde(default)]
    pub checked_count: usize,
    #[serde(default)]
    pub miss_count: usize,
    #[serde(default)]
    pub diff_count: usize,
    #[serde(default)]
    pub skip_count: usize,
}

impl Serialize for CheckTableSummaryLog {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut len = 6;
        let has_target = has_target_identity(
            &self.schema,
            &self.tb,
            self.target_schema.as_ref(),
            self.target_tb.as_ref(),
        );
        if has_target {
            len += 2;
        }

        let mut state = serializer.serialize_struct("CheckTableSummaryLog", len)?;
        state.serialize_field("schema", &self.schema)?;
        state.serialize_field("tb", &self.tb)?;
        if has_target {
            let target_schema = self.target_schema.as_ref().unwrap_or(&self.schema);
            let target_tb = self.target_tb.as_ref().unwrap_or(&self.tb);
            state.serialize_field("target_schema", target_schema)?;
            state.serialize_field("target_tb", target_tb)?;
        }
        state.serialize_field("checked_count", &self.checked_count)?;
        state.serialize_field("miss_count", &self.miss_count)?;
        state.serialize_field("diff_count", &self.diff_count)?;
        state.serialize_field("skip_count", &self.skip_count)?;
        state.end()
    }
}

impl CheckSummaryLog {
    pub fn merge(&mut self, other: &CheckSummaryLog) {
        if self.start_time.is_empty()
            || (!other.start_time.is_empty() && other.start_time < self.start_time)
        {
            self.start_time = other.start_time.clone();
        }
        if self.end_time.is_empty()
            || (!other.end_time.is_empty() && other.end_time > self.end_time)
        {
            self.end_time = other.end_time.clone();
        }
        self.is_consistent = self.is_consistent && other.is_consistent;
        self.miss_count += other.miss_count;
        self.diff_count += other.diff_count;
        self.skip_count += other.skip_count;
        if let Some(sql_count) = other.sql_count {
            self.sql_count = Some(self.sql_count.unwrap_or_default() + sql_count);
        }
        for table in &other.tables {
            self.merge_table(table.clone());
        }
    }

    pub fn merge_table(&mut self, table: CheckTableSummaryLog) {
        if let Some(existing) = self.tables.iter_mut().find(|existing| {
            existing.schema == table.schema
                && existing.tb == table.tb
                && existing.target_schema == table.target_schema
                && existing.target_tb == table.target_tb
        }) {
            existing.checked_count += table.checked_count;
            existing.miss_count += table.miss_count;
            existing.diff_count += table.diff_count;
            existing.skip_count += table.skip_count;
        } else {
            self.tables.push(table);
        }
    }
}

pub fn to_json_line<T: Serialize>(value: &T) -> Option<String> {
    match serde_json::to_string(value) {
        Ok(s) => Some(s),
        Err(e) => {
            log::warn!(
                "Skipping checker log output because serialization failed: {}",
                e
            );
            None
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct StructCheckLog {
    pub schema: String,
    pub tb: String,
    #[serde(serialize_with = "SerializeUtil::ordered_map")]
    pub id_col_values: HashMap<String, Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub src_sql: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dst_sql: Option<String>,
}

impl StructCheckLog {
    pub fn new(key: String, src_sql: Option<String>, dst_sql: Option<String>) -> Self {
        let (schema, tb) = struct_key_scope(&key);
        Self {
            schema: schema.clone(),
            tb,
            id_col_values: HashMap::from([("object_key".to_string(), Some(key))]),
            src_sql,
            dst_sql,
        }
    }
}

fn struct_key_scope(key: &str) -> (String, String) {
    let parts = key.split('.').collect::<Vec<_>>();
    let schema = parts.get(1).copied().unwrap_or_default().to_string();
    let tb = parts
        .get(2)
        .copied()
        .or_else(|| parts.get(1).copied())
        .unwrap_or_default()
        .to_string();
    (schema, tb)
}

impl FromStr for CheckLog {
    type Err = Error;
    fn from_str(str: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(str)
            .with_context(|| format!("invalid check log: [{}]", str))
            .map_err(|e| Error::Unexpected(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn json_line<T: serde::Serialize>(value: &T) -> serde_json::Value {
        serde_json::from_str(&to_json_line(value).unwrap()).unwrap()
    }

    #[test]
    fn check_log_serializes_null_diff_endpoints() {
        let log = CheckLog {
            schema: "s1".to_string(),
            tb: "t1".to_string(),
            target_schema: None,
            target_tb: None,
            id_col_values: HashMap::from([("id".to_string(), Some("1".to_string()))]),
            diff_col_values: HashMap::from([(
                "name".to_string(),
                DiffColValue {
                    src: None,
                    dst: Some("dst".to_string()),
                    src_type: Some("None".to_string()),
                    dst_type: Some("String".to_string()),
                },
            )]),
            src_row: None,
            dst_row: None,
        };

        let actual = json_line(&log);
        let expected = json!({
            "schema": "s1",
            "tb": "t1",
            "id_col_values": { "id": "1" },
            "diff_col_values": {
                "name": {
                    "src": null,
                    "dst": "dst",
                    "src_type": "None",
                    "dst_type": "String"
                }
            }
        });

        assert_eq!(actual, expected);
    }

    #[test]
    fn check_log_target_identity_is_serialized_only_when_changed() {
        let changed = CheckLog {
            schema: "src_s".to_string(),
            tb: "src_t".to_string(),
            target_schema: Some("dst_s".to_string()),
            target_tb: Some("src_t".to_string()),
            id_col_values: HashMap::from([("id".to_string(), Some("1".to_string()))]),
            diff_col_values: HashMap::new(),
            src_row: None,
            dst_row: None,
        };
        let actual = json_line(&changed);
        assert_eq!(actual["target_schema"], "dst_s");
        assert_eq!(actual["target_tb"], "src_t");

        let unchanged = CheckLog {
            schema: "src_s".to_string(),
            tb: "src_t".to_string(),
            target_schema: Some("src_s".to_string()),
            target_tb: Some("src_t".to_string()),
            id_col_values: HashMap::from([("id".to_string(), Some("1".to_string()))]),
            diff_col_values: HashMap::new(),
            src_row: None,
            dst_row: None,
        };
        let actual = json_line(&unchanged);
        assert!(actual.get("target_schema").is_none());
        assert!(actual.get("target_tb").is_none());
    }

    #[test]
    fn struct_check_log_uses_top_level_identity_fields() {
        let log = StructCheckLog::new(
            "index.s1.t1.idx_1".to_string(),
            Some("CREATE INDEX idx_1 ON t1(c1)".to_string()),
            None,
        );

        let actual = json_line(&log);
        let expected = json!({
            "schema": "s1",
            "tb": "t1",
            "id_col_values": { "object_key": "index.s1.t1.idx_1" },
            "src_sql": "CREATE INDEX idx_1 ON t1(c1)"
        });

        assert_eq!(actual, expected);
    }

    #[test]
    fn summary_log_always_serializes_tables() {
        let summary = CheckSummaryLog {
            start_time: "start".to_string(),
            end_time: "end".to_string(),
            is_consistent: true,
            ..Default::default()
        };

        let actual = json_line(&summary);
        assert_eq!(actual["tables"], json!([]));
    }

    #[test]
    fn table_summary_outputs_target_identity_as_pair() {
        let table = CheckTableSummaryLog {
            schema: "s1".to_string(),
            tb: "t1".to_string(),
            target_schema: Some("dst_s".to_string()),
            target_tb: Some("t1".to_string()),
            checked_count: 2,
            miss_count: 1,
            diff_count: 1,
            ..Default::default()
        };

        let actual = serde_json::to_value(table).unwrap();
        let expected = json!({
            "schema": "s1",
            "tb": "t1",
            "target_schema": "dst_s",
            "target_tb": "t1",
            "checked_count": 2,
            "miss_count": 1,
            "diff_count": 1,
            "skip_count": 0
        });

        assert_eq!(actual, expected);
    }
}
