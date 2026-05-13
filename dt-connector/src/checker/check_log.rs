use std::{collections::HashMap, str::FromStr};

use anyhow::Context;
use dt_common::{error::Error, meta::col_value::ColValue, utils::serialize_util::SerializeUtil};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct CheckLog {
    pub schema: String,
    pub tb: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_schema: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_tb: Option<String>,
    #[serde(serialize_with = "SerializeUtil::ordered_map")]
    pub id_col_values: HashMap<String, Option<String>>,
    // diff_col_values is empty means no diff, is miss
    #[serde(
        default,
        skip_serializing_if = "HashMap::is_empty",
        serialize_with = "SerializeUtil::ordered_map"
    )]
    pub diff_col_values: HashMap<String, DiffColValue>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "SerializeUtil::ordered_option_map"
    )]
    pub src_row: Option<HashMap<String, ColValue>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "SerializeUtil::ordered_option_map"
    )]
    pub dst_row: Option<HashMap<String, ColValue>>,
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
    pub checked_count: usize,
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

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct CheckTableSummaryLog {
    pub schema: String,
    pub tb: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_schema: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
        self.checked_count += other.checked_count;
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

    pub fn sort_tables(&mut self) {
        self.tables.sort_by(|a, b| {
            (
                a.schema.as_str(),
                a.tb.as_str(),
                a.target_schema.as_deref(),
                a.target_tb.as_deref(),
            )
                .cmp(&(
                    b.schema.as_str(),
                    b.tb.as_str(),
                    b.target_schema.as_deref(),
                    b.target_tb.as_deref(),
                ))
        });
    }
}

pub fn to_json_line<T: Serialize>(value: &T) -> Option<String> {
    serde_json::to_string(value)
        .map_err(|e| {
            log::warn!(
                "Skipping checker log output because serialization failed: {}",
                e
            )
        })
        .ok()
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
    let mut parts = key.split('.');
    let _ = parts.next();
    let schema = parts.next().unwrap_or_default();
    let tb = parts.next().unwrap_or(schema);
    (schema.to_string(), tb.to_string())
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

    fn json_line<T: Serialize>(value: &T) -> serde_json::Value {
        serde_json::from_str(&to_json_line(value).unwrap()).unwrap()
    }

    #[test]
    fn check_log_outputs_stable_json_fields() {
        let log = CheckLog {
            schema: "src_s".to_string(),
            tb: "src_t".to_string(),
            target_schema: Some("dst_s".to_string()),
            target_tb: Some("src_t".to_string()),
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

        let line = to_json_line(&log).unwrap();
        let parsed = CheckLog::from_str(&line).unwrap();
        assert_eq!(to_json_line(&parsed).unwrap(), line);

        let actual: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(
            actual,
            json!({
                "schema": "src_s",
                "tb": "src_t",
                "target_schema": "dst_s",
                "target_tb": "src_t",
                "id_col_values": { "id": "1" },
                "diff_col_values": {
                    "name": {
                        "src": null,
                        "dst": "dst",
                        "src_type": "None",
                        "dst_type": "String"
                    }
                }
            })
        );
    }

    #[test]
    fn summary_and_struct_logs_keep_expected_shape() {
        let struct_log = StructCheckLog::new(
            "index.s1.t1.idx_1".to_string(),
            Some("CREATE INDEX idx_1 ON t1(c1)".to_string()),
            None,
        );
        assert_eq!(
            json_line(&struct_log),
            json!({
                "schema": "s1",
                "tb": "t1",
                "id_col_values": { "object_key": "index.s1.t1.idx_1" },
                "src_sql": "CREATE INDEX idx_1 ON t1(c1)"
            })
        );

        let summary = CheckSummaryLog {
            start_time: "start".to_string(),
            end_time: "end".to_string(),
            is_consistent: true,
            checked_count: 2,
            tables: vec![CheckTableSummaryLog {
                schema: "s1".to_string(),
                tb: "t1".to_string(),
                target_schema: Some("dst_s".to_string()),
                target_tb: Some("t1".to_string()),
                checked_count: 2,
                diff_count: 1,
                ..Default::default()
            }],
            ..Default::default()
        };

        assert_eq!(
            json_line(&summary),
            json!({
                "start_time": "start",
                "end_time": "end",
                "is_consistent": true,
                "checked_count": 2,
                "miss_count": 0,
                "diff_count": 0,
                "skip_count": 0,
                "tables": [{
                    "schema": "s1",
                    "tb": "t1",
                    "target_schema": "dst_s",
                    "target_tb": "t1",
                    "checked_count": 2,
                    "miss_count": 0,
                    "diff_count": 1,
                    "skip_count": 0
                }]
            })
        );
    }
}
