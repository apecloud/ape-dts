use std::{collections::HashMap, str::FromStr};

use anyhow::Context;
use dt_common::{
    error::{DtError, DtResultExt, Stage},
    meta::col_value::ColValue,
    utils::serialize_util::SerializeUtil,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct CheckLog {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub db: String,
    pub schema: String,
    pub tb: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_db: Option<String>,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<CheckTableSummaryLog>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct CheckTableSummaryLog {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub db: String,
    pub schema: String,
    pub tb: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_db: Option<String>,
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
            existing.db == table.db
                && existing.schema == table.schema
                && existing.tb == table.tb
                && existing.target_db == table.target_db
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
                a.db.as_str(),
                a.schema.as_str(),
                a.tb.as_str(),
                a.target_db.as_deref(),
                a.target_schema.as_deref(),
                a.target_tb.as_deref(),
            )
                .cmp(&(
                    b.db.as_str(),
                    b.schema.as_str(),
                    b.tb.as_str(),
                    b.target_db.as_deref(),
                    b.target_schema.as_deref(),
                    b.target_tb.as_deref(),
                ))
        });
    }
}

pub fn to_json_line<T: Serialize>(value: &T) -> Option<String> {
    serde_json::to_string(value)
        .inspect_err(|e| {
            log::warn!(
                "Skipping checker log output because serialization failed: {}",
                e
            )
        })
        .ok()
}

#[derive(Serialize, Deserialize)]
pub struct StructCheckLog {
    pub key: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub db: String,
    pub schema: String,
    pub tb: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_db: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_schema: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_tb: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub src_sql: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dst_sql: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StructCheckKey {
    pub key: String,
    pub db: String,
    pub schema: String,
    pub tb: String,
    segments: Vec<String>,
    schema_index: Option<usize>,
    tb_index: Option<usize>,
    table_scoped: bool,
}

impl StructCheckKey {
    pub fn new(db: &str, key: &str) -> Self {
        let segments = key.split('.').map(str::to_string).collect::<Vec<_>>();
        let object_type = segments.first().map(String::as_str).unwrap_or_default();
        let (schema_index, tb_index, table_scoped) = match object_type {
            "database" | "schema" => (Some(1), None, false),
            "rbac" if segments.get(1).map(String::as_str) == Some("privilege") => {
                match segments.get(2).map(String::as_str) {
                    Some("table") | Some("sequence") => (Some(3), Some(4), true),
                    Some("schema") => (Some(3), None, false),
                    _ => (None, None, false),
                }
            }
            "table" | "index" | "constraint" | "column_comment" | "table_comment"
            | "sequence_owner" => (Some(1), Some(2), true),
            "sequence" | "udt" | "udf" => (Some(1), Some(2), false),
            _ => (None, None, false),
        };
        let schema = schema_index
            .and_then(|index| segments.get(index))
            .cloned()
            .unwrap_or_default();
        let tb = tb_index
            .and_then(|index| segments.get(index))
            .cloned()
            .unwrap_or_default();

        Self {
            key: key.to_string(),
            db: db.to_string(),
            schema,
            tb,
            segments,
            schema_index,
            tb_index,
            table_scoped,
        }
    }

    pub fn with_location(&self, db: &str, schema: &str, tb: &str) -> Self {
        let mut segments = self.segments.clone();
        if let Some(index) = self.schema_index {
            segments[index] = schema.to_string();
        }
        if let Some(index) = self.tb_index {
            segments[index] = tb.to_string();
        }
        Self::new(db, &segments.join("."))
    }

    pub fn is_table_scoped(&self) -> bool {
        self.table_scoped
    }
}

impl StructCheckLog {
    pub fn new(
        key: &StructCheckKey,
        target_key: &StructCheckKey,
        src_sql: Option<String>,
        dst_sql: Option<String>,
    ) -> Self {
        let db_changed = key.db != target_key.db;
        let target_changed = key.schema != target_key.schema || key.tb != target_key.tb;
        Self {
            key: key.key.clone(),
            db: key.db.clone(),
            schema: key.schema.clone(),
            tb: key.tb.clone(),
            target_db: db_changed.then(|| target_key.db.clone()),
            target_schema: target_changed.then(|| target_key.schema.clone()),
            target_tb: target_changed.then(|| target_key.tb.clone()),
            src_sql,
            dst_sql,
        }
    }
}

impl FromStr for CheckLog {
    type Err = anyhow::Error;
    fn from_str(str: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(str)
            .with_context(|| format!("invalid check log: [{}]", str))
            .dt_error(DtError::DataDecodeFailed(
                "failed to parse checker log".to_string(),
            ))
            .stage(Stage::Checker)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn json_line<T: Serialize>(value: &T) -> serde_json::Value {
        serde_json::from_str(&to_json_line(value).unwrap()).unwrap()
    }

    #[test]
    fn checker_logs_keep_expected_json_shape() {
        let log = CheckLog {
            db: String::new(),
            schema: "src_s".to_string(),
            tb: "src_t".to_string(),
            target_db: None,
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

        let mut log_with_db = log;
        log_with_db.db = "db1".to_string();
        assert_eq!(json_line(&log_with_db)["db"], "db1");

        let legacy =
            CheckLog::from_str(r#"{"schema":"s1","tb":"t1","id_col_values":{"id":"1"}}"#).unwrap();
        assert!(legacy.db.is_empty());
        assert!(legacy.target_db.is_none());

        let key = StructCheckKey::new("", "index.s1.t1.idx_1");
        let struct_log = StructCheckLog::new(
            &key,
            &key,
            Some("CREATE INDEX idx_1 ON t1(c1)".to_string()),
            None,
        );
        assert_eq!(
            json_line(&struct_log),
            json!({
                "key": "index.s1.t1.idx_1",
                "schema": "s1",
                "tb": "t1",
                "src_sql": "CREATE INDEX idx_1 ON t1(c1)"
            })
        );
        assert!(json_line(&struct_log).get("id_col_values").is_none());

        let target_key = StructCheckKey::new("", "index.s2.t2.idx_1");
        let routed_struct_log = StructCheckLog::new(&key, &target_key, None, None);
        assert_eq!(
            json_line(&routed_struct_log),
            json!({
                "key": "index.s1.t1.idx_1",
                "schema": "s1",
                "tb": "t1",
                "target_schema": "s2",
                "target_tb": "t2"
            })
        );

        let consistent_summary = CheckSummaryLog {
            start_time: "start".to_string(),
            end_time: "end".to_string(),
            is_consistent: true,
            checked_count: 2,
            tables: vec![CheckTableSummaryLog {
                schema: "s1".to_string(),
                tb: "t1".to_string(),
                checked_count: 2,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(
            json_line(&consistent_summary),
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
                    "checked_count": 2,
                    "miss_count": 0,
                    "diff_count": 0,
                    "skip_count": 0
                }]
            })
        );

        let problem_summary = CheckSummaryLog {
            start_time: "start".to_string(),
            end_time: "end".to_string(),
            is_consistent: false,
            checked_count: 5,
            miss_count: 1,
            diff_count: 1,
            tables: vec![
                CheckTableSummaryLog {
                    schema: "s1".to_string(),
                    tb: "clean".to_string(),
                    checked_count: 3,
                    ..Default::default()
                },
                CheckTableSummaryLog {
                    schema: "s1".to_string(),
                    tb: "bad".to_string(),
                    checked_count: 2,
                    miss_count: 1,
                    diff_count: 1,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert_eq!(
            json_line(&problem_summary),
            json!({
                "start_time": "start",
                "end_time": "end",
                "is_consistent": false,
                "checked_count": 5,
                "miss_count": 1,
                "diff_count": 1,
                "skip_count": 0,
                "tables": [
                    {
                        "schema": "s1",
                        "tb": "clean",
                        "checked_count": 3,
                        "miss_count": 0,
                        "diff_count": 0,
                        "skip_count": 0
                    },
                    {
                        "schema": "s1",
                        "tb": "bad",
                        "checked_count": 2,
                        "miss_count": 1,
                        "diff_count": 1,
                        "skip_count": 0
                    }
                ]
            })
        );
    }
}
