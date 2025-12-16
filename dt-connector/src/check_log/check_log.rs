use std::{collections::HashMap, str::FromStr};

use anyhow::Context;
use dt_common::{error::Error, meta::col_value::ColValue, utils::serialize_util::SerializeUtil};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Serialize, Deserialize)]
pub struct CheckLog {
    pub schema: String,
    pub tb: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_schema: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_tb: Option<String>,
    #[serde(serialize_with = "SerializeUtil::ordered_map")]
    pub id_col_values: HashMap<String, Option<String>>,
    #[serde(
        default,
        skip_serializing_if = "HashMap::is_empty",
        serialize_with = "SerializeUtil::ordered_map"
    )]
    // diff_col_values is empty means no diff, is miss
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
    pub src: Option<String>,
    pub dst: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub src_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dst_type: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct CheckSummaryLog {
    pub start_time: String,
    pub end_time: String,
    pub is_consistent: bool,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub miss_count: usize,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub diff_count: usize,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub extra_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql_count: Option<usize>,
}

fn is_zero(num: &usize) -> bool {
    *num == 0
}

impl CheckSummaryLog {
    pub fn merge(&mut self, other: &CheckSummaryLog) {
        if other.end_time > self.end_time {
            self.end_time = other.end_time.clone();
        }
        self.is_consistent = self.is_consistent && other.is_consistent;
        self.miss_count += other.miss_count;
        self.diff_count += other.diff_count;
        self.extra_count += other.extra_count;
        if let Some(sql_count) = other.sql_count {
            self.sql_count = Some(self.sql_count.unwrap_or(0) + sql_count);
        }
    }
}

impl std::fmt::Display for CheckSummaryLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

impl std::fmt::Display for CheckLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

#[derive(Serialize, Deserialize)]
pub struct StructCheckLog {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub src_sql: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dst_sql: Option<String>,
}

impl std::fmt::Display for StructCheckLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string());
        write!(f, "{}", s)
    }
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
    use std::collections::HashMap;

    fn sample_log() -> CheckLog {
        let mut id_col_values = HashMap::new();
        id_col_values.insert("id".to_string(), Some("1".to_string()));
        CheckLog {
            schema: "s".into(),
            tb: "t".into(),
            target_schema: None,
            target_tb: None,
            id_col_values,
            diff_col_values: HashMap::new(),
            src_row: None,
            dst_row: None,
        }
    }

    #[test]
    fn skips_row_fields_when_absent() {
        let log = sample_log();
        let value = serde_json::to_value(&log).unwrap();
        assert!(value.get("src_row").is_none());
        assert!(value.get("dst_row").is_none());
    }

    #[test]
    fn serializes_row_fields_when_present() {
        let mut log = sample_log();
        let mut row = HashMap::new();
        row.insert("f_0".to_string(), ColValue::Long(5));
        row.insert("f_1".to_string(), ColValue::String("ok".into()));
        log.src_row = Some(row.clone());
        log.dst_row = Some(row);
        let value = serde_json::to_value(&log).unwrap();
        assert_eq!(value["src_row"]["f_0"], 5);
        assert_eq!(value["dst_row"]["f_1"], "ok");
    }

    #[test]
    fn serializes_target_fields_when_present() {
        let mut log = sample_log();
        log.target_schema = Some("dst_schema".into());
        log.target_tb = Some("dst_tb".into());
        let value = serde_json::to_value(&log).unwrap();
        assert_eq!(value["target_schema"], "dst_schema");
        assert_eq!(value["target_tb"], "dst_tb");
    }
}
