use std::{collections::HashMap, str::FromStr};

use anyhow::Context;
use dt_common::{error::Error, meta::col_value::ColValue, utils::serialize_util::SerializeUtil};
use serde::{Deserialize, Serialize};
use serde_json::json;

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
    pub skip_count: usize,
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
        self.skip_count += other.skip_count;
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
        let s = serde_json::to_string(self).unwrap_or_else(|e| {
            log::warn!("Failed to serialize StructCheckLog: {}", e);
            "{}".to_string()
        });
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
