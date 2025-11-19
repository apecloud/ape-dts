use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
};

use anyhow::Context;
use dt_common::{error::Error, meta::col_value::ColValue};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::json;

use super::log_type::LogType;

#[derive(Serialize, Deserialize)]
pub struct CheckLog {
    pub log_type: LogType,
    pub schema: String,
    pub tb: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_schema: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_tb: Option<String>,
    #[serde(serialize_with = "ordered_map")]
    pub id_col_values: HashMap<String, Option<String>>,
    #[serde(
        skip_serializing_if = "HashMap::is_empty",
        serialize_with = "ordered_map"
    )]
    pub diff_col_values: HashMap<String, DiffColValue>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "ordered_option_map"
    )]
    pub src_row: Option<HashMap<String, ColValue>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "ordered_option_map"
    )]
    pub dst_row: Option<HashMap<String, ColValue>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revise_sql: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DiffColValue {
    pub src: Option<String>,
    pub dst: Option<String>,
}

impl std::fmt::Display for CheckLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

impl FromStr for CheckLog {
    type Err = Error;
    fn from_str(str: &str) -> Result<Self, Self::Err> {
        let me: Self = serde_json::from_str(str)
            .with_context(|| format!("invalid check log: [{}]", str))
            .unwrap();
        Ok(me)
    }
}

/// For use with serde's [serialize_with] attribute
pub fn ordered_map<S, K: Ord + Serialize, V: Serialize>(
    value: &HashMap<K, V>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let ordered: BTreeMap<_, _> = value.iter().collect();
    ordered.serialize(serializer)
}

/// Like `ordered_map` but works with optional maps so that serde can skip them when None.
pub fn ordered_option_map<S, K: Ord + Serialize, V: Serialize>(
    value: &Option<HashMap<K, V>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(map) => {
            let ordered: BTreeMap<_, _> = map.iter().collect();
            ordered.serialize(serializer)
        }
        None => serializer.serialize_none(),
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
            log_type: LogType::Diff,
            schema: "s".into(),
            tb: "t".into(),
            target_schema: None,
            target_tb: None,
            id_col_values,
            diff_col_values: HashMap::new(),
            src_row: None,
            dst_row: None,
            revise_sql: None,
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
