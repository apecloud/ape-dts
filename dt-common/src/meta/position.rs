use std::{collections::HashMap, str::FromStr};

use anyhow::Context;
use chrono::{DateTime, NaiveDateTime};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::log_error;
use crate::utils::serialize_util::SerializeUtil;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(tag = "type")]
pub enum Position {
    #[default]
    None,
    Kafka {
        topic: String,
        partition: i32,
        offset: i64,
    },
    RdbSnapshot {
        db_type: String,
        schema: String,
        tb: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "String::is_empty")]
        order_col: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "String::is_empty")]
        value: String,
        #[serde(default)]
        #[serde(serialize_with = "SerializeUtil::ordered_map")]
        order_col_values: HashMap<String, Option<String>>,
    },
    RdbSnapshotFinished {
        db_type: String,
        schema: String,
        tb: String,
    },
    MysqlCdc {
        server_id: String,
        binlog_filename: String,
        next_event_position: u32,
        gtid_set: String,
        timestamp: String,
    },
    PgCdc {
        lsn: String,
        timestamp: String,
    },
    MongoCdc {
        resume_token: String,
        operation_time: u32,
        timestamp: String,
    },
    Redis {
        repl_id: String,
        repl_port: u64,
        repl_offset: u64,
        now_db_id: i64,
        timestamp: String,
    },
    FoxlakeS3 {
        schema: String,
        tb: String,
        s3_meta_file: String,
    },
}

impl Position {
    pub fn format_timestamp_millis(millis: i64) -> String {
        if let Some(naive_datetime) = DateTime::from_timestamp_millis(millis) {
            naive_datetime.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
        } else {
            String::new()
        }
    }
}

impl std::fmt::Display for Position {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

impl FromStr for Position {
    type Err = anyhow::Error;
    fn from_str(str: &str) -> anyhow::Result<Self, anyhow::Error> {
        let me: Self = serde_json::from_str(str)
            .with_context(|| format!("invalid position str: [{}]", str))?;
        Ok(me)
    }
}

impl Position {
    pub fn from_log(log: &str) -> Position {
        // deprecated format: 2024-03-29 07:02:24.463776 | current_position | {"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk","order_col":"f_0","value":"9"}
        // new format: 2024-03-29 07:02:24.463776 | current_position | {"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk","order_col_values":{"f_0":"9"}}
        // 2024-04-01 03:25:18.701725 | {"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk"}
        if log.trim().is_empty() {
            return Position::None;
        }

        let left = log.find('{');
        let right = log.rfind('}');
        if left.is_some() && right.is_some() {
            let position_log = &log[left.unwrap()..=right.unwrap()];
            if let Ok(position) = Position::from_str(position_log) {
                return position;
            }
        }

        log_error!("invalid position log: {}", log);
        Position::None
    }

    pub fn to_timestamp(&self) -> u64 {
        match self {
            Position::MysqlCdc { timestamp, .. }
            | Position::PgCdc { timestamp, .. }
            | Position::MongoCdc { timestamp, .. }
            | Position::Redis { timestamp, .. } => {
                if timestamp.is_empty() {
                    return 0;
                }

                if let Ok(ts) = NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S%.3f") {
                    return ts.and_utc().timestamp_millis() as u64;
                }
                if let Ok(ts) = NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S") {
                    return ts.and_utc().timestamp_millis() as u64;
                }
                0
            }
            _ => 0,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_format_timestamp() {
        assert_eq!(
            "2023-03-28 07:33:48.396",
            Position::format_timestamp_millis(733304028396543 / 1000 + 946_684_800 * 1000)
        );

        assert_eq!(
            "2023-03-28 05:33:47.000",
            Position::format_timestamp_millis(1679981627 * 1000)
        );
    }

    #[test]
    fn test_from_str() {
        let strs = [
            r#"{"type":"None"}"#,
            // for compatibility
            r#"{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"numeric_table","order_col":"f_0","value":"127"}"#,
            r#"{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"numeric_table","order_col":"f_0","value":"127","order_col_values":{}}"#,
            r#"{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"numeric_table","order_col_values":{"f_0":"127","f_1":"128"}}"#,
        ];

        let expected = [
            r#"{"type":"None"}"#,
            r#"{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"numeric_table","order_col":"f_0","value":"127","order_col_values":{}}"#,
            r#"{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"numeric_table","order_col":"f_0","value":"127","order_col_values":{}}"#,
            r#"{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"numeric_table","order_col_values":{"f_0":"127","f_1":"128"}}"#,
        ];

        for (str, expected) in strs.iter().zip(expected.iter()) {
            let position = Position::from_str(str).unwrap();
            assert_eq!(expected, &position.to_string());
        }
    }

    #[test]
    fn test_from_log_deprecated() {
        let log1 = r#"2024-04-01 03:25:18.701725 | {"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk"}"#;
        let log2 = r#"2024-03-29 07:02:24.463776 | current_position | {"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk","order_col":"f_0","value":"9"}"#;
        let log3 = "task finished";

        if let Position::RdbSnapshotFinished {
            db_type,
            schema,
            tb,
        } = Position::from_log(log1)
        {
            assert_eq!(db_type, "mysql");
            assert_eq!(schema, "test_db_1");
            assert_eq!(tb, "one_pk_no_uk");
        } else {
            panic!()
        }
        let _res = Position::from_log(log2);
        if let Position::RdbSnapshot {
            db_type,
            schema,
            tb,
            order_col,
            value,
            order_col_values,
        } = Position::from_log(log2)
        {
            assert_eq!(db_type, "mysql");
            assert_eq!(schema, "test_db_1");
            assert_eq!(tb, "one_pk_no_uk");
            assert_eq!(order_col, "f_0");
            assert_eq!(value, "9");
            assert_eq!(order_col_values, HashMap::new());
        } else {
            panic!()
        }

        assert_eq!(Position::from_log(log3), Position::None);
    }

    #[test]
    fn test_from_log_one_pk() {
        let log1 = r#"2024-04-01 03:25:18.701725 | {"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk"}"#;
        let log2 = r#"2024-03-29 07:02:24.463776 | current_position | {"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk","order_col_values":{"f_0":"9"}}"#;
        let log3 = "task finished";

        if let Position::RdbSnapshotFinished {
            db_type,
            schema,
            tb,
        } = Position::from_log(log1)
        {
            assert_eq!(db_type, "mysql");
            assert_eq!(schema, "test_db_1");
            assert_eq!(tb, "one_pk_no_uk");
        } else {
            panic!()
        }
        let _res = Position::from_log(log2);
        if let Position::RdbSnapshot {
            db_type,
            schema,
            tb,
            order_col,
            value,
            order_col_values,
        } = Position::from_log(log2)
        {
            assert_eq!(db_type, "mysql");
            assert_eq!(schema, "test_db_1");
            assert_eq!(tb, "one_pk_no_uk");
            assert_eq!(order_col, "");
            assert_eq!(value, "");
            assert_eq!(
                order_col_values,
                HashMap::from([("f_0".to_string(), Some("9".to_string()))])
            );
        } else {
            panic!()
        }

        assert_eq!(Position::from_log(log3), Position::None);
    }

    #[test]
    fn test_from_log_multi_pk() {
        let log1 = r#"2024-04-01 03:25:18.701725 | {"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk"}"#;
        let log2 = r#"2024-03-29 07:02:24.463776 | current_position | {"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk","order_col_values":{"f_0":"9","f_1":"10"}}"#;
        let log3 = "task finished";

        if let Position::RdbSnapshotFinished {
            db_type,
            schema,
            tb,
        } = Position::from_log(log1)
        {
            assert_eq!(db_type, "mysql");
            assert_eq!(schema, "test_db_1");
            assert_eq!(tb, "one_pk_no_uk");
        } else {
            panic!()
        }
        let _res = Position::from_log(log2);
        if let Position::RdbSnapshot {
            db_type,
            schema,
            tb,
            order_col,
            value,
            order_col_values,
        } = Position::from_log(log2)
        {
            assert_eq!(db_type, "mysql");
            assert_eq!(schema, "test_db_1");
            assert_eq!(tb, "one_pk_no_uk");
            assert_eq!(order_col, "");
            assert_eq!(value, "");
            assert_eq!(
                order_col_values,
                HashMap::from([
                    ("f_0".to_string(), Some("9".to_string())),
                    ("f_1".to_string(), Some("10".to_string()))
                ])
            );
        } else {
            panic!()
        }

        assert_eq!(Position::from_log(log3), Position::None);
    }
}
