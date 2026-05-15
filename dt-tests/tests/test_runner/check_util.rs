use serde_json::Value;
use std::{collections::HashSet, fs, fs::File};

use dt_common::config::config_enums::DbType;

use super::base_test_runner::BaseTestRunner;

pub struct CheckUtil {}

impl CheckUtil {
    pub fn validate_check_log(
        expect_check_log_dir: &str,
        dst_check_log_dir: &str,
    ) -> anyhow::Result<()> {
        // check result
        let (expect_miss_logs, expect_diff_logs, expect_summary_logs, expect_sql_logs) =
            Self::load_check_log(expect_check_log_dir);
        let (actual_miss_logs, actual_diff_logs, actual_summary_logs, actual_sql_logs) =
            Self::load_check_log(dst_check_log_dir);

        for (file_name, expect_logs) in [
            ("miss.log", &expect_miss_logs),
            ("diff.log", &expect_diff_logs),
            ("sql.log", &expect_sql_logs),
        ] {
            if expect_logs.is_empty() {
                let actual_file = format!("{}/{}", dst_check_log_dir, file_name);
                assert!(
                    !BaseTestRunner::check_path_exists(&actual_file),
                    "{} should not be generated when there are no entries",
                    actual_file
                );
            }
        }

        assert_eq!(expect_diff_logs.len(), actual_diff_logs.len());
        assert_eq!(expect_miss_logs.len(), actual_miss_logs.len());
        assert_eq!(expect_sql_logs.len(), actual_sql_logs.len());
        for log in actual_diff_logs {
            println!("actual_diff_logs: {}", log);
            assert!(expect_diff_logs.contains(&log))
        }
        for log in actual_miss_logs {
            println!("actual_miss_logs: {}", log);
            assert!(expect_miss_logs.contains(&log))
        }
        for log in actual_sql_logs {
            println!("actual_sql_logs: {}", log);
            assert!(expect_sql_logs.contains(&log))
        }

        Self::validate_summary_logs(expect_summary_logs, actual_summary_logs)?;

        Ok(())
    }

    pub fn validate_check_log_with_size_limit(
        expect_check_log_dir: &str,
        dst_check_log_dir: &str,
        size_limit: u64,
    ) -> anyhow::Result<()> {
        let (_, _, expect_summary_logs, _) = Self::load_check_log(expect_check_log_dir);
        let (actual_miss_logs, actual_diff_logs, actual_summary_logs, actual_sql_logs) =
            Self::load_check_log(dst_check_log_dir);

        Self::validate_summary_logs(expect_summary_logs, actual_summary_logs)?;
        assert!(
            !actual_miss_logs.is_empty()
                || !actual_diff_logs.is_empty()
                || !actual_sql_logs.is_empty(),
            "expected at least one size-limited checker log entry"
        );

        for file in ["miss.log", "diff.log", "sql.log"] {
            let path = format!("{}/{}", dst_check_log_dir, file);
            if let Ok(metadata) = fs::metadata(&path) {
                assert!(
                    metadata.len() <= size_limit,
                    "{} exceeds size limit: {} > {}",
                    path,
                    metadata.len(),
                    size_limit
                );
            }
        }

        Ok(())
    }

    fn validate_summary_logs(
        expect_summary_logs: Vec<String>,
        actual_summary_logs: Vec<String>,
    ) -> anyhow::Result<()> {
        // summary log contains time, so we can't compare it directly
        // but we can compare the count and summary fields
        assert_eq!(
            expect_summary_logs.len(),
            1,
            "expect summary.log must contain exactly one JSON line"
        );
        assert_eq!(
            actual_summary_logs.len(),
            1,
            "actual summary.log must contain exactly one JSON line"
        );

        let expect_log = &expect_summary_logs[0];
        let expect_value: Value = serde_json::from_str(expect_log).map_err(|e| {
            anyhow::anyhow!(
                "Failed to parse expect summary log: {}, error: {}",
                expect_log,
                e
            )
        })?;
        let expect: dt_connector::checker::check_log::CheckSummaryLog =
            serde_json::from_value(expect_value.clone()).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to parse expect summary log: {}, error: {}",
                    expect_log,
                    e
                )
            })?;

        let actual_log = &actual_summary_logs[0];
        let actual: dt_connector::checker::check_log::CheckSummaryLog =
            serde_json::from_str(actual_log).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to parse actual summary log: {}, error: {}",
                    actual_log,
                    e
                )
            })?;

        assert_eq!(
            expect.is_consistent, actual.is_consistent,
            "is_consistent mismatch"
        );
        assert_eq!(
            Self::expected_summary_count(&expect_value, "checked_count"),
            actual.checked_count as u64,
            "checked_count mismatch"
        );
        assert_eq!(expect.miss_count, actual.miss_count, "miss_count mismatch");
        assert_eq!(expect.diff_count, actual.diff_count, "diff_count mismatch");
        assert_eq!(expect.skip_count, actual.skip_count, "skip_count mismatch");
        assert_eq!(expect.sql_count, actual.sql_count, "sql_count mismatch");
        Self::validate_summary_tables(&expect_value, &actual);
        Ok(())
    }

    fn expected_summary_count(expect_value: &Value, field: &str) -> u64 {
        if let Some(value) = expect_value.get(field) {
            return value
                .as_u64()
                .unwrap_or_else(|| panic!("summary {} is not a number", field));
        }
        expect_value
            .get("tables")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|table| table.get(field).and_then(Value::as_u64).unwrap_or(0))
            .sum()
    }

    fn validate_summary_tables(
        expect_value: &Value,
        actual: &dt_connector::checker::check_log::CheckSummaryLog,
    ) {
        let Some(expect_tables) = expect_value.get("tables").and_then(Value::as_array) else {
            assert!(
                actual.tables.is_empty(),
                "expect summary tables is missing but actual contains table entries"
            );
            return;
        };
        assert_eq!(
            expect_tables.len(),
            actual.tables.len(),
            "summary table count mismatch"
        );

        let mut matched_actual = vec![false; actual.tables.len()];
        for expect_table in expect_tables {
            let schema = expect_table
                .get("schema")
                .and_then(Value::as_str)
                .expect("expect summary table schema is missing");
            let tb = expect_table
                .get("tb")
                .and_then(Value::as_str)
                .expect("expect summary table tb is missing");
            let target_schema = expect_table.get("target_schema").and_then(Value::as_str);
            let target_tb = expect_table.get("target_tb").and_then(Value::as_str);
            let (actual_index, actual_table) = actual
                .tables
                .iter()
                .enumerate()
                .find(|(index, actual_table)| {
                    !matched_actual[*index]
                        && actual_table.schema == schema
                        && actual_table.tb == tb
                        && actual_table.target_schema.as_deref() == target_schema
                        && actual_table.target_tb.as_deref() == target_tb
                })
                .unwrap_or_else(|| panic!("summary table not found: {}.{}", schema, tb));
            matched_actual[actual_index] = true;

            Self::validate_summary_table_count(
                expect_table,
                "checked_count",
                actual_table.checked_count,
            );
            Self::validate_summary_table_count(expect_table, "miss_count", actual_table.miss_count);
            Self::validate_summary_table_count(expect_table, "diff_count", actual_table.diff_count);
            Self::validate_summary_table_count(expect_table, "skip_count", actual_table.skip_count);
        }
        assert!(
            matched_actual.iter().all(|matched| *matched),
            "actual summary contains unexpected table entries"
        );
    }

    fn validate_summary_table_count(expect_table: &Value, field: &str, actual: usize) {
        let expect = expect_table
            .get(field)
            .map(|value| {
                value
                    .as_u64()
                    .unwrap_or_else(|| panic!("summary table {} is not a number", field))
            })
            .unwrap_or(0);
        assert_eq!(expect, actual as u64, "summary table {} mismatch", field);
    }

    pub fn clear_check_log(dst_check_log_dir: &str) {
        if dst_check_log_dir.is_empty() {
            return;
        }
        for file in ["miss.log", "diff.log", "sql.log"] {
            let log_file = format!("{}/{}", dst_check_log_dir, file);
            if BaseTestRunner::check_path_exists(&log_file) {
                fs::remove_file(&log_file).unwrap();
            }
        }
        for file in ["summary.log"] {
            let log_file = format!("{}/{}", dst_check_log_dir, file);
            if BaseTestRunner::check_path_exists(&log_file) {
                File::create(&log_file).unwrap().set_len(0).unwrap();
            }
        }
    }

    pub fn get_check_log_dir(base_test_runner: &BaseTestRunner, version: &str) -> (String, String) {
        let mut expect_check_log_dir = format!("{}/expect_check_log", base_test_runner.test_dir);
        let dst_db_type = base_test_runner
            .get_config()
            .destination_target()
            .map(|target| target.db_type)
            .unwrap_or(base_test_runner.get_config().sinker_basic.db_type.clone());
        if !BaseTestRunner::check_path_exists(&expect_check_log_dir) && dst_db_type == DbType::Mysql
        {
            // mysql 5.7, 8.0
            if version.starts_with("5.") {
                expect_check_log_dir =
                    format!("{}/expect_check_log_5.7", base_test_runner.test_dir);
            } else {
                expect_check_log_dir =
                    format!("{}/expect_check_log_8.0", base_test_runner.test_dir);
            }
        }

        let config = base_test_runner.get_config();
        let dst_check_log_dir = config
            .checker
            .as_ref()
            .map(|checker| {
                if checker.check_log_dir.is_empty() {
                    format!("{}/check", config.runtime.log_dir)
                } else {
                    checker.check_log_dir.clone()
                }
            })
            .unwrap_or_default();
        (expect_check_log_dir, dst_check_log_dir)
    }

    fn load_check_log(
        log_dir: &str,
    ) -> (
        HashSet<String>,
        HashSet<String>,
        Vec<String>,
        HashSet<String>,
    ) {
        let miss_log_file = format!("{}/miss.log", log_dir);
        let diff_log_file = format!("{}/diff.log", log_dir);
        let summary_log_file = format!("{}/summary.log", log_dir);
        let sql_log_file = format!("{}/sql.log", log_dir);

        let mut miss_logs = HashSet::new();
        let mut diff_logs = HashSet::new();
        let mut summary_logs = Vec::new();
        let mut sql_logs = HashSet::new();

        for log in BaseTestRunner::load_file(&miss_log_file) {
            miss_logs.insert(Self::normalize_log(&miss_log_file, &log));
        }
        for log in BaseTestRunner::load_file(&diff_log_file) {
            diff_logs.insert(Self::normalize_log(&diff_log_file, &log));
        }
        for log in BaseTestRunner::load_file(&summary_log_file) {
            summary_logs.push(log);
        }
        for log in BaseTestRunner::load_file(&sql_log_file) {
            sql_logs.insert(log);
        }
        (miss_logs, diff_logs, summary_logs, sql_logs)
    }

    fn normalize_log(file: &str, log: &str) -> String {
        let map: std::collections::BTreeMap<String, Value> = serde_json::from_str(log)
            .unwrap_or_else(|e| {
                panic!(
                    "failed to parse check log [{}]: {}, error: {}",
                    file, log, e
                )
            });
        serde_json::to_string(&map).unwrap_or_else(|e| {
            panic!(
                "failed to normalize check log [{}]: {}, error: {}",
                file, log, e
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::CheckUtil;
    use serde_json::json;

    fn summary_log(diff_count: usize) -> String {
        format!(
            r#"{{"is_consistent":false,"diff_count":{},"tables":[{{"schema":"s1","tb":"t1","checked_count":2,"diff_count":{}}}]}}"#,
            diff_count, diff_count
        )
    }

    #[test]
    fn summary_log_validation_accepts_single_summary_with_table_counts() {
        let expect = summary_log(1);
        let actual = r#"{"is_consistent":false,"checked_count":2,"diff_count":1,"tables":[{"schema":"s1","tb":"t1","checked_count":2,"diff_count":1}]}"#;

        CheckUtil::validate_summary_logs(vec![expect], vec![actual.to_string()]).unwrap();
    }

    #[test]
    fn summary_helpers_treat_missing_counts_as_zero() {
        let expect = json!({
            "tables": [
                {
                    "schema": "s1",
                    "tb": "t1",
                    "diff_count": 1
                }
            ]
        });
        let actual: dt_connector::checker::check_log::CheckSummaryLog =
            serde_json::from_str(
                r#"{"is_consistent":false,"diff_count":1,"tables":[{"schema":"s1","tb":"t1","diff_count":1}]}"#,
            )
            .unwrap();

        assert_eq!(
            CheckUtil::expected_summary_count(&expect, "checked_count"),
            0
        );
        CheckUtil::validate_summary_tables(&expect, &actual);
    }
}
