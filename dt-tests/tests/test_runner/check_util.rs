use serde_json::Value;
use std::{collections::HashSet, fs::File};

use dt_common::config::{config_enums::DbType, sinker_config::SinkerConfig};

use super::base_test_runner::BaseTestRunner;

pub struct CheckUtil {}

impl CheckUtil {
    pub fn validate_check_log(
        expect_check_log_dir: &str,
        dst_check_log_dir: &str,
    ) -> anyhow::Result<()> {
        // check result
        let (
            expect_miss_logs,
            expect_diff_logs,
            expect_extra_logs,
            expect_summary_logs,
            expect_sql_logs,
        ) = Self::load_check_log(expect_check_log_dir);
        let (
            actual_miss_logs,
            actual_diff_logs,
            actual_extra_logs,
            actual_summary_logs,
            actual_sql_logs,
        ) = Self::load_check_log(dst_check_log_dir);

        assert_eq!(expect_diff_logs.len(), actual_diff_logs.len());
        assert_eq!(expect_miss_logs.len(), actual_miss_logs.len());
        assert_eq!(expect_extra_logs.len(), actual_extra_logs.len());
        assert_eq!(expect_sql_logs.len(), actual_sql_logs.len());
        for log in actual_diff_logs {
            println!("actual_diff_logs: {}", log);
            assert!(expect_diff_logs.contains(&log))
        }
        for log in actual_miss_logs {
            println!("actual_miss_logs: {}", log);
            assert!(expect_miss_logs.contains(&log))
        }
        for log in actual_extra_logs {
            println!("actual_extra_logs: {}", log);
            assert!(expect_extra_logs.contains(&log))
        }
        for log in actual_sql_logs {
            println!("actual_sql_logs: {}", log);
            assert!(expect_sql_logs.contains(&log))
        }

        // summary log contains time, so we can't compare it directly
        // but we can compare the count
        assert_eq!(expect_summary_logs.len(), actual_summary_logs.len());

        // validate summary log structure if present
        if !actual_summary_logs.is_empty() {
            let mut expect_summaries = Vec::new();
            for log in expect_summary_logs {
                let summary: dt_connector::check_log::check_log::CheckSummaryLog =
                    serde_json::from_str(&log).map_err(|e| {
                        anyhow::anyhow!("Failed to parse expect summary log: {}, error: {}", log, e)
                    })?;
                expect_summaries.push(summary);
            }

            let mut actual_summaries = Vec::new();
            for log in actual_summary_logs {
                let summary: dt_connector::check_log::check_log::CheckSummaryLog =
                    serde_json::from_str(&log).map_err(|e| {
                        anyhow::anyhow!("Failed to parse actual summary log: {}, error: {}", log, e)
                    })?;
                actual_summaries.push(summary);
            }

            for (expect, actual) in expect_summaries.iter().zip(actual_summaries.iter()) {
                assert_eq!(
                    expect.is_consistent, actual.is_consistent,
                    "is_consistent mismatch"
                );
                assert_eq!(expect.miss_count, actual.miss_count, "miss_count mismatch");
                assert_eq!(expect.diff_count, actual.diff_count, "diff_count mismatch");
                assert_eq!(
                    expect.extra_count, actual.extra_count,
                    "extra_count mismatch"
                );
                assert_eq!(expect.sql_count, actual.sql_count, "sql_count mismatch");
            }
        }

        Ok(())
    }

    pub fn clear_check_log(dst_check_log_dir: &str) {
        if dst_check_log_dir.is_empty() {
            return;
        }
        let files = [
            "miss.log",
            "diff.log",
            "extra.log",
            "summary.log",
            "sql.log",
        ];
        for file in files {
            let log_file = format!("{}/{}", dst_check_log_dir, file);
            if BaseTestRunner::check_path_exists(&log_file) {
                File::create(&log_file).unwrap().set_len(0).unwrap();
            }
        }
    }

    pub fn get_check_log_dir(base_test_runner: &BaseTestRunner, version: &str) -> (String, String) {
        let mut expect_check_log_dir = format!("{}/expect_check_log", base_test_runner.test_dir);
        if !BaseTestRunner::check_path_exists(&expect_check_log_dir)
            && base_test_runner.get_config().sinker_basic.db_type == DbType::Mysql
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

        let dst_check_log_dir = match base_test_runner.get_config().sinker {
            SinkerConfig::MysqlCheck { check_log_dir, .. }
            | SinkerConfig::PgCheck { check_log_dir, .. }
            | SinkerConfig::MongoCheck { check_log_dir, .. } => check_log_dir.clone(),
            _ => String::new(),
        };
        (expect_check_log_dir, dst_check_log_dir)
    }

    fn load_check_log(
        log_dir: &str,
    ) -> (
        HashSet<String>,
        HashSet<String>,
        HashSet<String>,
        HashSet<String>,
        HashSet<String>,
    ) {
        let miss_log_file = format!("{}/miss.log", log_dir);
        let diff_log_file = format!("{}/diff.log", log_dir);
        let extra_log_file = format!("{}/extra.log", log_dir);
        let summary_log_file = format!("{}/summary.log", log_dir);
        let sql_log_file = format!("{}/sql.log", log_dir);

        let mut miss_logs = HashSet::new();
        let mut diff_logs = HashSet::new();
        let mut extra_logs = HashSet::new();
        let mut summary_logs = HashSet::new();
        let mut sql_logs = HashSet::new();

        for log in BaseTestRunner::load_file(&miss_log_file) {
            if let Ok(normalized) = Self::normalize_log(&log) {
                miss_logs.insert(normalized);
            } else {
                miss_logs.insert(log);
            }
        }
        for log in BaseTestRunner::load_file(&diff_log_file) {
            if let Ok(normalized) = Self::normalize_log(&log) {
                diff_logs.insert(normalized);
            } else {
                diff_logs.insert(log);
            }
        }
        for log in BaseTestRunner::load_file(&extra_log_file) {
            extra_logs.insert(log);
        }
        for log in BaseTestRunner::load_file(&summary_log_file) {
            summary_logs.insert(log);
        }
        for log in BaseTestRunner::load_file(&sql_log_file) {
            sql_logs.insert(log);
        }
        (miss_logs, diff_logs, extra_logs, summary_logs, sql_logs)
    }

    fn normalize_log(log: &str) -> anyhow::Result<String> {
        let map: std::collections::BTreeMap<String, Value> = serde_json::from_str(log)?;
        Ok(serde_json::to_string(&map)?)
    }
}
