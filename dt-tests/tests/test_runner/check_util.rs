use std::{collections::HashSet, fs, fs::File, path::Path};

use dt_common::config::{config_enums::DbType, sinker_config::SinkerConfig};

use super::base_test_runner::BaseTestRunner;

pub struct CheckUtil {}

impl CheckUtil {
    pub fn validate_check_log(
        expect_check_log_dir: &str,
        dst_check_log_dir: &str,
    ) -> anyhow::Result<()> {
        // check result
        let (expect_miss_logs, expect_diff_logs, expect_extra_logs) =
            Self::load_check_log(expect_check_log_dir);
        let (actual_miss_logs, actual_diff_logs, actual_extra_logs) =
            Self::load_check_log(dst_check_log_dir);

        assert_eq!(expect_diff_logs.len(), actual_diff_logs.len());
        assert_eq!(expect_miss_logs.len(), actual_miss_logs.len());
        assert_eq!(expect_extra_logs.len(), actual_extra_logs.len());
        for log in actual_diff_logs {
            println!("expect_diff_log: {}", log);
            assert!(expect_diff_logs.contains(&log))
        }
        for log in actual_miss_logs {
            println!("expect_miss_log: {}", log);
            assert!(expect_miss_logs.contains(&log))
        }
        for log in actual_extra_logs {
            println!("expect_extra_log: {}", log);
            assert!(expect_extra_logs.contains(&log))
        }
        Ok(())
    }

    pub fn clear_check_log(dst_check_log_dir: &str) {
        let files = ["miss.log", "diff.log", "extra.log"];
        for file in files {
            let log_file = format!("{}/{}", dst_check_log_dir, file);
            if let Some(parent) = Path::new(&log_file).parent() {
                let _ = fs::create_dir_all(parent);
            }
            File::create(&log_file).unwrap().set_len(0).unwrap();
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

    fn load_check_log(log_dir: &str) -> (HashSet<String>, HashSet<String>, HashSet<String>) {
        let miss_log_file = format!("{}/miss.log", log_dir);
        let diff_log_file = format!("{}/diff.log", log_dir);
        let extra_log_file = format!("{}/extra.log", log_dir);
        let mut miss_logs = HashSet::new();
        let mut diff_logs = HashSet::new();
        let mut extra_logs = HashSet::new();

        for log in BaseTestRunner::load_file(&miss_log_file) {
            miss_logs.insert(log);
        }
        for log in BaseTestRunner::load_file(&diff_log_file) {
            diff_logs.insert(log);
        }
        for log in BaseTestRunner::load_file(&extra_log_file) {
            extra_logs.insert(log);
        }
        (miss_logs, diff_logs, extra_logs)
    }
}
