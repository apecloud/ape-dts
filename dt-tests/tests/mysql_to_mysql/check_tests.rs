#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::{test_config_util::TestConfigUtil, test_runner::test_base::TestBase};
    use dt_common::config::{sinker_config::SinkerConfig, task_config::TaskConfig};

    #[tokio::test]
    #[serial]
    async fn check_recheck_recover_test() {
        TestBase::run_check_recover_test(
            "mysql_to_mysql/check/recheck_recover",
            "UPDATE test_db_1.recheck_table SET name='Bob' WHERE id=2",
            2,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn check_basic_test() {
        TestBase::run_check_test("mysql_to_mysql/check/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_all_cols_pk_test() {
        TestBase::run_check_test("mysql_to_mysql/check/all_cols_pk_test").await;
    }
    #[tokio::test]
    #[serial]
    async fn check_basic_struct_test() {
        TestBase::run_check_test("mysql_to_mysql/check/basic_struct_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_revise_struct_test() {
        TestBase::run_check_test("mysql_to_mysql/check/revise_struct_test").await;
    }

    // this should run separately from other tests since it has a different check log dir,
    // all tests will be run in one progress, the log4rs will only be initialized once, it makes this test fails
    #[tokio::test]
    #[ignore]
    async fn set_check_log_dir_test() {
        TestBase::run_check_test("mysql_to_mysql/check/set_check_log_dir_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_route_test() {
        TestBase::run_check_test("mysql_to_mysql/check/route_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_sample_test() {
        TestBase::run_check_test("mysql_to_mysql/check/sample_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_output_full_row_test() {
        TestBase::run_check_test("mysql_to_mysql/check/output_full_row_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_output_revise_sql_test() {
        TestBase::run_check_test("mysql_to_mysql/check/output_revise_sql_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_revise_match_full_row_test() {
        TestBase::run_check_test("mysql_to_mysql/check/revise_match_full_row_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_size_limit_test() {
        // gen log, and verify log size limit
        TestBase::run_check_test("mysql_to_mysql/check/log_size_limit_test").await;
    }

    #[test]
    fn check_recheck_default_config_loaded() {
        let config_path =
            TestConfigUtil::get_absolute_path("mysql_to_mysql/check/basic_test/task_config.ini");
        let config = TaskConfig::new(&config_path).expect("load default check config");

        match config.sinker {
            SinkerConfig::MysqlCheck {
                recheck_interval_secs,
                recheck_attempts,
                ..
            } => {
                assert_eq!(recheck_interval_secs, 0);
                assert_eq!(recheck_attempts, 1);
            }
            _ => panic!("unexpected sinker config variant"),
        }
    }

    #[test]
    fn check_recheck_config_loaded() {
        let config_path = TestConfigUtil::get_absolute_path(
            "mysql_to_mysql/check/recheck_config/task_config.ini",
        );
        let config = TaskConfig::new(&config_path).expect("load recheck config");

        match config.sinker {
            SinkerConfig::MysqlCheck {
                recheck_interval_secs,
                recheck_attempts,
                ..
            } => {
                assert_eq!(recheck_interval_secs, 5);
                assert_eq!(recheck_attempts, 4);
            }
            _ => panic!("unexpected sinker config variant"),
        }
    }

    #[tokio::test]
    #[serial]
    async fn check_recheck_test() {
        TestBase::run_check_test("mysql_to_mysql/check/recheck_config").await;
    }
}
