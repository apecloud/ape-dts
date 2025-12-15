#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::{test_config_util::TestConfigUtil, test_runner::test_base::TestBase};
    use dt_common::config::{sinker_config::SinkerConfig, task_config::TaskConfig};

    #[tokio::test]
    #[serial]
    async fn check_basic_test() {
        TestBase::run_check_test("pg_to_pg/check/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_route_test() {
        TestBase::run_check_test("pg_to_pg/check/route_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_sample_test() {
        TestBase::run_check_test("pg_to_pg/check/sample_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_output_full_row_test() {
        TestBase::run_check_test("pg_to_pg/check/output_full_row_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_output_revise_sql_test() {
        TestBase::run_check_test("pg_to_pg/check/output_revise_sql_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_revise_match_full_row_test() {
        TestBase::run_check_test("pg_to_pg/check/revise_match_full_row_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_struct_basic_test() {
        TestBase::run_check_test("pg_to_pg/check/basic_struct_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_revise_struct_test() {
        TestBase::run_check_test("pg_to_pg/check/revise_struct_test").await;
    }

    #[test]
    fn check_recheck_default_config_loaded() {
        let config_path =
            TestConfigUtil::get_absolute_path("pg_to_pg/check/basic_test/task_config.ini");
        let config = TaskConfig::new(&config_path).expect("load default check config");

        match config.sinker {
            SinkerConfig::PgCheck {
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
        let config_path =
            TestConfigUtil::get_absolute_path("pg_to_pg/check/recheck_config/task_config.ini");
        let config = TaskConfig::new(&config_path).expect("load recheck config");

        match config.sinker {
            SinkerConfig::PgCheck {
                recheck_interval_secs,
                recheck_attempts,
                ..
            } => {
                assert_eq!(recheck_interval_secs, 2);
                assert_eq!(recheck_attempts, 3);
            }
            _ => panic!("unexpected sinker config variant"),
        }
    }

    #[tokio::test]
    #[serial]
    async fn check_recheck_test() {
        TestBase::run_check_test("pg_to_pg/check/recheck_config").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_recheck_recover_test() {
        TestBase::run_check_recover_test(
            "pg_to_pg/recheck/basic_recheck_config",
            "UPDATE public.recheck_table SET name='Bob' WHERE id=2",
            1,
        )
        .await;
    }
}
