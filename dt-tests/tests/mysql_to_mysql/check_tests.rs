#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn check_basic_test() {
        TestBase::run_check_test("mysql_to_mysql/check/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_check_basic_test() {
        TestBase::run_cdc_check_test("mysql_to_mysql/check/cdc_check_basic_test", 3000, 8000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_check_large_data_test() {
        TestBase::run_cdc_check_test(
            "mysql_to_mysql/check/cdc_check_large_data_test",
            5000,
            120000,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_check_update_delete_test() {
        TestBase::run_cdc_check_test(
            "mysql_to_mysql/check/cdc_check_update_delete_test",
            3000,
            8000,
        )
        .await;
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

    #[tokio::test]
    #[serial]
    async fn check_recheck_basic_test() {
        TestBase::run_check_test("mysql_to_mysql/check/recheck_basic").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_recheck_recover_test() {
        TestBase::run_recheck_test("mysql_to_mysql/check/recheck_recover").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_recheck_struct_recover_test() {
        TestBase::run_recheck_test("mysql_to_mysql/check/recheck_struct_recover").await;
    }
}
