#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn check_basic_test() {
        TestBase::run_check_test("pg_to_pg/check/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_check_basic_test() {
        TestBase::run_cdc_check_test("pg_to_pg/check/cdc_check_basic_test", 3000, 9000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_check_large_data_test() {
        TestBase::run_cdc_check_test("pg_to_pg/check/cdc_check_large_data_test", 5000, 600000)
            .await;
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

    #[tokio::test]
    #[serial]
    async fn check_recheck_struct_recover_test() {
        TestBase::run_recheck_test("pg_to_pg/check/recheck_struct_recover").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_recheck_basic_test() {
        TestBase::run_check_test("pg_to_pg/check/recheck_basic").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_recheck_recover_test() {
        TestBase::run_recheck_test("pg_to_pg/check/recheck_recover").await;
    }
}
