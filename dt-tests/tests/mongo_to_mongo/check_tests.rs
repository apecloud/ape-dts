#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn check_basic_test() {
        TestBase::run_mongo_check_test("mongo_to_mongo/check/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_check_basic_test() {
        TestBase::run_mongo_cdc_check_test("mongo_to_mongo/check/cdc_check_basic_test", 3000, 3000)
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_check_large_data_test() {
        TestBase::run_mongo_cdc_check_large_data_test(
            "mongo_to_mongo/check/cdc_check_large_data_test",
            5000,
            30000,
            "check_large_test",
            1000,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_check_unhashable_id_test() {
        TestBase::run_mongo_cdc_check_test(
            "mongo_to_mongo/check/cdc_check_unhashable_id_test",
            3000,
            3000,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn check_route_test() {
        TestBase::run_mongo_check_test("mongo_to_mongo/check/route_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_output_full_row_test() {
        TestBase::run_mongo_check_test("mongo_to_mongo/check/output_full_row_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_output_revise_sql_test() {
        TestBase::run_mongo_check_test("mongo_to_mongo/check/output_revise_sql_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_recheck_test() {
        TestBase::run_mongo_check_test("mongo_to_mongo/check/recheck_basic").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_recheck_recover_test() {
        TestBase::run_mongo_recheck_test("mongo_to_mongo/check/recheck_recover").await;
    }
}
