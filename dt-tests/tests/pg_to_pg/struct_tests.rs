#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::{rdb_struct_test_runner::RdbStructTestRunner, test_base::TestBase};

    #[tokio::test]
    #[serial]
    async fn struct_basic_test() {
        TestBase::run_pg_struct_test("pg_to_pg/struct/basic_test").await;
    }

    /// do_structures=database,table
    #[tokio::test]
    #[serial]
    async fn struct_filter_test_1() {
        let mut runner = RdbStructTestRunner::new("pg_to_pg/struct/filter_test_1/src_to_dst")
            .await
            .unwrap();
        runner.run_struct_test_without_check().await.unwrap();
        TestBase::run_check_test("pg_to_pg/struct/filter_test_1/check").await;
    }

    /// do_structures=constraint,index
    #[tokio::test]
    #[serial]
    async fn struct_filter_test_2() {
        TestBase::run_pg_struct_test("pg_to_pg/struct/filter_test_2").await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_postgis_test() {
        TestBase::run_pg_struct_test("pg_to_pg/struct/postgis_test").await;
    }

    // #[tokio::test]
    #[serial]
    async fn struct_route_test() {
        TestBase::run_pg_struct_test("pg_to_pg/struct/route_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_rbac_test() {
        let mut runner = RdbStructTestRunner::new("pg_to_pg/struct/rbac_test")
            .await
            .unwrap();
        runner.run_struct_test_without_check().await.unwrap();
        TestBase::run_dcl_check_test("pg_to_pg/struct/rbac_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_batch_basic_test() {
        TestBase::run_pg_struct_test("pg_to_pg/struct/batch_test/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_batch_bench_test_1() {
        let mut runner =
            RdbStructTestRunner::new("pg_to_pg/struct/batch_test/bench_test_1/src_to_dst")
                .await
                .unwrap();
        runner.run_struct_test_without_check().await.unwrap();
        // TestBase::run_check_test("pg_to_pg/struct/batch_test/bench_test_1/check").await;
    }
}
