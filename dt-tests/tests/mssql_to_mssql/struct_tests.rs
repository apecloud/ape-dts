#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn struct_basic_test() {
        TestBase::run_mssql_struct_test("mssql_to_mssql/struct/basic_test").await;
    }

    /// do_structures=database,table
    #[tokio::test]
    #[serial]
    async fn struct_filter_test_1() {
        TestBase::run_mssql_struct_test("mssql_to_mssql/struct/filter_test_1").await;
    }

    /// do_structures=constraint,index
    #[tokio::test]
    #[serial]
    async fn struct_filter_test_2() {
        TestBase::run_mssql_struct_test("mssql_to_mssql/struct/filter_test_2").await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_collation_test() {
        TestBase::run_mssql_struct_test("mssql_to_mssql/struct/collation_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_route_test() {
        TestBase::run_mssql_struct_test("mssql_to_mssql/struct/route_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_batch_basic_test() {
        TestBase::run_mssql_struct_test("mssql_to_mssql/struct/batch_test/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_batch_bench_test_1() {
        TestBase::run_mssql_struct_test("mssql_to_mssql/struct/batch_test/bench_test_1").await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_ssl_test() {
        TestBase::run_mssql_struct_test("mssql_to_mssql/struct/ssl_test").await;
    }
}
