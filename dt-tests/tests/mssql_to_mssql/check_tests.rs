#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn check_basic_test() {
        TestBase::run_check_test("mssql_to_mssql/check/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_inline_basic_test() {
        TestBase::run_check_test("mssql_to_mssql/check/snapshot_inline_basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn basic_struct_test() {
        TestBase::run_check_test("mssql_to_mssql/check/basic_struct_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn consistent_struct_test() {
        TestBase::run_check_test("mssql_to_mssql/check/consistent_struct_test").await;
    }
}
