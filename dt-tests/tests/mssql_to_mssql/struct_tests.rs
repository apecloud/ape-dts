#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    #[ignore = "MSSQL struct metadata fetch, DDL generation/sinking, and comparison are not implemented"]
    async fn struct_basic_test() {
        TestBase::run_mssql_struct_test("mssql_to_mssql/struct/basic_test").await;
    }
}
