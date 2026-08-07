#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    #[ignore = "MSSQL snapshot core is scaffolded only"]
    async fn snapshot_basic_test() {
        TestBase::run_snapshot_test("mssql_to_mssql/snapshot/basic_test").await;
    }
}
