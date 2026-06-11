#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn snapshot_mock_test() {
        TestBase::run_snapshot_test("mock_test/pg_to_pg/snapshot").await;
    }
}
