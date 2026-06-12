#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn snapshot_table_parallel_test() {
        TestBase::run_snapshot_test(
            "mock_test/mysql_to_mysql/8_0_to_8_0/snapshot/table_parallel_test",
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_chunk_parallel_test() {
        TestBase::run_snapshot_test("mock_test/mysql_to_mysql/8_0_to_8_0/snapshot/parallel_test")
            .await;
    }
}
