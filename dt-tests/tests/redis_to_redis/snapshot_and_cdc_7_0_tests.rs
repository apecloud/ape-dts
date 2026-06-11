#[cfg(test)]
mod test {
    use crate::test_runner::test_base::TestBase;

    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn snapshot_and_cdc_basic_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/snapshot_and_cdc/7_0/basic_test", 2000, 3000)
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_and_cdc_repl_stream_db_test() {
        TestBase::run_redis_cdc_test(
            "redis_to_redis/snapshot_and_cdc/7_0/repl_stream_db_test",
            2000,
            3000,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_and_cdc_db_map_test() {
        TestBase::run_redis_cdc_test(
            "redis_to_redis/snapshot_and_cdc/7_0/db_map_test",
            2000,
            3000,
        )
        .await;
    }
}
