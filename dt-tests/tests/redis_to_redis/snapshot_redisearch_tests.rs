#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    // TODO, fix psync for redisearch
    // #[tokio::test]
    #[serial]
    async fn snapshot_cmds_test() {
        TestBase::run_redis_redisearch_snapshot_test(
            "redis_to_redis/snapshot/redisearch/cmds_test",
        )
        .await;
    }
}
