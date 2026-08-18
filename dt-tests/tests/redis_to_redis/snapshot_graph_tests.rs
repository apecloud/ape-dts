#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn snapshot_cmds_test() {
        TestBase::run_redis_graph_snapshot_test("redis_to_redis/snapshot/graph/cmds_test").await;
    }
}
