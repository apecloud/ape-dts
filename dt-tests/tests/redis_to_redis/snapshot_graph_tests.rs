#[cfg(test)]
mod test {
    use crate::test_runner::test_base::TestBase;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn snapshot_cmds_test() {
        TestBase::run_redis_graph_snapshot_test("redis_to_redis/snapshot/graph/cmds_test").await;
    }
}
