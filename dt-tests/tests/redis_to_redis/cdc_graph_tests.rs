#[cfg(test)]
mod test {
    use crate::test_runner::test_base::TestBase;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn cdc_cmds_test() {
        TestBase::run_redis_graph_cdc_test("redis_to_redis/cdc/graph/cmds_test", 2000, 10000)
            .await;
    }
}
