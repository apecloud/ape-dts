#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn snapshot_cmds_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/4_0/cmds_test").await;
    }
}
