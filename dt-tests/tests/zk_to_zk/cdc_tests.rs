#[cfg(test)]
mod test {
    use crate::test_runner::{test_base::TestBase, zk_cycle_test_runner::ZkCycleTestRunner};

    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn cdc_basic_test() {
        TestBase::run_zk_cdc_test("zk_to_zk/cdc/basic_test", 3000, 8000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_cycle_basic_test() {
        ZkCycleTestRunner::run_cycle_cdc_test("zk_to_zk/cdc/basic_test", 2000, 10000).await;
    }
}
