#[cfg(test)]
mod test {

    use crate::test_runner::test_base::TestBase;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn cdc_6_2_to_7_0_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/cross_version/6_2_to_7_0", 2000, 3000)
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_6_0_to_7_0_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/cross_version/6_0_to_7_0", 2000, 3000)
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_5_0_to_7_0_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/cross_version/5_0_to_7_0", 2000, 3000)
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_4_0_to_7_0_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/cross_version/4_0_to_7_0", 2000, 3000)
            .await;
    }

    // Disabled temporarily: local Redis 2.8 image replacement is not ready yet.
    // #[tokio::test]
    // #[serial]
    // async fn cdc_2_8_to_7_0_test() {
    //     TestBase::run_redis_cdc_test("redis_to_redis/cdc/cross_version/2_8_to_7_0", 2000, 3000)
    //         .await;
    // }
}
