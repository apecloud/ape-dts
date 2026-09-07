#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn redis_6_2_tls_cluster_snapshot_and_cdc_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/tls/6_2", 2000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn redis_7_0_tls_verify_ca_snapshot_and_cdc_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/tls/7_0/verify_ca", 2000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn redis_7_0_tls_cluster_require_snapshot_and_cdc_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/tls/7_0/require_cluster", 2000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn redis_8_0_tls_require_snapshot_and_cdc_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/tls/8_0/require", 2000, 3000).await;
    }
}
