#[cfg(test)]
mod test {
    use crate::test_runner::{redis_cycle_test_runner::RedisCycleTestRunner, test_base::TestBase};

    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn cdc_basic_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/7_0/basic_test", 2000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_multi_dbs_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/7_0/multi_dbs_test", 2000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_multi_exec_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/7_0/multi_exec_test", 2000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_cmds_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/7_0/cmds_test", 2000, 5000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_filter_db_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/7_0/filter_db_test", 2000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_to_cluster_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/7_0/to_cluster_test", 2000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_from_cluster_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/7_0/from_cluster_test", 2000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_heartbeat_test() {
        TestBase::run_redis_heartbeat_test("redis_to_redis/cdc/7_0/heartbeat_test", 2000, 2000)
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_cycle_basic_test() {
        RedisCycleTestRunner::run_cycle_cdc_test(
            "redis_to_redis/cdc/7_0/cycle_basic_test",
            1000,
            2000,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_cycle_net_test() {
        RedisCycleTestRunner::run_cycle_cdc_test(
            "redis_to_redis/cdc/7_0/cycle_net_test",
            1000,
            2000,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_cycle_star_test() {
        RedisCycleTestRunner::run_cycle_cdc_test(
            "redis_to_redis/cdc/7_0/cycle_star_test",
            1000,
            2000,
        )
        .await;
    }
}
