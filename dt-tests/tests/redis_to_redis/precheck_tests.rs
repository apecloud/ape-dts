#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};

    use crate::test_runner::test_base::TestBase;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn precheck_basic_test() {
        TestBase::run_precheck_test(
            "redis_to_redis/precheck/basic_test",
            &HashSet::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn precheck_cluster_test() {
        TestBase::run_precheck_test(
            "redis_to_redis/precheck/cluster_test",
            &HashSet::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .await;
    }
}
