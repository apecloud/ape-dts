#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

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
