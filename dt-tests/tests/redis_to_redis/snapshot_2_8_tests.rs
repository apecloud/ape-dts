#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn snapshot_cmds_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/2_8/cmds_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_hash_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/2_8/hash_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_list_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/2_8/list_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_set_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/2_8/set_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_string_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/2_8/string_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_zset_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/2_8/zset_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_length_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/2_8/length_test").await;
    }
}
