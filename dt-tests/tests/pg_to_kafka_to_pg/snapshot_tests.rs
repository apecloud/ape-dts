#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn snapshot_basic_test() {
        TestBase::run_rdb_kafka_rdb_snapshot_test(
            "pg_to_kafka_to_pg/snapshot/basic_test",
            5000,
            5000,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_rules_filter_test() {
        TestBase::run_rdb_kafka_rdb_snapshot_test(
            "pg_to_kafka_to_pg/snapshot/rules_filter_test",
            5000,
            8000,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_rules_router_test() {
        TestBase::run_rdb_kafka_rdb_snapshot_test(
            "pg_to_kafka_to_pg/snapshot/rules_router_test",
            5000,
            8000,
        )
        .await;
    }
}
