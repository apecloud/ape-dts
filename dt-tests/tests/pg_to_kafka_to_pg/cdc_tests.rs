#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn cdc_basic_test() {
        TestBase::run_rdb_kafka_rdb_cdc_test("pg_to_kafka_to_pg/cdc/basic_test", 5000, 10000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_rules_filter_test() {
        TestBase::run_rdb_kafka_rdb_cdc_test("pg_to_kafka_to_pg/cdc/rules_filter_test", 5000, 8000)
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_rules_router_test() {
        TestBase::run_rdb_kafka_rdb_cdc_test("pg_to_kafka_to_pg/cdc/rules_router_test", 5000, 8000)
            .await;
    }
}
