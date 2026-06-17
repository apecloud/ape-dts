#[cfg(test)]
mod test {
    use mongodb::bson::doc;
    use serial_test::serial;

    use crate::test_runner::{mongo_test_runner::MongoTestRunner, test_base::TestBase};

    #[tokio::test]
    #[serial]
    async fn cdc_op_log_test() {
        TestBase::run_mongo_cdc_test("mongo_to_mongo/cdc/op_log_test", 3000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_change_stream_test() {
        TestBase::run_mongo_cdc_test("mongo_to_mongo/cdc/change_stream_test", 3000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_changestream_ddl_test() {
        TestBase::run_mongo_changestream_ddl_test(
            "mongo_to_mongo/cdc/changestream_ddl_test",
            3000,
            5000,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_sharding_test() {
        let runner = MongoTestRunner::new("mongo_to_mongo/cdc/sharding_test")
            .await
            .unwrap();
        runner.run_cdc_in_order_test(3000, 8000).await.unwrap();
        runner
            .assert_dst_shard_collection(
                "sharding_cdc_db.accounts",
                doc! { "tenant_id": 1, "account_id": 1, "region": 1 },
                false,
            )
            .await;
        runner
            .assert_dst_shard_collection(
                "sharding_cdc_db.events_hashed",
                doc! { "region": "hashed" },
                false,
            )
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_resume_test() {
        TestBase::run_mongo_cdc_resume_test("mongo_to_mongo/cdc/resume_test", 3000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_idempotent_test() {
        TestBase::run_mongo_cdc_test("mongo_to_mongo/cdc/idempotent_test", 3000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_serial_test() {
        TestBase::run_mongo_cdc_test("mongo_to_mongo/cdc/serial_sink_test", 3000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_route_test() {
        TestBase::run_mongo_cdc_test("mongo_to_mongo/cdc/route_test", 3000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_heartbeat_test() {
        TestBase::run_mongo_heartbeat_test("mongo_to_mongo/cdc/heartbeat_test", 3000, 3000).await;
    }
}
