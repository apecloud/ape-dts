#[cfg(test)]
mod test {
    use mongodb::bson::doc;
    use serial_test::serial;

    use crate::test_runner::{mongo_test_runner::MongoTestRunner, test_base::TestBase};

    #[tokio::test]
    #[serial]
    async fn tls_require_snapshot_test() {
        TestBase::run_mongo_snapshot_test("mongo_to_mongo/tls/require_snapshot").await;
    }

    #[tokio::test]
    #[serial]
    async fn tls_require_cdc_test() {
        TestBase::run_mongo_cdc_test("mongo_to_mongo/tls/require_cdc", 2000, 2000).await;
    }

    #[tokio::test]
    #[serial]
    async fn tls_require_struct_test() {
        let runner = MongoTestRunner::new("mongo_to_mongo/tls/require_struct")
            .await
            .unwrap();
        runner.run_struct_test().await.unwrap();
        runner
            .assert_dst_collection_exists("mongo_tls_struct", "accounts")
            .await;
        runner
            .assert_dst_index_exists(
                "mongo_tls_struct",
                "accounts",
                "tenant_account_idx",
                doc! { "tenant_id": 1, "account_id": 1 },
            )
            .await;
        runner
            .assert_dst_collection_option_bool("mongo_tls_struct", "accounts", "capped", true)
            .await;
    }
}
