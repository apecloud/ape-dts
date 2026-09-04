#[cfg(test)]
mod test {
    use mongodb::bson::doc;
    use serial_test::serial;

    use crate::test_runner::mongo_test_runner::MongoTestRunner;

    #[tokio::test]
    #[serial]
    async fn tls_require_sharding_snapshot_test() {
        let runner = MongoTestRunner::new("mongo_to_mongo/tls_sharding/require_snapshot")
            .await
            .unwrap();
        runner.run_snapshot_test(true).await.unwrap();
        runner
            .assert_dst_shard_collection(
                "mongo_tls_sharding_snapshot.accounts",
                doc! { "tenant_id": 1, "account_id": 1 },
                false,
            )
            .await;
    }
}
