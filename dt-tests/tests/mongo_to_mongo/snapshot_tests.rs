#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use mongodb::bson::doc;
    use serial_test::serial;

    use crate::test_runner::mongo_test_runner::MongoTestRunner;
    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn snapshot_basic_test() {
        TestBase::run_mongo_snapshot_test("mongo_to_mongo/snapshot/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_table_parallel_test() {
        TestBase::run_mongo_snapshot_test("mongo_to_mongo/snapshot/table_parallel_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_route_test() {
        TestBase::run_mongo_snapshot_test("mongo_to_mongo/snapshot/route_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_sharding_test() {
        let runner = MongoTestRunner::new("mongo_to_mongo/snapshot/sharding_test")
            .await
            .unwrap();
        runner.run_snapshot_test(true).await.unwrap();
        runner
            .assert_dst_shard_collection(
                "mongo_snapshot_sharding_db.accounts",
                doc! { "tenant_id": 1, "account_id": 1 },
                false,
            )
            .await;
        runner
            .assert_dst_shard_collection(
                "mongo_snapshot_sharding_db.events_hashed",
                doc! { "region": "hashed" },
                false,
            )
            .await;
        runner
            .assert_dst_shard_collection(
                "mongo_snapshot_sharding_db.upsert_accounts",
                doc! { "tenant_id": 1, "account_id": 1 },
                false,
            )
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_sharding_to_standalone_test() {
        let runner = MongoTestRunner::new("mongo_to_mongo/snapshot/sharding_to_standalone_test")
            .await
            .unwrap();
        runner.run_snapshot_test(true).await.unwrap();
        runner
            .assert_dst_collection_exists("mongo_snapshot_sharding_to_standalone_db", "accounts")
            .await;
        runner
            .assert_dst_collection_exists(
                "mongo_snapshot_sharding_to_standalone_db",
                "events_hashed",
            )
            .await;
        runner
            .assert_dst_collection_exists(
                "mongo_snapshot_sharding_to_standalone_db",
                "upsert_accounts",
            )
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_resume_test() {
        TestBase::run_mongo_snapshot_test_and_check_dst_count(
            "mongo_to_mongo/snapshot/resume_log_test",
            resume_expected_counts(),
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_resume_from_db_test() {
        TestBase::run_mongo_snapshot_test_and_check_dst_count(
            "mongo_to_mongo/snapshot/resume_db_test",
            resume_expected_counts(),
        )
        .await;
    }

    fn resume_expected_counts() -> HashMap<(&'static str, &'static str), usize> {
        let mut dst_expected_counts = HashMap::new();
        dst_expected_counts.insert(("test_db_1", "finish_tb_1"), 0);
        dst_expected_counts.insert(("test_db_1", "resume_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "non_resume_tb_1"), 3);
        dst_expected_counts.insert(("test_db_1", "finish_tb_in_log_1"), 0);
        dst_expected_counts.insert(("test_db_1", "resume_tb_in_log_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_string_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_int32_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_int64_in_log_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_datetime_in_log_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_binary_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_decimal_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_document_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_minmax_in_log_tb_1"), 1);
        dst_expected_counts
    }
}
