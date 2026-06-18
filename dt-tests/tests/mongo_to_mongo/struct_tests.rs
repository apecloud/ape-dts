#[cfg(test)]
mod test {
    use mongodb::bson::doc;
    use serial_test::serial;

    use crate::test_runner::mongo_test_runner::MongoTestRunner;

    #[tokio::test]
    #[serial]
    async fn struct_basic_test() {
        let runner = MongoTestRunner::new("mongo_to_mongo/struct/basic_test")
            .await
            .unwrap();
        runner.run_struct_test().await.unwrap();
        runner
            .assert_dst_collection_exists("mongo_struct_basic_db", "accounts")
            .await;
        runner
            .assert_dst_index_exists(
                "mongo_struct_basic_db",
                "accounts",
                "tenant_account_idx",
                doc! { "tenant_id": 1, "account_id": 1 },
            )
            .await;
        runner
            .assert_dst_collection_option_bool("mongo_struct_basic_db", "accounts", "capped", true)
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_route_test() {
        let runner = MongoTestRunner::new("mongo_to_mongo/struct/route_test")
            .await
            .unwrap();
        runner.run_struct_test().await.unwrap();
        runner
            .assert_dst_routed_collection_exists("mongo_struct_route_db", "accounts")
            .await;
        runner
            .assert_dst_collection_exists("mongo_struct_route_db_dst", "accounts_dst")
            .await;
        runner
            .assert_dst_collection_not_exists("mongo_struct_route_db", "accounts")
            .await;
        runner
            .assert_dst_index_exists(
                "mongo_struct_route_db_dst",
                "accounts_dst",
                "tenant_account_idx",
                doc! { "tenant_id": 1, "account_id": 1 },
            )
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_cursor_batch_test() {
        let runner = MongoTestRunner::new("mongo_to_mongo/struct/cursor_batch_test")
            .await
            .unwrap();
        runner.run_struct_test().await.unwrap();
        runner
            .assert_dst_collection_exists("mongo_struct_cursor_batch_db", "coll_000")
            .await;
        runner
            .assert_dst_collection_exists("mongo_struct_cursor_batch_db", "coll_100")
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_options_and_indexes_test() {
        let runner = MongoTestRunner::new("mongo_to_mongo/struct/options_and_indexes_test")
            .await
            .unwrap();
        runner.run_struct_test().await.unwrap();
        runner
            .assert_dst_collection_exists("mongo_struct_options_db", "validated_accounts")
            .await;
        runner
            .assert_dst_collection_option_str(
                "mongo_struct_options_db",
                "validated_accounts",
                "validationLevel",
                "moderate",
            )
            .await;
        runner
            .assert_dst_collection_option_str(
                "mongo_struct_options_db",
                "validated_accounts",
                "validationAction",
                "warn",
            )
            .await;
        runner
            .assert_dst_collection_option_doc_contains(
                "mongo_struct_options_db",
                "validated_accounts",
                "collation",
                doc! { "locale": "en", "strength": 2 },
            )
            .await;
        runner
            .assert_dst_index_exists(
                "mongo_struct_options_db",
                "validated_accounts",
                "tenant_email_unique_idx",
                doc! { "tenant_id": 1, "email": 1 },
            )
            .await;
        runner
            .assert_dst_index_option_bool(
                "mongo_struct_options_db",
                "validated_accounts",
                "tenant_email_unique_idx",
                "unique",
                true,
            )
            .await;
        runner
            .assert_dst_index_exists(
                "mongo_struct_options_db",
                "validated_accounts",
                "age_partial_idx",
                doc! { "age": -1 },
            )
            .await;
        runner
            .assert_dst_index_option_doc(
                "mongo_struct_options_db",
                "validated_accounts",
                "age_partial_idx",
                "partialFilterExpression",
                doc! { "age": { "$gte": 18 } },
            )
            .await;
        runner
            .assert_dst_index_option_bool(
                "mongo_struct_options_db",
                "validated_accounts",
                "email_sparse_idx",
                "sparse",
                true,
            )
            .await;
        runner
            .assert_dst_index_option_i32(
                "mongo_struct_options_db",
                "ttl_events",
                "expire_at_ttl_idx",
                "expireAfterSeconds",
                3600,
            )
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_special_names_test() {
        let runner = MongoTestRunner::new("mongo_to_mongo/struct/special_names_test")
            .await
            .unwrap();
        runner.run_struct_test().await.unwrap();
        runner
            .assert_dst_collection_exists("mongo_struct_special_db", "orders-2026")
            .await;
        runner
            .assert_dst_index_exists(
                "mongo_struct_special_db",
                "orders-2026",
                "tenant.id_order-no_idx",
                doc! { "tenant.id": 1, "order-no": 1 },
            )
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_filter_test() {
        let runner = MongoTestRunner::new("mongo_to_mongo/struct/filter_test")
            .await
            .unwrap();
        runner.run_struct_test().await.unwrap();
        runner
            .assert_dst_collection_exists("mongo_struct_filter_db", "keep_accounts")
            .await;
        runner
            .assert_dst_index_exists(
                "mongo_struct_filter_db",
                "keep_accounts",
                "tenant_idx",
                doc! { "tenant_id": 1 },
            )
            .await;
        runner
            .assert_dst_collection_not_exists("mongo_struct_filter_db", "keep_ignored")
            .await;
        runner
            .assert_dst_collection_not_exists("mongo_struct_filter_db", "drop_accounts")
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_system_collection_filter_test() {
        let runner = MongoTestRunner::new("mongo_to_mongo/struct/system_collection_filter_test")
            .await
            .unwrap();
        runner.run_struct_test().await.unwrap();
        runner
            .assert_dst_collection_exists("mongo_struct_system_filter_db", "normal_accounts")
            .await;
        runner
            .assert_dst_collection_exists("mongo_struct_system_filter_db", "systematic_logs")
            .await;
        runner
            .assert_dst_collection_not_exists("mongo_struct_system_filter_db", "v_normal")
            .await;
        runner
            .assert_dst_collection_not_exists("mongo_struct_system_filter_db", "system.views")
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_sharding_test() {
        let runner = MongoTestRunner::new("mongo_to_mongo/struct/sharding_test")
            .await
            .unwrap();
        runner.run_struct_test().await.unwrap();
        runner
            .assert_dst_collection_exists("mongo_struct_sharding_db", "accounts")
            .await;
        runner
            .assert_dst_index_exists(
                "mongo_struct_sharding_db",
                "accounts",
                "tenant_account_idx",
                doc! { "tenant_id": 1, "account_id": 1 },
            )
            .await;
        runner
            .assert_dst_shard_collection(
                "mongo_struct_sharding_db.accounts",
                doc! { "tenant_id": 1, "account_id": 1 },
                false,
            )
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_sharding_to_standalone_test() {
        let runner = MongoTestRunner::new("mongo_to_mongo/struct/sharding_to_standalone_test")
            .await
            .unwrap();
        runner.run_struct_test().await.unwrap();
        runner
            .assert_dst_collection_exists("mongo_struct_sharding_to_standalone_db", "accounts")
            .await;
        runner
            .assert_dst_index_exists(
                "mongo_struct_sharding_to_standalone_db",
                "accounts",
                "tenant_account_idx",
                doc! { "tenant_id": 1, "account_id": 1 },
            )
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_shardkey_id_test() {
        let runner = MongoTestRunner::new("mongo_to_mongo/struct/shardkey_id_test")
            .await
            .unwrap();
        runner.run_struct_test().await.unwrap();
        runner
            .assert_dst_shard_collection(
                "mongo_struct_shardkey_id_db.by_object_id",
                doc! { "_id": "hashed" },
                false,
            )
            .await;
        runner
            .assert_dst_shard_collection(
                "mongo_struct_shardkey_id_db.by_string_id",
                doc! { "_id": 1 },
                false,
            )
            .await;
    }
}
