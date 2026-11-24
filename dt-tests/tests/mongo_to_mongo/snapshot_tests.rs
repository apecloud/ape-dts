#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use futures::TryStreamExt;
    use mongodb::bson::{doc, raw::RawDocumentBuf, Document};
    use serial_test::serial;

    use crate::test_config_util::TestConfigUtil;
    use crate::test_runner::base_test_runner::BaseTestRunner;
    use crate::test_runner::mongo_test_runner::{MongoTestRunner, DST, SRC};
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
    async fn snapshot_invalid_utf8_preserves_raw_bson() {
        let runner = MongoTestRunner::new("mongo_to_mongo/snapshot/utf8_error_test")
            .await
            .unwrap();
        runner.execute_prepare_sqls().await.unwrap();

        // { _id: ObjectId("65733a82fb2ce9836745de01"), value: <invalid UTF-8 string> }
        let raw_document = invalid_utf8_raw_document(1);
        let expected_raw = raw_document.clone();
        runner
            .src_mongo_client()
            .database("utf8_error_db")
            .collection::<RawDocumentBuf>("docs")
            .insert_one(raw_document)
            .await
            .unwrap();

        runner.base.start_task().await.unwrap();

        let src_collection = runner
            .src_mongo_client()
            .database("utf8_error_db")
            .collection::<RawDocumentBuf>("docs");
        let dst_collection = runner
            .dst_mongo_client()
            .database("utf8_error_db")
            .collection::<RawDocumentBuf>("docs");

        assert_eq!(src_collection.count_documents(doc! {}).await.unwrap(), 1);
        assert_eq!(dst_collection.count_documents(doc! {}).await.unwrap(), 1);

        let src_document = src_collection.find_one(doc! {}).await.unwrap().unwrap();
        let dst_document = dst_collection.find_one(doc! {}).await.unwrap().unwrap();
        assert_eq!(src_document.as_bytes(), expected_raw.as_bytes());
        assert_eq!(dst_document.as_bytes(), expected_raw.as_bytes());
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_invalid_utf8_preserves_raw_bson_after_duplicate_fallback() {
        let runner = MongoTestRunner::new("mongo_to_mongo/snapshot/utf8_error_test")
            .await
            .unwrap();
        runner.execute_prepare_sqls().await.unwrap();

        let raw_document = invalid_utf8_raw_document(2);
        let expected_raw = raw_document.clone();
        let id = mongodb::bson::oid::ObjectId::parse_str("65733a82fb2ce9836745de02").unwrap();
        runner
            .src_mongo_client()
            .database("utf8_error_db")
            .collection::<RawDocumentBuf>("docs")
            .insert_one(raw_document)
            .await
            .unwrap();
        runner
            .dst_mongo_client()
            .database("utf8_error_db")
            .collection::<mongodb::bson::Document>("docs")
            .insert_one(doc! { "_id": id, "stale_target_field": true })
            .await
            .unwrap();

        runner.base.start_task().await.unwrap();

        let dst_document = runner
            .dst_mongo_client()
            .database("utf8_error_db")
            .collection::<RawDocumentBuf>("docs")
            .find_one(doc! { "_id": id })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(dst_document.as_bytes(), expected_raw.as_bytes());
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_sharding_test() {
        let runner = MongoTestRunner::new("mongo_to_mongo/snapshot/sharding_test")
            .await
            .unwrap();
        runner.execute_prepare_sqls().await.unwrap();
        run_zone_struct(&runner).await;
        runner.execute_test_sqls().await.unwrap();
        runner.base.start_task().await.unwrap();

        for collection in ["accounts", "events_hashed", "upsert_accounts"] {
            assert_eq!(
                runner
                    .fetch_data("mongo_snapshot_sharding_db", collection, SRC)
                    .await,
                runner
                    .fetch_data("mongo_snapshot_sharding_db", collection, DST)
                    .await,
                "{collection}"
            );
        }
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

        let target = runner.dst_mongo_client();
        let ns = "mongo_snapshot_sharding_db.duplicate_ids";
        let tags = target.database("config").collection::<Document>("tags");
        let initial_tags: Vec<Document> = tags
            .find(doc! { "ns": ns })
            .sort(doc! { "min": 1 })
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let collection = target
            .database("mongo_snapshot_sharding_db")
            .collection::<Document>("duplicate_ids");
        assert_eq!(
            collection
                .count_documents(doc! { "_id": 42 })
                .await
                .unwrap(),
            2
        );
        for tenant in ["A", "B"] {
            assert_eq!(
                collection
                    .find_one(doc! { "_id": 42, "tenant": tenant })
                    .await
                    .unwrap(),
                Some(doc! { "_id": 42, "tenant": tenant, "bucket": 1 })
            );
        }

        assert_eq!(initial_tags.len(), 2);
        let stats: Vec<Document> = collection
            .aggregate([doc! { "$collStats": { "count": {} } }])
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let counts: HashMap<_, _> = stats
            .iter()
            .map(|stat| {
                let count = stat
                    .get_i64("count")
                    .or_else(|_| stat.get_i32("count").map(i64::from))
                    .unwrap();
                (stat.get_str("shard").unwrap(), count)
            })
            .collect();
        assert_eq!(
            counts,
            HashMap::from([("target-shard-rs", 1), ("target-shard2-rs", 1)])
        );

        run_zone_struct(&runner).await;
        let repeated_tags: Vec<Document> = tags
            .find(doc! { "ns": ns })
            .sort(doc! { "min": 1 })
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(initial_tags, repeated_tags);
    }

    async fn run_zone_struct(runner: &MongoTestRunner) {
        BaseTestRunner::start_task_with_config_file(&runner.base.struct_task_config_file)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_sharding_conflicting_destination_test() {
        let runner = MongoTestRunner::new("mongo_to_mongo/snapshot/sharding_test")
            .await
            .unwrap();
        let config_file = &runner.base.task_config_file;
        TestConfigUtil::update_task_config_2(
            config_file,
            config_file,
            &[
                ("filter", "do_tbs", "mongo_snapshot_sharding_db.duplicate_ids"),
                ("router", "tb_map", "mongo_snapshot_sharding_db.duplicate_ids:mongo_snapshot_sharding_db.conflicting_duplicate_ids"),
            ],
        );

        let error = runner.run_snapshot_test(false).await.unwrap_err();
        assert!(format!("{error:#}").contains("E11000"), "{error:#}");
        let collection = runner
            .dst_mongo_client()
            .database("mongo_snapshot_sharding_db")
            .collection::<Document>("conflicting_duplicate_ids");
        assert_eq!(collection.count_documents(doc! {}).await.unwrap(), 1);
        assert_eq!(
            collection.find_one(doc! {}).await.unwrap().unwrap(),
            doc! { "_id": 42, "tenant": "A", "bucket": 1 }
        );
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_sharding_resume_log_test() {
        let runner = MongoTestRunner::new("mongo_to_mongo/snapshot/sharding_resume_log_test")
            .await
            .unwrap();
        // The fixture pre-shards this target to isolate checkpoint behavior from Struct.
        runner.run_snapshot_test(false).await.unwrap();
        let collection = runner
            .dst_mongo_client()
            .database("mongo_snapshot_resume_dst")
            .collection::<Document>("duplicate_ids");
        assert_eq!(collection.count_documents(doc! {}).await.unwrap(), 3);
        for expected in [
            doc! { "_id": 42, "tenant": "A", "region": "east", "checkpoint_marker": true },
            doc! { "_id": 42, "tenant": "A", "region": "west" },
            doc! { "_id": 43, "tenant": "B", "region": "west" },
        ] {
            assert_eq!(
                collection
                    .find_one(expected.clone())
                    .await
                    .unwrap()
                    .as_ref(),
                Some(&expected),
                "missing expected snapshot document: {expected}"
            );
        }
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
        dst_expected_counts.insert(("test_db_1", "resume_dollar_string_tb_1"), 2);
        dst_expected_counts.insert(("test_db_1", "resume_int32_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_int64_in_log_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_datetime_in_log_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_binary_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_decimal_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_document_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_minmax_in_log_tb_1"), 1);
        dst_expected_counts
    }

    fn invalid_utf8_raw_document(id_last_byte: u8) -> RawDocumentBuf {
        let mut bytes = vec![
            0x23, 0x00, 0x00, 0x00, 0x07, b'_', b'i', b'd', 0x00, 0x65, 0x73, 0x3a, 0x82, 0xfb,
            0x2c, 0xe9, 0x83, 0x67, 0x45, 0xde, 0x01, 0x02, b'v', b'a', b'l', b'u', b'e', 0x00,
            0x02, 0x00, 0x00, 0x00, 0xff, 0x00, 0x00,
        ];
        bytes[20] = id_last_byte;
        RawDocumentBuf::from_bytes(bytes).unwrap()
    }
}
