#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use mongodb::bson::{doc, raw::RawDocumentBuf};
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
