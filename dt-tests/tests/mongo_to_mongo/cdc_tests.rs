#[cfg(test)]
mod test {
    use dt_common::utils::time_util::TimeUtil;
    use mongodb::bson::{doc, oid::ObjectId, raw::RawDocumentBuf};
    use serial_test::serial;

    use crate::test_runner::{mongo_test_runner::MongoTestRunner, test_base::TestBase};

    #[tokio::test]
    #[serial]
    async fn cdc_op_log_test() {
        TestBase::run_mongo_cdc_test("mongo_to_mongo/cdc/oplog_test", 3000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_op_log_invalid_utf8_keeps_payload_raw() {
        let runner = MongoTestRunner::new("mongo_to_mongo/cdc/oplog_utf8_test")
            .await
            .unwrap();
        runner.execute_prepare_sqls().await.unwrap();
        let task = runner.base.spawn_task().await.unwrap();
        TimeUtil::sleep_millis(2000).await;

        let id = ObjectId::parse_str("65733a82fb2ce9836745de04").unwrap();
        let src_raw_collection = runner
            .src_mongo_client()
            .database("utf8_oplog_db")
            .collection::<RawDocumentBuf>("docs");
        let dst_raw_collection = runner
            .dst_mongo_client()
            .database("utf8_oplog_db")
            .collection::<RawDocumentBuf>("docs");
        let src_collection = runner
            .src_mongo_client()
            .database("utf8_oplog_db")
            .collection::<mongodb::bson::Document>("docs");
        let dst_collection = runner
            .dst_mongo_client()
            .database("utf8_oplog_db")
            .collection::<mongodb::bson::Document>("docs");

        dst_collection
            .insert_one(doc! {
                "_id": "valid_duplicate",
                "status": "target",
                "target_only": true,
            })
            .await
            .unwrap();
        src_collection
            .insert_one(doc! {
                "_id": "valid_duplicate",
                "status": "source",
            })
            .await
            .unwrap();
        TimeUtil::sleep_millis(3000).await;
        let valid_duplicate = dst_collection
            .find_one(doc! { "_id": "valid_duplicate" })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(valid_duplicate.get_str("status").unwrap(), "source");
        assert!(valid_duplicate.get_bool("target_only").unwrap());

        src_raw_collection
            .insert_one(invalid_utf8_raw_document(4))
            .await
            .unwrap();
        TimeUtil::sleep_millis(3000).await;
        let src_inserted = src_raw_collection
            .find_one(doc! { "_id": id })
            .await
            .unwrap()
            .unwrap();
        let dst_inserted = dst_raw_collection
            .find_one(doc! { "_id": id })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(dst_inserted.as_bytes(), src_inserted.as_bytes());

        dst_collection
            .update_one(doc! { "_id": id }, doc! { "$set": { "target_only": true } })
            .await
            .unwrap();
        src_collection
            .update_one(
                doc! { "_id": id },
                doc! { "$set": { "updated_by_oplog": true } },
            )
            .await
            .unwrap();
        TimeUtil::sleep_millis(3000).await;
        let dst_updated = dst_raw_collection
            .find_one(doc! { "_id": id })
            .await
            .unwrap()
            .unwrap();
        assert!(dst_updated.get("value").is_err());
        assert!(dst_updated.get_bool("updated_by_oplog").unwrap());
        assert!(dst_updated.get_bool("target_only").unwrap());

        src_collection.delete_one(doc! { "_id": id }).await.unwrap();
        TimeUtil::sleep_millis(3000).await;
        assert!(dst_raw_collection
            .find_one(doc! { "_id": id })
            .await
            .unwrap()
            .is_none());

        runner.base.abort_task(&task).await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn cdc_change_stream_test() {
        TestBase::run_mongo_cdc_test("mongo_to_mongo/cdc/changestream_test", 3000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_change_stream_invalid_utf8_uses_raw_replacement() {
        let runner = MongoTestRunner::new("mongo_to_mongo/cdc/changestream_utf8_error_test")
            .await
            .unwrap();
        runner.execute_prepare_sqls().await.unwrap();
        let task = runner.base.spawn_task().await.unwrap();
        TimeUtil::sleep_millis(2000).await;

        let id = ObjectId::parse_str("65733a82fb2ce9836745de03").unwrap();
        let src_raw_collection = runner
            .src_mongo_client()
            .database("utf8_cdc_db")
            .collection::<RawDocumentBuf>("docs");
        let dst_raw_collection = runner
            .dst_mongo_client()
            .database("utf8_cdc_db")
            .collection::<RawDocumentBuf>("docs");
        let src_collection = runner
            .src_mongo_client()
            .database("utf8_cdc_db")
            .collection::<mongodb::bson::Document>("docs");
        let dst_collection = runner
            .dst_mongo_client()
            .database("utf8_cdc_db")
            .collection::<mongodb::bson::Document>("docs");

        dst_collection
            .insert_one(doc! {
                "_id": "valid_duplicate",
                "status": "target",
                "target_only": true,
            })
            .await
            .unwrap();
        src_collection
            .insert_one(doc! {
                "_id": "valid_duplicate",
                "status": "source",
            })
            .await
            .unwrap();
        TimeUtil::sleep_millis(3000).await;
        let valid_duplicate = dst_collection
            .find_one(doc! { "_id": "valid_duplicate" })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(valid_duplicate.get_str("status").unwrap(), "source");
        assert_eq!(valid_duplicate.get_bool("target_only").unwrap(), true);

        src_raw_collection
            .insert_one(invalid_utf8_raw_document(3))
            .await
            .unwrap();
        TimeUtil::sleep_millis(3000).await;

        let src_inserted = src_raw_collection
            .find_one(doc! { "_id": id })
            .await
            .unwrap()
            .unwrap();
        let dst_inserted = dst_raw_collection
            .find_one(doc! { "_id": id })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(dst_inserted.as_bytes(), src_inserted.as_bytes());

        runner
            .dst_mongo_client()
            .database("utf8_cdc_db")
            .collection::<mongodb::bson::Document>("docs")
            .update_one(doc! { "_id": id }, doc! { "$set": { "target_only": true } })
            .await
            .unwrap();
        runner
            .src_mongo_client()
            .database("utf8_cdc_db")
            .collection::<mongodb::bson::Document>("docs")
            .update_one(
                doc! { "_id": id },
                doc! { "$set": { "updated_by_cdc": true } },
            )
            .await
            .unwrap();
        TimeUtil::sleep_millis(3000).await;

        let src_updated = src_raw_collection
            .find_one(doc! { "_id": id })
            .await
            .unwrap()
            .unwrap();
        let dst_updated = dst_raw_collection
            .find_one(doc! { "_id": id })
            .await
            .unwrap()
            .unwrap();
        assert!(src_updated.get("value").is_err());
        assert!(dst_updated.get("value").is_err());
        assert_eq!(dst_updated.get_bool("updated_by_cdc").unwrap(), true);
        assert_eq!(dst_updated.get_bool("target_only").unwrap(), true);

        src_collection
            .update_one(
                doc! { "_id": id },
                vec![doc! { "$set": { "invalid_copy": "$value" } }],
            )
            .await
            .unwrap();
        TimeUtil::sleep_millis(3000).await;

        let src_fallback = src_raw_collection
            .find_one(doc! { "_id": id })
            .await
            .unwrap()
            .unwrap();
        let dst_fallback = dst_raw_collection
            .find_one(doc! { "_id": id })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(dst_fallback.as_bytes(), src_fallback.as_bytes());
        assert!(dst_fallback.get("invalid_copy").is_err());
        assert!(dst_fallback.get("target_only").unwrap().is_none());

        dst_collection
            .update_one(doc! { "_id": id }, doc! { "$set": { "target_only": true } })
            .await
            .unwrap();
        src_raw_collection
            .replace_one(doc! { "_id": id }, invalid_utf8_raw_document(3))
            .await
            .unwrap();
        TimeUtil::sleep_millis(3000).await;

        let src_replaced = src_raw_collection
            .find_one(doc! { "_id": id })
            .await
            .unwrap()
            .unwrap();
        let dst_replaced = dst_raw_collection
            .find_one(doc! { "_id": id })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(dst_replaced.as_bytes(), src_replaced.as_bytes());
        assert!(dst_replaced.get("target_only").unwrap().is_none());
        assert!(dst_replaced.get("updated_by_cdc").unwrap().is_none());

        runner.base.abort_task(&task).await.unwrap();
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

        let task = runner.base.spawn_task().await.unwrap();
        TimeUtil::sleep_millis(2000).await;
        let src_raw_collection = runner
            .src_mongo_client()
            .database("sharding_cdc_db")
            .collection::<RawDocumentBuf>("accounts");
        let dst_raw_collection = runner
            .dst_mongo_client()
            .database("sharding_cdc_db")
            .collection::<RawDocumentBuf>("accounts");
        let src_collection = runner
            .src_mongo_client()
            .database("sharding_cdc_db")
            .collection::<mongodb::bson::Document>("accounts");
        let dst_collection = runner
            .dst_mongo_client()
            .database("sharding_cdc_db")
            .collection::<mongodb::bson::Document>("accounts");

        dst_collection
            .insert_one(doc! {
                "_id": "valid_shard_duplicate",
                "tenant_id": "tenant_duplicate",
                "account_id": 102,
                "region": "duplicate_region",
                "status": "target",
                "target_only": true,
            })
            .await
            .unwrap();
        src_collection
            .insert_one(doc! {
                "_id": "valid_shard_duplicate",
                "tenant_id": "tenant_duplicate",
                "account_id": 102,
                "region": "duplicate_region",
                "status": "source",
            })
            .await
            .unwrap();
        TimeUtil::sleep_millis(3000).await;
        let valid_duplicate = dst_collection
            .find_one(doc! {
                "tenant_id": "tenant_duplicate",
                "account_id": 102,
                "region": "duplicate_region",
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(valid_duplicate.get_str("status").unwrap(), "source");
        assert_eq!(valid_duplicate.get_bool("target_only").unwrap(), true);

        src_raw_collection
            .insert_one(invalid_utf8_sharded_document())
            .await
            .unwrap();
        TimeUtil::sleep_millis(3000).await;
        runner
            .dst_mongo_client()
            .database("sharding_cdc_db")
            .collection::<mongodb::bson::Document>("accounts")
            .update_one(
                doc! {
                    "tenant_id": "tenant_raw",
                    "account_id": 101,
                    "region": "raw_region",
                },
                doc! { "$set": { "target_only": true } },
            )
            .await
            .unwrap();
        runner
            .src_mongo_client()
            .database("sharding_cdc_db")
            .collection::<mongodb::bson::Document>("accounts")
            .update_one(
                doc! {
                    "tenant_id": "tenant_raw",
                    "account_id": 101,
                    "region": "raw_region",
                },
                doc! { "$set": { "status": "updated" } },
            )
            .await
            .unwrap();
        TimeUtil::sleep_millis(3000).await;

        let filter = doc! { "_id": "raw_shard_1" };
        let src_updated = src_raw_collection
            .find_one(filter.clone())
            .await
            .unwrap()
            .unwrap();
        let dst_updated = dst_raw_collection
            .find_one(filter.clone())
            .await
            .unwrap()
            .unwrap();
        assert!(src_updated.get("value").is_err());
        assert!(dst_updated.get("value").is_err());
        assert_eq!(dst_updated.get_str("status").unwrap(), "updated");
        assert_eq!(dst_updated.get_bool("target_only").unwrap(), true);

        let raw_shard_filter = doc! {
            "tenant_id": "tenant_raw",
            "account_id": 101,
            "region": "raw_region",
        };
        src_collection
            .update_one(
                raw_shard_filter.clone(),
                vec![doc! { "$set": { "invalid_copy": "$value" } }],
            )
            .await
            .unwrap();
        TimeUtil::sleep_millis(3000).await;

        let src_fallback = src_raw_collection
            .find_one(filter.clone())
            .await
            .unwrap()
            .unwrap();
        let dst_fallback = dst_raw_collection
            .find_one(filter.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(dst_fallback.as_bytes(), src_fallback.as_bytes());
        assert!(dst_fallback.get("invalid_copy").is_err());
        assert!(dst_fallback.get("target_only").unwrap().is_none());

        dst_collection
            .update_one(
                raw_shard_filter.clone(),
                doc! { "$set": { "target_only": true } },
            )
            .await
            .unwrap();
        src_raw_collection
            .replace_one(raw_shard_filter, invalid_utf8_sharded_document())
            .await
            .unwrap();
        TimeUtil::sleep_millis(3000).await;

        let src_replaced = src_raw_collection
            .find_one(filter.clone())
            .await
            .unwrap()
            .unwrap();
        let dst_replaced = dst_raw_collection.find_one(filter).await.unwrap().unwrap();
        assert_eq!(dst_replaced.as_bytes(), src_replaced.as_bytes());
        assert!(dst_replaced.get("target_only").unwrap().is_none());
        runner.base.abort_task(&task).await.unwrap();
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

    fn invalid_utf8_raw_document(id_last_byte: u8) -> RawDocumentBuf {
        let mut bytes = vec![
            0x23, 0x00, 0x00, 0x00, 0x07, b'_', b'i', b'd', 0x00, 0x65, 0x73, 0x3a, 0x82, 0xfb,
            0x2c, 0xe9, 0x83, 0x67, 0x45, 0xde, 0x01, 0x02, b'v', b'a', b'l', b'u', b'e', 0x00,
            0x02, 0x00, 0x00, 0x00, 0xff, 0x00, 0x00,
        ];
        bytes[20] = id_last_byte;
        RawDocumentBuf::from_bytes(bytes).unwrap()
    }

    fn invalid_utf8_sharded_document() -> RawDocumentBuf {
        let mut bytes = RawDocumentBuf::from_document(&doc! {
            "_id": "raw_shard_1",
            "tenant_id": "tenant_raw",
            "account_id": 101,
            "region": "raw_region",
            "status": "inserted",
            "value": "invalid_utf8_marker",
        })
        .unwrap()
        .into_bytes();
        let marker = b"invalid_utf8_marker\0";
        let value_offset = bytes
            .windows(marker.len())
            .position(|window| window == marker)
            .unwrap();
        bytes[value_offset] = 0xff;
        RawDocumentBuf::from_bytes(bytes).unwrap()
    }
}
