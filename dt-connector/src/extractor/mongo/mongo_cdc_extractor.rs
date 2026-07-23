use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::Context;
use async_trait::async_trait;
use chrono::Utc;
use mongodb::{
    bson::{doc, raw::RawDocument, raw::RawDocumentBuf, Bson, Document, Timestamp},
    change_stream::event::ResumeToken,
    options::{FullDocumentBeforeChangeType, FullDocumentType},
    Client,
};
use serde_json::json;
use tokio::{sync::Mutex, time::Instant};

use crate::{
    common::mongo::{changestream_parser, oplog_parser},
    extractor::{
        base_extractor::{BaseExtractor, ExtractState},
        resumer::recovery::Recovery,
    },
    Extractor,
};
use dt_common::{
    config::config_enums::DbType,
    log_error, log_info, log_warn,
    meta::{
        col_value::ColValue,
        ddl_meta::{ddl_data::DdlData, ddl_type::DdlType},
        dt_data::DtData,
        mongo::{
            mongo_cdc_source::MongoCdcSource,
            mongo_constant::MongoConstants,
            mongo_ddl::{change_stream_event_to_ddl, raw_change_stream_event_to_ddl},
            mongo_key::MongoKey,
            mongo_version::{get_server_version, MongoServerVersion},
        },
        position::Position,
        row_data::RowData,
        row_type::RowType,
        syncer::Syncer,
    },
    rdb_filter::RdbFilter,
    system_dbs::SystemDb,
    utils::time_util::TimeUtil,
};

pub struct MongoCdcExtractor {
    pub base_extractor: BaseExtractor,
    pub extract_state: ExtractState,
    pub filter: RdbFilter,
    pub resume_token: String,
    pub start_timestamp: u32,
    pub source: MongoCdcSource,
    pub mongo_client: Client,
    pub app_name: String,
    pub heartbeat_interval_secs: u64,
    pub heartbeat_tb: String,
    pub use_raw_document: bool,
    pub syncer: Arc<Mutex<Syncer>>,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

#[async_trait]
impl Extractor for MongoCdcExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        if let Some(recovery) = &self.recovery {
            if let Some(position) = recovery.get_cdc_resume_position().await {
                match &position {
                    Position::MongoCdc {
                        resume_token,
                        operation_time,
                        ..
                    } => {
                        self.resume_token = resume_token.to_owned();
                        self.start_timestamp = operation_time.to_owned();
                        log_info!(
                            "cdc recovery from resume_token:[{}], operation_time:[{}]",
                            resume_token,
                            operation_time
                        );
                        self.base_extractor
                            .push_dt_data(&mut self.extract_state, DtData::Heartbeat {}, position)
                            .await?;
                    }
                    _ => {
                        log_warn!("position:{} is not a valid mongo cdc position", position);
                    }
                }
            }
        }

        log_info!(
            "MongoCdcExtractor starts, resume_token: {}, start_timestamp: {}, source: {:?} ",
            self.resume_token,
            self.start_timestamp,
            self.source,
        );

        // start heartbeat
        self.start_heartbeat(self.base_extractor.shut_down.clone())?;

        match self.source {
            MongoCdcSource::OpLog => self.extract_oplog().await?,
            MongoCdcSource::ChangeStream => self.extract_change_stream().await?,
        }
        self.base_extractor
            .wait_task_finish(&mut self.extract_state)
            .await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.mongo_client.clone().shutdown().await;
        Ok(())
    }
}

impl MongoCdcExtractor {
    async fn extract_oplog(&mut self) -> anyhow::Result<()> {
        if self.use_raw_document {
            self.extract_raw_oplog().await
        } else {
            self.extract_document_oplog().await
        }
    }

    async fn extract_document_oplog(&mut self) -> anyhow::Result<()> {
        let start_timestamp = self.parse_start_timestamp();
        let filter = doc! {
            "ts": { "$gte": start_timestamp }
        };
        let oplog = self
            .mongo_client
            .database("local")
            .collection::<Document>("oplog.rs");
        let mut cursor = oplog
            .find(filter)
            .cursor_type(mongodb::options::CursorType::TailableAwait)
            .await?;

        while cursor.advance().await? {
            let doc: Document = cursor.deserialize_current()?;
            // https://github.com/mongodb/mongo/blob/master/src/mongo/db/repl/oplog.cpp
            // op:
            //     "i" insert
            //     "u" update
            //     "d" delete
            //     "c" db cmd
            //     "n" no op
            //     "xi" insert global index key
            //     "xd" delete global index key

            let op = Self::get_op(&doc);
            let mut row_type = RowType::Insert;
            let mut before = HashMap::new();
            let mut after = HashMap::new();
            let o = doc.get("o");
            let o2 = doc.get("o2");
            let ts = doc.get("ts");
            let ns = doc.get("ns");

            match op.as_str() {
                "i" => {
                    let doc = o.unwrap().as_document().unwrap().clone();
                    Self::insert_id_from_doc(&mut after, &doc);
                    after.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(doc));
                }
                "u" => {
                    row_type = RowType::Update;
                    // for update op log, doc.o contains only diff instead of full doc
                    let after_doc = o.unwrap().as_document().unwrap();
                    if let Some(id_doc) = o2.and_then(|doc| doc.as_document()) {
                        Self::insert_id_from_doc(&mut after, id_doc);
                    }
                    // refer: https://www.mongodb.com/community/forums/t/oplog-update-entry-without-set-and-unset/171771
                    // https://www.mongodb.com/docs/manual/reference/operator/update/#update-operators-1
                    // in MongoDB 4.4 and earlier, after_doc contains $set with all new document fields,
                    // after that, after_doc contains diff with only changed fields.
                    let diff_doc = Self::build_oplog_update_doc(after_doc);

                    if diff_doc.is_empty() {
                        log_error!(
                            "update op_log is neither $set nor $unset, ignore, o2: {:?}, o: {:?}",
                            o2,
                            o
                        );
                        continue;
                    }

                    after.insert(
                        MongoConstants::DIFF_DOC.to_string(),
                        ColValue::MongoDoc(diff_doc.clone()),
                    );
                    before.insert(
                        MongoConstants::DOC.to_string(),
                        ColValue::MongoDoc(o2.unwrap().as_document().unwrap().clone()),
                    );
                }
                "d" => {
                    row_type = RowType::Delete;
                    let doc = o.unwrap().as_document().unwrap().clone();
                    Self::insert_id_from_doc(&mut before, &doc);
                    before.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(doc));
                }
                // TODO, DDL
                "c" | "xi" | "xd" => {
                    // after version 7.0, the oplog generated by deleteMany is "c" instead of "d"
                    let data = Self::extract_oplog_delete_many(&doc);
                    for (row_data, position) in data {
                        self.push_row_to_buf(row_data, position).await.unwrap();
                    }
                    continue;
                }
                "n" => {
                    // TODO, heartbeat
                    // Document({"op": String("n"), "ns": String(""), "o": Document({"msg": String("periodic noop")}), "ts": Timestamp { time: 1693470874, increment: 1 }, "t": Int64(67), "v": Int64(2), "wall": DateTime(2023-08-31 8:34:34.19 +00:00:00)})
                    continue;
                }
                _ => {
                    continue;
                }
            }

            // get db & tb
            let (row_data, position) =
                Self::build_oplog_row_data(&ns, &ts, row_type, before, after);
            self.push_row_to_buf(row_data, position).await?;
        }
        Ok(())
    }

    fn raw_document_key(doc: &RawDocument) -> anyhow::Result<Option<Document>> {
        let Some(id) = doc.get(MongoConstants::ID)? else {
            return Ok(None);
        };
        Ok(Some(doc! { MongoConstants::ID: Bson::try_from(id)? }))
    }

    async fn extract_raw_oplog(&mut self) -> anyhow::Result<()> {
        let filter = doc! {
            "ts": { "$gte": self.parse_start_timestamp() }
        };
        let oplog = self
            .mongo_client
            .database("local")
            .collection::<RawDocumentBuf>("oplog.rs");
        let mut cursor = oplog
            .find(filter)
            .cursor_type(mongodb::options::CursorType::TailableAwait)
            .await?;

        while cursor.advance().await? {
            let event = cursor.current();
            let op = event.get_str("op").unwrap_or_default();

            if matches!(op, "c" | "xi" | "xd") {
                // Keep the uncommon applyOps/command path unchanged. Normal Oplog DML stays raw.
                let event_doc = Document::try_from(event)?;
                for (row_data, position) in Self::extract_oplog_delete_many(&event_doc) {
                    self.push_row_to_buf(row_data, position).await?;
                }
                continue;
            }
            if op == "n" || !matches!(op, "i" | "u" | "d") {
                continue;
            }

            let ns = event.get_str("ns")?;
            let ts = event.get_timestamp("ts")?;
            let oplog_doc = event.get_document("o")?;
            let mut before = HashMap::new();
            let mut after = HashMap::new();
            let row_type = match op {
                "i" => {
                    Self::insert_id_from_raw_doc(&mut after, oplog_doc)?;
                    if let Some(document_key) = Self::raw_document_key(oplog_doc)? {
                        Self::insert_document_key(&mut after, &document_key);
                    }
                    after.insert(
                        MongoConstants::DOC.to_string(),
                        ColValue::MongoRawDoc(oplog_doc.to_raw_document_buf()),
                    );
                    RowType::Insert
                }
                "u" => {
                    let document_key = event
                        .get("o2")?
                        .and_then(|value| value.as_document())
                        .context("mongo update oplog entry missing document o2")?;
                    Self::insert_id_from_raw_doc(&mut after, document_key)?;
                    before.insert(
                        MongoConstants::DOC.to_string(),
                        ColValue::MongoDoc(Document::try_from(document_key)?),
                    );
                    after.insert(
                        MongoConstants::OPLOG_DIFF_DOC.to_string(),
                        ColValue::MongoRawDoc(oplog_doc.to_raw_document_buf()),
                    );
                    RowType::Update
                }
                "d" => {
                    Self::insert_id_from_raw_doc(&mut before, oplog_doc)?;
                    if let Some(document_key) = Self::raw_document_key(oplog_doc)? {
                        Self::insert_document_key(&mut before, &document_key);
                    }
                    before.insert(
                        MongoConstants::DOC.to_string(),
                        ColValue::MongoRawDoc(oplog_doc.to_raw_document_buf()),
                    );
                    RowType::Delete
                }
                _ => unreachable!(),
            };

            let (row_data, position) =
                Self::build_oplog_row_data_from_parts(ns, ts, row_type, before, after);
            self.push_row_to_buf(row_data, position).await?;
        }
        Ok(())
    }

    fn get_op(doc: &Document) -> String {
        if doc.get("op").is_none() || doc.get("op").unwrap().as_str().is_none() {
            return String::new();
        }
        let op = doc.get("op").unwrap().as_str().unwrap();
        op.into()
    }

    fn extract_oplog_delete_many(doc: &Document) -> Vec<(RowData, Position)> {
        // Some(Document({
        //     "applyOps": Array([Document({
        //         "op": String("d"),
        //         "ns": String("test_db_2.tb_1"),
        //         "ui": Binary {
        //             subtype: Uuid,
        //             bytes: [253, 133, 25, 188, 63, 140, 74, 157, 141, 86, 245, 125, 168, 32, 95, 231]
        //         },
        //         "o": Document({
        //             "_id": String("1")
        //         })
        //     }), Document({
        //         "op": String("d"),
        //         "ns": String("test_db_2.tb_1"),
        //         "ui": Binary {
        //             subtype: Uuid,
        //             bytes: [253, 133, 25, 188, 63, 140, 74, 157, 141, 86, 245, 125, 168, 32, 95, 231]
        //         },
        //         "o": Document({
        //             "_id": String("2")
        //         })
        //     })])
        // }))

        let mut data = vec![];
        let o = doc.get("o");
        let ts = doc.get("ts");

        if o.is_none() || o.unwrap().as_document().is_none() {
            return data;
        }

        let doc = o.unwrap().as_document().unwrap();
        if doc.get("applyOps").is_none() {
            return data;
        }

        let apply_ops = doc.get("applyOps").unwrap();
        if apply_ops.as_array().is_none() {
            return data;
        }

        for ops in apply_ops.as_array().unwrap() {
            if ops.as_document().is_none() {
                continue;
            }

            let item = ops.as_document().unwrap();
            let op = Self::get_op(item);
            let ns = item.get("ns");

            if op.as_str() != "d" {
                continue;
            }

            let o = item.get("o");
            let mut before = HashMap::new();
            let doc = o.unwrap().as_document().unwrap().clone();
            let after = HashMap::new();
            Self::insert_id_from_doc(&mut before, &doc);
            before.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(doc));

            data.push(Self::build_oplog_row_data(
                &ns,
                &ts,
                RowType::Delete,
                before,
                after,
            ));
        }
        data
    }

    fn build_oplog_row_data(
        ns: &Option<&Bson>,
        ts: &Option<&Bson>,
        row_type: RowType,
        before: HashMap<String, ColValue>,
        after: HashMap<String, ColValue>,
    ) -> (RowData, Position) {
        let ts = ts.unwrap().as_timestamp().unwrap();
        let ns = ns.unwrap().as_str().unwrap();

        Self::build_oplog_row_data_from_parts(ns, ts, row_type, before, after)
    }

    fn build_oplog_row_data_from_parts(
        ns: &str,
        ts: Timestamp,
        row_type: RowType,
        before: HashMap<String, ColValue>,
        after: HashMap<String, ColValue>,
    ) -> (RowData, Position) {
        let (db, tb) = ns.split_once('.').unwrap();
        let before = if before.is_empty() {
            None
        } else {
            Some(before)
        };
        let after = if after.is_empty() { None } else { Some(after) };

        // get ts for position
        let position = Position::MongoCdc {
            resume_token: String::new(),
            operation_time: ts.time,
            timestamp: Position::format_timestamp_millis(ts.time as i64 * 1000),
        };
        let row_data = RowData::new(db.to_string(), tb.to_string(), 0, row_type, before, after);
        (row_data, position)
    }

    // Event example:
    /*
    {
        _id : { // stores metadata
            "_data" : <BinData|hex string> // resumeToken
        },
        "operationType" : "<operation>", // insert, delete, replace, update, drop, rename, dropDatabase, invalidate
        "fullDocument" : { <document> }, // data after modification, appears in insert, replace, delete, update. Equivalent to the original "o" field
        "ns" : { // namespace
            "db" : "<database>",
            "coll" : "<collection>"
        },
        "to" : { // only valid when operationType == rename, indicates the new namespace after renaming
            "db" : "<database>",
            "coll" : "<collection>"
        },
        "documentKey" : { "_id" : <value> }, // equivalent to o2 field. Appears in insert, replace, delete, update. Normally only contains _id, for sharded collections also includes shard key
        "updateDescription" : { // only appears when operationType == update; represents incremental modification, while replace is complete replacement
            "updatedFields" : { <document> }, // values of updated fields
            "removedFields" : [ "<field>", ... ] // list of removed fields
        },
        "fullDocument" : { <document> }, // if full_document is enabled, is updateLookup, otherwise default
        "clusterTime" : <Timestamp>, // equivalent to ts field
        "txnNumber" : <NumberLong>, // equivalent to txnNumber in oplog, only appears in transactions. txnNumber is monotonically increasing within a logical session
        "lsid" : { // equivalent to lsid field in oplog, only appears in transactions. Logical session id, id of session for the request
               "id" : <UUID>,
               "uid" : <BinData>
           },
        "operationDescription": { // stores index-related info in DDL operations (createIndexes/dropIndexes/collMod, etc.)
          "index": {
             "name": "age_1",
             "hidden": true
          }
        },
        "stateBeforeChange": { // only present in modify event; records status before collMod command
          "collectionOptions": { // namespace options
              "uuid": UUID("47d6baac-eeaa-488b-98ae-893f3abaaf25")
          },
          "indexOptions": { // index options
             "hidden": false
          }
       },
        "wallTime" : <Date>, // equivalent to wall field
    }
    */
    async fn extract_change_stream(&mut self) -> anyhow::Result<()> {
        let (resume_token, start_timestamp) = if self.resume_token.is_empty() {
            (None, Some(self.parse_start_timestamp()))
        } else {
            let token: ResumeToken = serde_json::from_str(&self.resume_token)?;
            (Some(token), None)
        };

        let server_version = get_server_version(&self.mongo_client).await?;
        let supports_change_stream_6_0_features =
            Self::supports_change_stream_6_0_features(&server_version);
        let requests_change_stream_ddl = self.filter_requests_change_stream_ddl();
        let enable_change_stream_ddl =
            requests_change_stream_ddl && supports_change_stream_6_0_features;
        if requests_change_stream_ddl && !supports_change_stream_6_0_features {
            log_warn!(
                "MongoDB {} does not support change stream showExpandedEvents; change stream DDL events will be skipped",
                server_version
            );
        }

        let mut watch = self
            .mongo_client
            .watch()
            .full_document(FullDocumentType::UpdateLookup);
        if supports_change_stream_6_0_features {
            watch = watch.full_document_before_change(FullDocumentBeforeChangeType::WhenAvailable);
        }
        if supports_change_stream_6_0_features {
            watch = watch.show_expanded_events(true);
        }
        if let Some(resume_token) = resume_token {
            watch = watch.start_after(resume_token);
        } else if let Some(start_time) = start_timestamp {
            watch = watch.start_at_operation_time(start_time);
        }
        let mut change_stream = watch.await?.with_type::<RawDocumentBuf>();

        loop {
            let result = change_stream.next_if_any().await?;
            if let Some(raw_event) = result {
                let resume_token = change_stream.resume_token();
                let position = if let Ok(operation_time) = raw_event.get_timestamp("clusterTime") {
                    Position::MongoCdc {
                        resume_token: resume_token
                            .map(|token| json!(token).to_string())
                            .unwrap_or_default(),
                        operation_time: operation_time.time,
                        timestamp: Position::format_timestamp_millis(
                            operation_time.time as i64 * 1000,
                        ),
                    }
                } else {
                    Position::MongoCdc {
                        resume_token: resume_token
                            .map(|token| json!(token).to_string())
                            .unwrap_or_default(),
                        operation_time: 0,
                        timestamp: String::new(),
                    }
                };

                let (db, tb) = Self::parse_raw_change_stream_ns(&raw_event).unwrap_or_default();
                if self.use_raw_document {
                    match Self::build_raw_change_stream_row(&raw_event, db.clone(), tb.clone()) {
                        Ok(Some(row_data)) => {
                            self.push_row_to_buf(row_data, position).await?;
                        }
                        Ok(None) if enable_change_stream_ddl => {
                            if let Some(ddl_data) = raw_change_stream_event_to_ddl(&raw_event) {
                                self.push_change_stream_ddl(ddl_data, position).await?;
                            }
                        }
                        Ok(None) => {}
                        Err(err) => {
                            log_error!(
                                "failed to build raw change stream row, db: {}, table: {}, error: {}",
                                db,
                                tb,
                                err
                            );
                        }
                    }
                    continue;
                } else {
                    let event = raw_event.to_document()?;
                    let operation_type = event.get_str("operationType").unwrap_or("");
                    let mut row_type = RowType::Insert;
                    let mut before = HashMap::new();
                    let mut after = HashMap::new();

                    match operation_type {
                        "insert" => {
                            let document = match event.get_document("fullDocument") {
                                Ok(document) => document.clone(),
                                Err(_) => continue,
                            };
                            if let Ok(document_key) = event.get_document("documentKey") {
                                Self::insert_id_from_doc(&mut after, document_key);
                                Self::insert_document_key(&mut after, document_key);
                            }
                            Self::insert_id_from_doc(&mut after, &document);
                            after.insert(
                                MongoConstants::DOC.to_string(),
                                ColValue::MongoDoc(document),
                            );
                        }

                        "delete" => {
                            row_type = RowType::Delete;
                            let document_key = match event.get_document("documentKey") {
                                Ok(document_key) => document_key.clone(),
                                Err(_) => continue,
                            };
                            Self::insert_id_from_doc(&mut before, &document_key);
                            Self::insert_document_key(&mut before, &document_key);
                            before.insert(
                                MongoConstants::DOC.to_string(),
                                ColValue::MongoDoc(document_key),
                            );
                        }

                        "update" => {
                            row_type = RowType::Update;
                            let document = event.get_document("fullDocument").ok().cloned();
                            let document_key = match event.get_document("documentKey") {
                                Ok(document_key) => document_key.clone(),
                                Err(_) => continue,
                            };
                            Self::insert_id_from_doc(&mut before, &document_key);
                            Self::insert_document_key(&mut before, &document_key);
                            Self::insert_id_from_doc(&mut after, &document_key);
                            Self::insert_document_key(&mut after, &document_key);
                            if let Some(document) = &document {
                                Self::insert_id_from_doc(&mut after, document);
                            }
                            if let Ok(pre_image) = event.get_document("fullDocumentBeforeChange") {
                                before.insert(
                                    MongoConstants::PRE_IMAGE.to_string(),
                                    ColValue::MongoDoc(pre_image.clone()),
                                );
                                before.insert(
                                    MongoConstants::DOC.to_string(),
                                    ColValue::MongoDoc(pre_image.clone()),
                                );
                            }
                            let update_description = match event.get_document("updateDescription") {
                                Ok(update_description) => update_description,
                                Err(_) => continue,
                            };
                            if Self::change_stream_update_requires_full_document(update_description)
                            {
                                // Ambiguous paths may refer to literal dotted field names, so a normal
                                // $set/$unset dotted path can update the wrong shape.
                                let Some(document) = document else {
                                    log_error!(
                                    "change stream updateDescription has disambiguatedPaths, but fullDocument is missing, ignore, event: {:?}",
                                    event
                                );
                                    continue;
                                };
                                after.insert(
                                    MongoConstants::DOC.to_string(),
                                    ColValue::MongoDoc(document),
                                );
                            } else {
                                let update_doc = Self::build_change_stream_update_doc(
                                    update_description,
                                    document.as_ref(),
                                );
                                if update_doc.is_empty() {
                                    log_error!(
                                "change stream updateDescription is empty or unsupported, ignore, event: {:?}",
                                event
                            );
                                    continue;
                                }
                                after.insert(
                                    MongoConstants::DIFF_DOC.to_string(),
                                    ColValue::MongoDoc(update_doc),
                                );
                                if let Some(document) = document {
                                    after.insert(
                                        MongoConstants::DOC.to_string(),
                                        ColValue::MongoDoc(document),
                                    );
                                }
                            }
                        }

                        "replace" => {
                            row_type = RowType::Update;
                            let document = match event.get_document("fullDocument") {
                                Ok(document) => document.clone(),
                                Err(_) => continue,
                            };
                            let document_key = match event.get_document("documentKey") {
                                Ok(document_key) => document_key.clone(),
                                Err(_) => continue,
                            };
                            Self::insert_id_from_doc(&mut before, &document_key);
                            Self::insert_document_key(&mut before, &document_key);
                            Self::insert_id_from_doc(&mut after, &document_key);
                            Self::insert_document_key(&mut after, &document_key);
                            Self::insert_id_from_doc(&mut after, &document);
                            if let Ok(pre_image) = event.get_document("fullDocumentBeforeChange") {
                                before.insert(
                                    MongoConstants::PRE_IMAGE.to_string(),
                                    ColValue::MongoDoc(pre_image.clone()),
                                );
                                before.insert(
                                    MongoConstants::DOC.to_string(),
                                    ColValue::MongoDoc(pre_image.clone()),
                                );
                            }
                            after.insert(
                                MongoConstants::DOC.to_string(),
                                ColValue::MongoDoc(document),
                            );
                        }
                        _ => {
                            if !enable_change_stream_ddl {
                                continue;
                            }
                            if let Some(ddl_data) = change_stream_event_to_ddl(&event) {
                                self.push_change_stream_ddl(ddl_data, position).await?;
                            }
                            continue;
                        }
                    }

                    let before = if before.is_empty() {
                        None
                    } else {
                        Some(before)
                    };
                    let after = if after.is_empty() { None } else { Some(after) };
                    let row_data = RowData::new(db, tb, 0, row_type, before, after);
                    self.push_row_to_buf(row_data, position).await?;
                }
            }
        }
    }

    async fn push_row_to_buf(
        &mut self,
        row_data: RowData,
        position: Position,
    ) -> anyhow::Result<()> {
        if SystemDb::is_system_db(&row_data.schema, &DbType::Mongo) {
            return Ok(());
        }

        if self
            .filter
            .filter_event(&row_data.schema, &row_data.tb, &row_data.row_type)
        {
            self.extract_state.record_extracted_metrics_row(&row_data);
            return self
                .base_extractor
                .push_dt_data(&mut self.extract_state, DtData::Heartbeat {}, position)
                .await;
        }
        self.base_extractor
            .push_row(&mut self.extract_state, row_data, position)
            .await
    }

    fn parse_start_timestamp(&mut self) -> Timestamp {
        let time = if self.start_timestamp > 0 {
            self.start_timestamp
        } else {
            Utc::now().timestamp() as u32
        };
        Timestamp { time, increment: 0 }
    }

    fn start_heartbeat(&mut self, shut_down: Arc<AtomicBool>) -> anyhow::Result<()> {
        let db_tb = self.base_extractor.precheck_heartbeat(
            self.heartbeat_interval_secs,
            &self.heartbeat_tb,
            DbType::Mongo,
        );
        if db_tb.len() != 2 {
            return Ok(());
        }

        self.filter.add_ignore_tb(&db_tb[0], &db_tb[1]);

        let (app_name, heartbeat_interval_secs, syncer, mongo_client) = (
            self.app_name.clone(),
            self.heartbeat_interval_secs,
            self.syncer.clone(),
            self.mongo_client.clone(),
        );

        tokio::spawn(async move {
            let mut start_time = Instant::now();
            while !shut_down.load(Ordering::Acquire) {
                if start_time.elapsed().as_secs() >= heartbeat_interval_secs {
                    Self::heartbeat(&app_name, &db_tb[0], &db_tb[1], &syncer, &mongo_client)
                        .await
                        .unwrap();
                    start_time = Instant::now();
                }
                TimeUtil::sleep_millis(1000 * heartbeat_interval_secs).await;
            }
        });
        log_info!("heartbeat started");
        Ok(())
    }

    async fn heartbeat(
        app_name: &str,
        db: &str,
        tb: &str,
        syncer: &Arc<Mutex<Syncer>>,
        client: &Client,
    ) -> anyhow::Result<()> {
        let (received_resume_token, received_operation_time, received_timestamp) =
            if let Position::MongoCdc {
                resume_token,
                operation_time,
                timestamp,
            } = &syncer.lock().await.received_position
            {
                (
                    resume_token.to_owned(),
                    *operation_time,
                    timestamp.to_owned(),
                )
            } else {
                (String::new(), 0, String::new())
            };
        let (committed_resume_token, committed_operation_time, committed_timestamp) =
            if let Position::MongoCdc {
                resume_token,
                operation_time,
                timestamp,
            } = &syncer.lock().await.committed_position
            {
                (
                    resume_token.to_owned(),
                    *operation_time,
                    timestamp.to_owned(),
                )
            } else {
                (String::new(), 0, String::new())
            };

        let query_doc = doc! {MongoConstants::ID: app_name };
        let update_doc = doc! {MongoConstants::SET: doc! {MongoConstants::ID: app_name,
            "update_timestamp": Position::format_timestamp_millis(Utc::now().timestamp() * 1000),
            "received_resume_token": received_resume_token,
            "received_operation_time": received_operation_time,
            "received_timestamp": received_timestamp,
            "committed_resume_token": committed_resume_token,
            "committed_operation_time": committed_operation_time,
            "committed_timestamp": committed_timestamp,
        }};

        let collection = client.database(db).collection::<Document>(tb);
        if let Err(err) = collection
            .update_one(query_doc, update_doc)
            .upsert(true)
            .await
        {
            log_error!("heartbeat failed: {:?}", err);
        }
        Ok(())
    }
}

impl MongoCdcExtractor {
    fn supports_change_stream_6_0_features(version: &MongoServerVersion) -> bool {
        version >= &MongoServerVersion::new(6, 0, 0)
    }

    fn insert_id_from_doc(target: &mut HashMap<String, ColValue>, doc: &Document) {
        if let Some(key) = MongoKey::from_doc(doc) {
            target.insert(
                MongoConstants::ID.to_string(),
                ColValue::String(key.to_string()),
            );
        }
    }

    fn insert_id_from_raw_doc(
        target: &mut HashMap<String, ColValue>,
        doc: &RawDocument,
    ) -> anyhow::Result<()> {
        if let Some(key) = MongoKey::from_raw_doc(doc)? {
            target.insert(
                MongoConstants::ID.to_string(),
                ColValue::String(key.to_string()),
            );
        }
        Ok(())
    }

    fn insert_document_key(target: &mut HashMap<String, ColValue>, document_key: &Document) {
        target.insert(
            MongoConstants::DOCUMENT_KEY.to_string(),
            ColValue::MongoDoc(document_key.clone()),
        );
    }

    fn build_oplog_update_doc(after_doc: &Document) -> Document {
        oplog_parser::build_update_doc(after_doc)
    }

    fn build_change_stream_update_doc(
        update_description: &Document,
        full_document: Option<&Document>,
    ) -> Document {
        changestream_parser::build_update_doc(update_description, full_document)
    }

    fn change_stream_update_requires_full_document(update_description: &Document) -> bool {
        changestream_parser::requires_full_document(update_description)
    }

    fn parse_raw_change_stream_ns(event: &RawDocument) -> Option<(String, String)> {
        let ns = event.get_document("ns").ok()?;
        let db = ns.get_str("db").ok()?.to_string();
        let tb = ns.get_str("coll").unwrap_or("").to_string();
        Some((db, tb))
    }

    fn build_raw_change_stream_row(
        event: &RawDocument,
        db: String,
        tb: String,
    ) -> anyhow::Result<Option<RowData>> {
        let operation_type = event.get_str("operationType").unwrap_or("");
        let mut before = HashMap::new();
        let mut after = HashMap::new();

        let document_key = || -> anyhow::Result<Document> {
            let document_key = event.get_document("documentKey")?;
            Ok(mongodb::bson::from_slice(document_key.as_bytes())?)
        };

        match operation_type {
            "insert" => {
                let document = event.get_document("fullDocument")?;
                if let Ok(document_key) = document_key() {
                    Self::insert_id_from_doc(&mut after, &document_key);
                    Self::insert_document_key(&mut after, &document_key);
                }
                Self::insert_id_from_raw_doc(&mut after, document)?;
                after.insert(
                    MongoConstants::DOC.to_string(),
                    ColValue::MongoRawDoc(document.to_raw_document_buf()),
                );
                Ok(Some(RowData::new(
                    db,
                    tb,
                    0,
                    RowType::Insert,
                    None,
                    Some(after),
                )))
            }
            "delete" => {
                let document_key = document_key()?;
                Self::insert_id_from_doc(&mut before, &document_key);
                Self::insert_document_key(&mut before, &document_key);
                before.insert(
                    MongoConstants::DOC.to_string(),
                    ColValue::MongoDoc(document_key),
                );
                Ok(Some(RowData::new(
                    db,
                    tb,
                    0,
                    RowType::Delete,
                    Some(before),
                    None,
                )))
            }
            "update" => {
                let document = event.get_document("fullDocument").ok();
                let document_key = document_key()?;
                Self::insert_id_from_doc(&mut before, &document_key);
                Self::insert_document_key(&mut before, &document_key);
                Self::insert_id_from_doc(&mut after, &document_key);
                Self::insert_document_key(&mut after, &document_key);
                if let Some(document) = document {
                    Self::insert_id_from_raw_doc(&mut after, document)?;
                }

                if let Ok(pre_image) = event.get_document("fullDocumentBeforeChange") {
                    before.insert(
                        MongoConstants::PRE_IMAGE.to_string(),
                        ColValue::MongoRawDoc(pre_image.to_raw_document_buf()),
                    );
                }
                if let Some(document) = document {
                    after.insert(
                        MongoConstants::DOC.to_string(),
                        ColValue::MongoRawDoc(document.to_raw_document_buf()),
                    );
                }
                let update_description = event.get_document("updateDescription")?;
                after.insert(
                    MongoConstants::DIFF_DOC.to_string(),
                    ColValue::MongoRawDoc(update_description.to_raw_document_buf()),
                );
                Ok(Some(RowData::new(
                    db,
                    tb,
                    0,
                    RowType::Update,
                    Some(before),
                    Some(after),
                )))
            }
            "replace" => {
                let document = event.get_document("fullDocument")?;
                let document_key = document_key()?;
                Self::insert_id_from_doc(&mut before, &document_key);
                Self::insert_document_key(&mut before, &document_key);
                Self::insert_id_from_doc(&mut after, &document_key);
                Self::insert_document_key(&mut after, &document_key);
                Self::insert_id_from_raw_doc(&mut after, document)?;

                if let Ok(pre_image) = event.get_document("fullDocumentBeforeChange") {
                    before.insert(
                        MongoConstants::PRE_IMAGE.to_string(),
                        ColValue::MongoRawDoc(pre_image.to_raw_document_buf()),
                    );
                }
                after.insert(
                    MongoConstants::DOC.to_string(),
                    ColValue::MongoRawDoc(document.to_raw_document_buf()),
                );
                Ok(Some(RowData::new(
                    db,
                    tb,
                    0,
                    RowType::Update,
                    Some(before),
                    Some(after),
                )))
            }
            _ => Ok(None),
        }
    }

    fn filter_requests_change_stream_ddl(&self) -> bool {
        if self.filter.do_ddls.contains("*") {
            return true;
        }

        [
            DdlType::MongoCreateCollection,
            DdlType::MongoCreateIndex,
            DdlType::MongoDropIndex,
            DdlType::MongoCollMod,
            DdlType::MongoShardCollection,
            DdlType::MongoReshardCollection,
            DdlType::MongoRefineCollectionShardKey,
        ]
        .iter()
        .any(|ddl_type| self.filter.do_ddls.contains(&ddl_type.to_string()))
    }

    async fn push_change_stream_ddl(
        &mut self,
        ddl_data: DdlData,
        position: Position,
    ) -> anyhow::Result<()> {
        let (ddl_db, ddl_tb) = ddl_data.get_schema_tb();
        if !self.filter.filter_ddl(&ddl_db, &ddl_tb, &ddl_data.ddl_type) {
            self.base_extractor
                .push_ddl(&mut self.extract_state, ddl_data, position)
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_oplog_update_doc_merges_insert_update_and_delete_diff() {
        let after_doc = doc! {
            "diff": {
                "i": { "created": true },
                "u": { "name": "new-name" },
                "d": { "removed": false },
            },
        };

        let update_doc = MongoCdcExtractor::build_oplog_update_doc(&after_doc);

        assert_eq!(
            update_doc.get_document(MongoConstants::SET).unwrap(),
            &doc! { "created": true, "name": "new-name" }
        );
        assert_eq!(
            update_doc.get_document(MongoConstants::UNSET).unwrap(),
            &doc! { "removed": false }
        );
    }

    #[test]
    fn build_oplog_update_doc_flattens_nested_sub_diff() {
        let after_doc = doc! {
            "diff": {
                "sprofile": {
                    "u": { "name": "new-name" },
                    "d": { "age": false },
                    "saddress": {
                        "i": { "city": "Hangzhou" },
                    },
                },
            },
        };

        let update_doc = MongoCdcExtractor::build_oplog_update_doc(&after_doc);

        assert_eq!(
            update_doc.get_document(MongoConstants::SET).unwrap(),
            &doc! {
                "profile.name": "new-name",
                "profile.address.city": "Hangzhou",
            }
        );
        assert_eq!(
            update_doc.get_document(MongoConstants::UNSET).unwrap(),
            &doc! { "profile.age": false }
        );
    }

    #[test]
    fn build_oplog_update_doc_keeps_legacy_set_and_unset() {
        let after_doc = doc! {
            MongoConstants::SET: { "name": "new-name" },
            MongoConstants::UNSET: { "age": "" },
        };

        let update_doc = MongoCdcExtractor::build_oplog_update_doc(&after_doc);

        assert_eq!(
            update_doc.get_document(MongoConstants::SET).unwrap(),
            &doc! { "name": "new-name" }
        );
        assert_eq!(
            update_doc.get_document(MongoConstants::UNSET).unwrap(),
            &doc! { "age": "" }
        );
    }

    #[test]
    fn build_change_stream_update_doc_converts_update_description() {
        let update_description = doc! {
            "updatedFields": {
                "name": "new-name",
                "profile.score": 10,
            },
            "removedFields": ["old_field"],
            "truncatedArrays": [
                { "field": "attrs", "newSize": 1 },
            ],
        };
        let full_document = doc! {
            "name": "new-name",
            "profile": { "score": 10 },
            "attrs": ["kept"],
        };

        let update_doc = MongoCdcExtractor::build_change_stream_update_doc(
            &update_description,
            Some(&full_document),
        );

        assert_eq!(
            update_doc.get_document(MongoConstants::SET).unwrap(),
            &doc! {
                "name": "new-name",
                "profile.score": 10,
                "attrs": ["kept"],
            }
        );
        assert_eq!(
            update_doc.get_document(MongoConstants::UNSET).unwrap(),
            &doc! { "old_field": "" }
        );
    }

    #[test]
    fn change_stream_update_requires_full_document_for_literal_dot_path() {
        let update_description = doc! {
            "updatedFields": {
                "home.town": "London",
            },
            "disambiguatedPaths": {
                "home.town": ["home.town"],
            },
        };

        assert!(
            MongoCdcExtractor::change_stream_update_requires_full_document(&update_description)
        );

        let update_description = doc! {
            "updatedFields": {
                "profile.score": 10,
            },
        };

        assert!(
            !MongoCdcExtractor::change_stream_update_requires_full_document(&update_description)
        );
    }

    #[test]
    fn change_stream_disambiguated_paths_keeps_safe_update_paths_as_diff() {
        let update_description = doc! {
            "updatedFields": {
                "profile.score": 10,
                "attrs.0": "first",
            },
            "removedFields": ["profile.old"],
            "disambiguatedPaths": {
                "profile.score": ["profile", "score"],
                "attrs.0": ["attrs", 0],
                "profile.old": ["profile", "old"],
            },
        };

        assert!(
            !MongoCdcExtractor::change_stream_update_requires_full_document(&update_description)
        );
        let update_doc =
            MongoCdcExtractor::build_change_stream_update_doc(&update_description, None);

        assert_eq!(
            update_doc.get_document(MongoConstants::SET).unwrap(),
            &doc! {
                "profile.score": 10,
                "attrs.0": "first",
            }
        );
        assert_eq!(
            update_doc.get_document(MongoConstants::UNSET).unwrap(),
            &doc! { "profile.old": "" }
        );
    }

    #[test]
    fn change_stream_disambiguated_paths_keeps_array_and_numeric_field_paths_as_diff() {
        let update_description = doc! {
            "updatedFields": {
                "scores.2": 99,
                "matrix.0.1": 42,
                "residences.0.0": "street",
                "profile.0": "zero-field",
            },
            "removedFields": ["old_scores.1", "profile.1"],
            "disambiguatedPaths": {
                "scores.2": ["scores", 2],
                "matrix.0.1": ["matrix", 0, 1],
                "residences.0.0": ["residences", 0, "0"],
                "profile.0": ["profile", "0"],
                "old_scores.1": ["old_scores", 1],
                "profile.1": ["profile", "1"],
            },
        };

        assert!(
            !MongoCdcExtractor::change_stream_update_requires_full_document(&update_description)
        );
        let update_doc =
            MongoCdcExtractor::build_change_stream_update_doc(&update_description, None);

        assert_eq!(
            update_doc.get_document(MongoConstants::SET).unwrap(),
            &doc! {
                "scores.2": 99,
                "matrix.0.1": 42,
                "residences.0.0": "street",
                "profile.0": "zero-field",
            }
        );
        assert_eq!(
            update_doc.get_document(MongoConstants::UNSET).unwrap(),
            &doc! {
                "old_scores.1": "",
                "profile.1": "",
            }
        );
    }

    #[test]
    fn change_stream_disambiguated_paths_requires_full_document_for_literal_dot_fields() {
        for update_description in [
            doc! {
                "updatedFields": { "home.town": "London" },
                "disambiguatedPaths": { "home.town": ["home.town"] },
            },
            doc! {
                "updatedFields": { "profile.name.first": "Ada" },
                "disambiguatedPaths": { "profile.name.first": ["profile", "name.first"] },
            },
            doc! {
                "removedFields": ["archive.2026.status"],
                "disambiguatedPaths": { "archive.2026.status": ["archive.2026", "status"] },
            },
        ] {
            assert!(
                MongoCdcExtractor::change_stream_update_requires_full_document(&update_description),
                "update_description should require fullDocument: {:?}",
                update_description
            );
        }
    }

    #[test]
    fn change_stream_disambiguated_paths_requires_full_document_for_malformed_paths() {
        for update_description in [
            doc! {
                "updatedFields": { "profile.score": 10 },
                "disambiguatedPaths": { "profile.score": [] },
            },
            doc! {
                "updatedFields": { "profile.score": 10 },
                "disambiguatedPaths": { "profile.score": "profile.score" },
            },
            doc! {
                "updatedFields": { "profile.score": 10 },
                "disambiguatedPaths": { "profile.score": ["profile", true] },
            },
        ] {
            assert!(
                MongoCdcExtractor::change_stream_update_requires_full_document(&update_description),
                "malformed disambiguated path should require fullDocument: {:?}",
                update_description
            );
        }
    }

    #[test]
    fn raw_change_stream_row_keeps_invalid_full_document_and_valid_diff_separate() {
        let mut bytes = RawDocumentBuf::from_document(&doc! {
            "operationType": "update",
            "ns": { "db": "test_db", "coll": "test_tb" },
            "documentKey": { "_id": 1 },
            "fullDocument": {
                "_id": 1,
                "invalid": "invalid_full_document_marker",
            },
            "updateDescription": {
                "updatedFields": { "status": "updated" },
                "removedFields": [],
                "truncatedArrays": [],
            },
        })
        .unwrap()
        .into_bytes();
        let marker = b"invalid_full_document_marker\0";
        let value_offset = bytes
            .windows(marker.len())
            .position(|window| window == marker)
            .unwrap();
        bytes[value_offset] = 0xff;
        let raw_event = RawDocumentBuf::from_bytes(bytes).unwrap();

        assert!(raw_event.to_document().is_err());
        let row = MongoCdcExtractor::build_raw_change_stream_row(
            &raw_event,
            "test_db".to_string(),
            "test_tb".to_string(),
        )
        .unwrap()
        .unwrap();
        let after = row.after.unwrap();
        let ColValue::MongoRawDoc(full_doc) = after.get(MongoConstants::DOC).unwrap() else {
            panic!("fullDocument should remain raw");
        };
        let ColValue::MongoRawDoc(update_description) =
            after.get(MongoConstants::DIFF_DOC).unwrap()
        else {
            panic!("updateDescription should remain raw");
        };

        assert!(full_doc.to_document().is_err());
        assert_eq!(
            update_description
                .to_document()
                .unwrap()
                .get_document("updatedFields")
                .unwrap(),
            &doc! { "status": "updated" }
        );
    }
}
