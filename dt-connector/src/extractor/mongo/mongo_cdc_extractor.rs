use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use chrono::Utc;
use serde_json::json;
use tokio::{sync::Mutex, time::Instant};

use crate::{
    extractor::{base_extractor::BaseExtractor, resumer::recovery::Recovery},
    Extractor,
};
use dt_common::{
    config::config_enums::DbType,
    log_error, log_info, log_warn,
    meta::{
        col_value::ColValue,
        dt_data::DtData,
        mongo::{mongo_cdc_source::MongoCdcSource, mongo_constant::MongoConstants},
        position::Position,
        row_data::RowData,
        row_type::RowType,
        syncer::Syncer,
    },
    rdb_filter::RdbFilter,
    utils::time_util::TimeUtil,
};
use mongodb::{
    bson::{doc, Bson, Document, Timestamp},
    change_stream::event::{OperationType, ResumeToken},
    options::{ChangeStreamOptions, FullDocumentBeforeChangeType, FullDocumentType, UpdateOptions},
    Client,
};

const SYSTEM_DBS: [&str; 3] = ["admin", "config", "local"];

pub struct MongoCdcExtractor {
    pub base_extractor: BaseExtractor,
    pub filter: RdbFilter,
    pub resume_token: String,
    pub start_timestamp: u32,
    pub source: MongoCdcSource,
    pub mongo_client: Client,
    pub app_name: String,
    pub heartbeat_interval_secs: u64,
    pub heartbeat_tb: String,
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
                            .push_dt_data(DtData::Heartbeat {}, position)
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
        self.base_extractor.wait_task_finish().await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.mongo_client.clone().shutdown().await;
        Ok(())
    }
}

impl MongoCdcExtractor {
    async fn extract_oplog(&mut self) -> anyhow::Result<()> {
        let start_timestamp = self.parse_start_timestamp();
        let filter = doc! {
            "ts": { "$gte": start_timestamp }
        };
        let options = mongodb::options::FindOptions::builder()
            .cursor_type(mongodb::options::CursorType::TailableAwait)
            .build();

        let oplog = self
            .mongo_client
            .database("local")
            .collection::<Document>("oplog.rs");
        let mut cursor = oplog.find(filter, options).await?;

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

            let ts_value = ts.unwrap().as_timestamp().unwrap();
            BaseExtractor::update_time_filter(
                &mut self.base_extractor.time_filter,
                ts_value.time,
                &Position::MongoCdc {
                    resume_token: String::new(),
                    operation_time: ts_value.time,
                    timestamp: Position::format_timestamp_millis(ts_value.time as i64 * 1000),
                },
            );

            if !self.base_extractor.time_filter.started {
                continue;
            }

            match op.as_str() {
                "i" => {
                    after.insert(
                        MongoConstants::DOC.to_string(),
                        ColValue::MongoDoc(o.unwrap().as_document().unwrap().clone()),
                    );
                }
                "u" => {
                    row_type = RowType::Update;
                    // for update op log, doc.o contains only diff instead of full doc
                    let after_doc = o.unwrap().as_document().unwrap();
                    // refer: https://www.mongodb.com/community/forums/t/oplog-update-entry-without-set-and-unset/171771
                    // https://www.mongodb.com/docs/manual/reference/operator/update/#update-operators-1
                    // in MongoDB 4.4 and earlier, after_doc contains $set with all new document fields,
                    // after that, after_doc contains diff with only changed fields.
                    let diff_doc = if let Some(doc) = after_doc.get("diff") {
                        let doc = doc.as_document().unwrap();
                        if let Some(i_doc) = doc.get("i") {
                            doc! {MongoConstants::SET: i_doc.as_document().unwrap()}
                        } else if let Some(u_doc) = doc.get("u") {
                            doc! {MongoConstants::SET: u_doc.as_document().unwrap()}
                        } else if let Some(d_doc) = doc.get("d") {
                            doc! {MongoConstants::UNSET: d_doc.as_document().unwrap()}
                        } else {
                            doc! {}
                        }
                    } else if let Some(set_doc) = after_doc.get(MongoConstants::SET) {
                        doc! {MongoConstants::SET: set_doc.as_document().unwrap()}
                    } else if let Some(unset_doc) = after_doc.get(MongoConstants::UNSET) {
                        doc! {MongoConstants::UNSET: unset_doc.as_document().unwrap()}
                    } else {
                        doc! {}
                    };

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
                    before.insert(
                        MongoConstants::DOC.to_string(),
                        ColValue::MongoDoc(o.unwrap().as_document().unwrap().clone()),
                    );
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
            before.insert(
                MongoConstants::DOC.to_string(),
                ColValue::MongoDoc(o.unwrap().as_document().unwrap().clone()),
            );

            data.push(Self::build_oplog_row_data(
                &ns,
                &ts,
                RowType::Delete,
                before,
                HashMap::new(),
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

        // get db & tb
        let tokens: Vec<&str> = ns.split('.').collect();
        let db: String = tokens[0].into();
        let tb: String = ns[db.len() + 1..].into();
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
        let row_data = RowData::new(db, tb, row_type, before, after);
        (row_data, position)
    }

    async fn extract_change_stream(&mut self) -> anyhow::Result<()> {
        let (resume_token, start_timestamp) = if self.resume_token.is_empty() {
            (None, Some(self.parse_start_timestamp()))
        } else {
            let token: ResumeToken = serde_json::from_str(&self.resume_token)?;
            (Some(token), None)
        };

        // refer: https://www.mongodb.com/docs/manual/changeStreams/
        // Starting in MongoDB 6.0, you can use change stream events to output the version of
        // a document before and after changes (the document pre- and post-images)
        let stream_options = ChangeStreamOptions::builder()
            .start_at_operation_time(start_timestamp)
            .start_after(resume_token)
            .full_document(Some(FullDocumentType::UpdateLookup))
            .full_document_before_change(Some(FullDocumentBeforeChangeType::WhenAvailable))
            .build();

        let mut change_stream = self.mongo_client.watch(None, stream_options).await?;
        loop {
            let result = change_stream.next_if_any().await?;
            if let Some(doc) = result {
                let resume_token = doc.id;
                let position = if let Some(operation_time) = doc.cluster_time {
                    let pos = Position::MongoCdc {
                        resume_token: json!(resume_token).to_string(),
                        operation_time: operation_time.time,
                        timestamp: Position::format_timestamp_millis(
                            operation_time.time as i64 * 1000,
                        ),
                    };
                    BaseExtractor::update_time_filter(
                        &mut self.base_extractor.time_filter,
                        operation_time.time,
                        &pos,
                    );
                    pos
                } else {
                    Position::MongoCdc {
                        resume_token: json!(resume_token).to_string(),
                        operation_time: 0,
                        timestamp: String::new(),
                    }
                };

                if !self.base_extractor.time_filter.started {
                    continue;
                }

                let (mut db, mut tb) = (String::new(), String::new());
                if let Some(ns) = doc.ns {
                    db = ns.db.clone();
                    if let Some(coll) = ns.coll {
                        tb = coll.clone();
                    }
                }

                let mut row_type = RowType::Insert;
                let mut before = HashMap::new();
                let mut after = HashMap::new();

                match doc.operation_type {
                    OperationType::Insert => {
                        after.insert(
                            MongoConstants::DOC.to_string(),
                            ColValue::MongoDoc(doc.full_document.unwrap()),
                        );
                    }

                    OperationType::Delete => {
                        row_type = RowType::Delete;
                        before.insert(
                            MongoConstants::DOC.to_string(),
                            ColValue::MongoDoc(doc.document_key.unwrap()),
                        );
                    }

                    OperationType::Update | OperationType::Replace => {
                        row_type = RowType::Update;
                        if let Some(document) = doc.full_document {
                            before.insert(
                                MongoConstants::DOC.to_string(),
                                ColValue::MongoDoc(doc.document_key.unwrap()),
                            );
                            after.insert(
                                MongoConstants::DOC.to_string(),
                                ColValue::MongoDoc(document),
                            );
                        }
                    }

                    // TODO, heartbeat and DDL
                    _ => {
                        continue;
                    }
                }

                let row_data = RowData::new(db, tb, row_type, Some(before), Some(after));
                self.push_row_to_buf(row_data, position).await?;
            }
        }
    }

    async fn push_row_to_buf(
        &mut self,
        row_data: RowData,
        position: Position,
    ) -> anyhow::Result<()> {
        if SYSTEM_DBS.contains(&row_data.schema.as_str()) {
            return Ok(());
        }
        if self
            .filter
            .filter_event(&row_data.schema, &row_data.tb, &row_data.row_type)
        {
            self.base_extractor.record_extracted_metrics_row(&row_data);
            return self
                .base_extractor
                .push_dt_data(DtData::Heartbeat {}, position)
                .await;
        }
        self.base_extractor.push_row(row_data, position).await
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
        let options = UpdateOptions::builder().upsert(true).build();
        if let Err(err) = collection
            .update_one(query_doc, update_doc, Some(options))
            .await
        {
            log_error!("heartbeat failed: {:?}", err);
        }
        Ok(())
    }
}
