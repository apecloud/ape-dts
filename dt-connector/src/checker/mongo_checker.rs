use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use mongodb::{
    bson::{doc, oid::ObjectId, Bson, Document},
    Client,
};
use serde_json::Value as JsonValue;
use tokio::sync::{mpsc, Mutex};

use dt_common::meta::{
    col_value::ColValue,
    mongo::{mongo_constant::MongoConstants, mongo_key::MongoKey},
    rdb_tb_meta::RdbTbMeta,
    row_data::RowData,
};
use dt_common::{log_error, log_info, log_warn};

use crate::checker::base_checker::{CheckProcessor, Checker, CheckerCommon, CheckerTbMeta};
use crate::checker::{CheckerHandle, CheckerMode};

struct MongoChecker {
    mongo_client: Client,
}

#[async_trait]
impl Checker for MongoChecker {
    async fn get_tb_meta(&mut self, row: &Arc<RowData>) -> anyhow::Result<CheckerTbMeta> {
        let mut meta = Self::mock_tb_meta(&row.schema, &row.tb);
        if let Some(after) = &row.after {
            meta.cols = after.keys().cloned().collect();
        }
        for col in [MongoConstants::DOC, MongoConstants::ID] {
            let col = col.to_string();
            if !meta.cols.contains(&col) {
                meta.cols.push(col);
            }
        }
        Ok(CheckerTbMeta::Mongo(meta))
    }

    async fn fetch_batch(
        &self,
        tb_meta: &CheckerTbMeta,
        data: &[&Arc<RowData>],
    ) -> anyhow::Result<Vec<RowData>> {
        let basic_meta = tb_meta.basic();

        let mut ids = Vec::with_capacity(data.len());

        for &row_data in data {
            let id = Self::get_id_from_row(row_data).with_context(|| {
                format!(
                    "row_data missing `_id`, schema: {}, tb: {}",
                    row_data.schema, row_data.tb
                )
            })?;

            let doc = doc! { MongoConstants::ID: id.clone() };
            if MongoKey::from_doc(&doc).is_some() {
                ids.push(id);
            }
        }

        let filter = doc! {
            MongoConstants::ID: {
                "$in": ids
            }
        };

        let mut dst_row_data_vec = Vec::new();
        let collection = self
            .mongo_client
            .database(&basic_meta.schema)
            .collection::<Document>(&basic_meta.tb);
        let mut cursor = collection.find(filter, None).await?;

        while cursor.advance().await? {
            let doc = cursor.deserialize_current()?;
            if let Some(key) = MongoKey::from_doc(&doc) {
                let row_data = Self::build_row_data(&basic_meta.schema, &basic_meta.tb, doc, &key);
                dst_row_data_vec.push(row_data);
            } else {
                let id = doc.get(MongoConstants::ID);
                log_error!("dst row_data's _id type not supported, _id: {:?}", id);
            }
        }
        Ok(dst_row_data_vec)
    }
}

impl MongoChecker {
    fn mock_tb_meta(schema: &str, tb: &str) -> RdbTbMeta {
        RdbTbMeta {
            schema: schema.to_string(),
            tb: tb.to_string(),
            id_cols: vec![MongoConstants::ID.to_string()],
            ..Default::default()
        }
    }

    fn build_row_data(schema: &str, tb: &str, doc: Document, key: &MongoKey) -> RowData {
        let mut dst_after = HashMap::new();
        dst_after.insert(
            MongoConstants::ID.to_string(),
            ColValue::String(key.to_string()),
        );
        dst_after.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(doc));
        RowData::new(
            schema.to_string(),
            tb.to_string(),
            dt_common::meta::row_type::RowType::Insert,
            None,
            Some(dst_after),
        )
    }

    fn get_id_from_row(row: &Arc<RowData>) -> anyhow::Result<Bson> {
        if let Some(after) = &row.after {
            if let Some(ColValue::MongoDoc(doc)) = after.get(MongoConstants::DOC) {
                if let Some(id) = doc.get(MongoConstants::ID) {
                    return Ok(id.clone());
                }
            }
            if let Some(ColValue::String(s)) = after.get(MongoConstants::ID) {
                if let Ok(oid) = ObjectId::parse_str(s) {
                    return Ok(Bson::ObjectId(oid));
                }
                if let Ok(json) = serde_json::from_str::<JsonValue>(s) {
                    if let Some(val) = json.get("String").and_then(|v| v.as_str()) {
                        if let Ok(oid) = ObjectId::parse_str(val) {
                            return Ok(Bson::ObjectId(oid));
                        }
                        return Ok(Bson::String(val.to_string()));
                    }
                    if let Some(oid) = json
                        .get("ObjectId")
                        .and_then(|v| v.get("$oid"))
                        .and_then(|v| v.as_str())
                    {
                        if let Ok(oid) = ObjectId::parse_str(oid) {
                            return Ok(Bson::ObjectId(oid));
                        }
                    }
                }
                return Ok(Bson::String(s.clone()));
            }
        }
        anyhow::bail!("missing _id in row data")
    }
}

pub struct MongoCheckerHandle {
    processor: Option<Mutex<CheckProcessor<MongoChecker>>>,
    sender: Option<mpsc::Sender<Vec<Arc<RowData>>>>,
    mode: CheckerMode,
    dropped_batches: AtomicU64,
}

impl MongoCheckerHandle {
    pub fn new(mongo_client: Client, common: CheckerCommon, mode: CheckerMode) -> Self {
        let backend = MongoChecker { mongo_client };
        let processor = CheckProcessor::new(backend, common);
        let (sender, processor) = match mode {
            CheckerMode::Sync => (None, Some(Mutex::new(processor))),
            CheckerMode::AsyncBlocking { buffer_size } | CheckerMode::AsyncDrop { buffer_size } => {
                let (tx, mut rx) = mpsc::channel(buffer_size.max(1));
                tokio::spawn(async move {
                    log_info!("Checker [MongoChecker] background worker started.");
                    let mut processor = processor;
                    while let Some(batch) = rx.recv().await {
                        if let Err(err) = processor.sink_dml(batch, true).await {
                            log_error!("Checker [MongoChecker] batch failed: {}", err);
                        }
                    }
                    if let Err(err) = processor.close().await {
                        log_error!("Checker [MongoChecker] close failed: {}", err);
                    }
                    log_info!("Checker [MongoChecker] background worker stopped.");
                });
                (Some(tx), None)
            }
        };

        Self {
            processor,
            sender,
            mode,
            dropped_batches: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl CheckerHandle for MongoCheckerHandle {
    async fn check(&self, data: Vec<Arc<RowData>>) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        match self.mode {
            CheckerMode::Sync => {
                let processor = self
                    .processor
                    .as_ref()
                    .ok_or_else(|| anyhow!("Checker processor missing"))?;
                let mut guard = processor.lock().await;
                guard.sink_dml(data, true).await
            }
            CheckerMode::AsyncBlocking { .. } => {
                if let Some(tx) = &self.sender {
                    tx.send(data)
                        .await
                        .map_err(|_| anyhow!("Checker worker closed"))?;
                }
                Ok(())
            }
            CheckerMode::AsyncDrop { .. } => {
                if let Some(tx) = &self.sender {
                    match tx.try_send(data) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            let dropped = self.dropped_batches.fetch_add(1, Ordering::Relaxed) + 1;
                            if dropped % 1000 == 0 {
                                log_warn!("Checker queue full, dropped {} batches.", dropped);
                            }
                        }
                        Err(_) => return Err(anyhow!("Checker worker closed")),
                    }
                }
                Ok(())
            }
        }
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.sender.take();
        if matches!(self.mode, CheckerMode::Sync) {
            if let Some(processor) = &self.processor {
                let mut guard = processor.lock().await;
                guard.close().await?;
            }
        }
        Ok(())
    }
}
