use std::collections::HashMap;

use anyhow::Context;
use async_trait::async_trait;

use mongodb::{
    bson::{doc, oid::ObjectId, Bson, Document},
    Client,
};
use serde_json::Value as JsonValue;
use tokio::time::Instant;

use crate::sinker::{
    base_checker::{Checker, CheckerCommon, CheckerTbMeta},
    base_sinker::BaseSinker,
};
use dt_common::{
    log_error,
    meta::{
        col_value::ColValue,
        mongo::{mongo_constant::MongoConstants, mongo_key::MongoKey},
        rdb_tb_meta::RdbTbMeta,
        row_data::RowData,
    },
    utils::limit_queue::LimitedQueue,
};

#[derive(Clone)]
pub struct MongoChecker {
    pub mongo_client: Client,
    pub common: CheckerCommon,
}

#[async_trait]
impl Checker for MongoChecker {
    fn common_mut(&mut self) -> &mut CheckerCommon {
        &mut self.common
    }

    async fn get_tb_meta_by_row(&mut self, row: &RowData) -> anyhow::Result<CheckerTbMeta> {
        let mut meta = Self::mock_tb_meta(&row.schema, &row.tb);
        if let Some(after) = &row.after {
            meta.cols = after.keys().cloned().collect();
        }
        // Ensure standard Mongo columns are always checked
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
        data: &[&RowData],
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
        let start_time = Instant::now();
        let mut rts = LimitedQueue::new(1);
        let collection = self
            .mongo_client
            .database(&basic_meta.schema)
            .collection::<Document>(&basic_meta.tb);
        let mut cursor = collection.find(filter, None).await?;
        rts.push((start_time.elapsed().as_millis() as u64, 1));
        BaseSinker::update_monitor_rt(&self.common.monitor, &rts).await?;

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

    fn get_id_from_row(row: &RowData) -> anyhow::Result<Bson> {
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
                // Try to parse as JSON for wrapped types (e.g. from CheckLogExtractor)
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
