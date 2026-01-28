use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use mongodb::{
    bson::{doc, oid::ObjectId, Bson, Document},
    Client,
};
use serde_json::Value as JsonValue;

use crate::checker::base_checker::{
    CheckContext, Checker, CheckerTbMeta, DataCheckerHandle, FetchResult, CHECKER_MAX_QUERY_BATCH,
};
use dt_common::log_error;
use dt_common::meta::{
    col_value::ColValue,
    mongo::{mongo_constant::MongoConstants, mongo_key::MongoKey},
    rdb_tb_meta::RdbTbMeta,
    row_data::RowData,
    row_type::RowType,
};

pub struct MongoChecker {
    mongo_client: Client,
}

#[async_trait]
impl Checker for MongoChecker {
    async fn fetch(&mut self, src_rows: &[Arc<RowData>]) -> anyhow::Result<FetchResult> {
        let first_row = src_rows
            .first()
            .context("fetch called with empty src rows")?;

        let mut meta = Self::mock_tb_meta(&first_row.schema, &first_row.tb);
        if let Some(after) = &first_row.after {
            meta.cols = after.keys().cloned().collect();
        }
        for col in [MongoConstants::DOC, MongoConstants::ID] {
            let col = col.to_string();
            if !meta.cols.contains(&col) {
                meta.cols.push(col);
            }
        }
        let tb_meta = Arc::new(CheckerTbMeta::Mongo(meta));
        let basic_meta = tb_meta.basic();

        let mut ids = Vec::with_capacity(src_rows.len());
        let mut batch_rows = Vec::with_capacity(src_rows.len());
        for row_data in src_rows {
            let id = Self::get_id_from_row(row_data).with_context(|| {
                format!(
                    "row_data missing `_id`, schema: {}, tb: {}",
                    row_data.schema, row_data.tb
                )
            })?;

            let doc = doc! { MongoConstants::ID: id.clone() };
            if MongoKey::from_doc(&doc).is_some() {
                ids.push(id);
                batch_rows.push(row_data.clone());
            }
        }

        if ids.is_empty() {
            return Ok(FetchResult {
                tb_meta,
                src_rows: Vec::new(),
                dst_rows: Vec::new(),
            });
        }

        let mut dst_row_data_vec = Vec::new();
        let collection = self
            .mongo_client
            .database(&basic_meta.schema)
            .collection::<Document>(&basic_meta.tb);
        for chunk in ids.chunks(CHECKER_MAX_QUERY_BATCH) {
            let filter = doc! {
                MongoConstants::ID: {
                    "$in": chunk.to_vec()
                }
            };
            let mut cursor = collection.find(filter, None).await?;

            while cursor.advance().await? {
                let doc = cursor.deserialize_current()?;
                if let Some(key) = MongoKey::from_doc(&doc) {
                    let row_data =
                        Self::build_row_data(&basic_meta.schema, &basic_meta.tb, doc, &key);
                    dst_row_data_vec.push(row_data);
                } else {
                    let id = doc.get(MongoConstants::ID);
                    log_error!("dst row_data's _id type not supported, _id: {:?}", id);
                }
            }
        }

        Ok(FetchResult {
            tb_meta,
            src_rows: batch_rows,
            dst_rows: dst_row_data_vec,
        })
    }
}

impl MongoChecker {
    pub fn spawn(mongo_client: Client, ctx: CheckContext, buffer_size: usize) -> DataCheckerHandle {
        DataCheckerHandle::spawn(Self { mongo_client }, ctx, buffer_size, "MongoChecker")
    }

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
            RowType::Insert,
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
