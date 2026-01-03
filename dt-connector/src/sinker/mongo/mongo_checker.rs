use std::collections::HashMap;

use anyhow::Context;
use async_trait::async_trait;

use mongodb::{
    bson::{doc, oid::ObjectId, Bson, Document},
    Client,
};
use serde_json::Value as JsonValue;
use tokio::time::Instant;

use crate::{
    sinker::{
        base_checker::{BaseChecker, CheckerBackend, CheckerCommon},
        base_sinker::BaseSinker,
    },
    Sinker,
};
use dt_common::{
    log_error,
    meta::{
        col_value::ColValue,
        mongo::{mongo_constant::MongoConstants, mongo_key::MongoKey},
        rdb_tb_meta::RdbTbMeta,
        row_data::RowData,
        struct_meta::{statement::struct_statement::StructStatement, struct_data::StructData},
    },
    utils::limit_queue::LimitedQueue,
};

#[derive(Clone)]
pub struct MongoChecker {
    pub mongo_client: Client,
    pub common: CheckerCommon,
}

#[async_trait]
impl Sinker for MongoChecker {
    async fn sink_dml(&mut self, data: Vec<RowData>, batch: bool) -> anyhow::Result<()> {
        let mut backend = self.clone();
        BaseChecker::standard_sink_dml(&mut backend, &mut self.common, data, batch).await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        BaseChecker::standard_close(&mut self.common).await
    }

    async fn sink_struct(&mut self, _data: Vec<StructData>) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl CheckerBackend for MongoChecker {
    async fn get_tb_meta_by_row(
        &mut self,
        row: &RowData,
    ) -> anyhow::Result<crate::sinker::base_checker::CheckerTbMeta> {
        let mut meta = Self::mock_tb_meta(&row.schema, &row.tb);
        if let Some(after) = &row.after {
            meta.cols = after.keys().cloned().collect();
        }
        // Ensure standard Mongo columns are always checked
        let doc_col = MongoConstants::DOC.to_string();
        if !meta.cols.contains(&doc_col) {
            meta.cols.push(doc_col);
        }
        let id_col = MongoConstants::ID.to_string();
        if !meta.cols.contains(&id_col) {
            meta.cols.push(id_col);
        }
        Ok(crate::sinker::base_checker::CheckerTbMeta::Mongo(meta))
    }

    async fn fetch_single(
        &self,
        _tb_meta: &crate::sinker::base_checker::CheckerTbMeta,
        row: &RowData,
    ) -> anyhow::Result<Option<RowData>> {
        let id = Self::get_id_from_row(row).context("missing _id")?;
        let filter = doc! { MongoConstants::ID: id };
        let collection = self
            .mongo_client
            .database(&row.schema)
            .collection::<Document>(&row.tb);

        if let Some(doc) = collection.find_one(filter, None).await? {
            if let Some(key) = MongoKey::from_doc(&doc) {
                let row_data = Self::build_row_data(&row.schema, &row.tb, doc, &key);
                Ok(Some(row_data))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    async fn fetch_batch(
        &self,
        _tb_meta: &crate::sinker::base_checker::CheckerTbMeta,
        schema: &str,
        tb: &str,
        data: &[RowData],
    ) -> anyhow::Result<HashMap<u128, RowData>> {
        let mut ids = Vec::with_capacity(data.len());

        for row_data in data {
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

        let mut dst_row_data_map = HashMap::new();
        let start_time = Instant::now();
        let mut rts = LimitedQueue::new(1);
        let collection = self
            .mongo_client
            .database(schema)
            .collection::<Document>(tb);
        let mut cursor = collection.find(filter, None).await?;
        rts.push((start_time.elapsed().as_millis() as u64, 1));
        BaseSinker::update_monitor_rt(&self.common.monitor, &rts).await?;

        // We need a meta to calc hash code.
        let tb_meta = Self::mock_tb_meta(schema, tb);

        while cursor.advance().await? {
            let doc = cursor.deserialize_current()?;
            if let Some(key) = MongoKey::from_doc(&doc) {
                let row_data = Self::build_row_data(schema, tb, doc, &key);
                let hash_code = row_data.get_hash_code(&tb_meta)?;
                dst_row_data_map.insert(hash_code, row_data);
            } else {
                let id = doc.get(MongoConstants::ID);
                log_error!("dst row_data's _id type not supported, _id: {:?}", id);
            }
        }
        Ok(dst_row_data_map)
    }

    async fn fetch_dst_struct(&self, _src: &StructStatement) -> anyhow::Result<StructStatement> {
        Ok(StructStatement::Unknown)
    }
}

impl MongoChecker {
    fn mock_tb_meta(schema: &str, tb: &str) -> RdbTbMeta {
        let mut meta = RdbTbMeta::default();
        meta.schema = schema.to_string();
        meta.tb = tb.to_string();
        meta.id_cols = vec![MongoConstants::ID.to_string()];
        meta
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
            if let Some(val) = after.get(MongoConstants::ID) {
                match val {
                    ColValue::String(s) => {
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
                    _ => {
                        // Fallback for other types if necessary
                    }
                }
            }
        }
        anyhow::bail!("missing _id in row data")
    }
}
