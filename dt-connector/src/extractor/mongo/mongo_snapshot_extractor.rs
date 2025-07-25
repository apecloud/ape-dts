use async_trait::async_trait;
use dt_common::meta::{
    col_value::ColValue,
    mongo::{mongo_constant::MongoConstants, mongo_key::MongoKey},
    position::Position,
    row_data::RowData,
    row_type::RowType,
};
use dt_common::{config::config_enums::DbType, log_info};
use mongodb::{
    bson::{doc, oid::ObjectId, Bson, Document},
    options::FindOptions,
    Client,
};
use std::collections::HashMap;

use crate::{
    extractor::{base_extractor::BaseExtractor, resumer::snapshot_resumer::SnapshotResumer},
    Extractor,
};

pub struct MongoSnapshotExtractor {
    pub base_extractor: BaseExtractor,
    pub resumer: SnapshotResumer,
    pub db: String,
    pub tb: String,
    pub mongo_client: Client,
}

#[async_trait]
impl Extractor for MongoSnapshotExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        log_info!(
            "MongoSnapshotExtractor starts, schema: {}, tb: {}",
            self.db,
            self.tb
        );
        self.extract_internal().await?;
        self.base_extractor.wait_task_finish().await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.mongo_client.clone().shutdown().await;
        Ok(())
    }
}

impl MongoSnapshotExtractor {
    pub async fn extract_internal(&mut self) -> anyhow::Result<()> {
        log_info!("start extracting data from {}.{}", self.db, self.tb);

        let filter = if let Some(resume_value) =
            self.resumer
                .get_resume_value(&self.db, &self.tb, MongoConstants::ID, false)
        {
            let start_id = ObjectId::parse_str(resume_value)?;
            log_info!("start_id: {}", start_id.to_string());
            Some(doc! {MongoConstants::ID: {"$gt": start_id}})
        } else {
            None
        };

        // order by asc
        let find_options = FindOptions::builder()
            .sort(doc! {MongoConstants::ID: 1})
            .build();

        let collection = self
            .mongo_client
            .database(&self.db)
            .collection::<Document>(&self.tb);
        let mut cursor = collection.find(filter, find_options).await?;
        while cursor.advance().await? {
            let doc = cursor.deserialize_current()?;
            let object_id = Self::get_object_id(&doc);

            let mut after = HashMap::new();
            let id: String = if let Some(key) = MongoKey::from_doc(&doc) {
                key.to_string()
            } else {
                String::new()
            };
            after.insert(MongoConstants::ID.to_string(), ColValue::String(id));
            after.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(doc));
            let row_data = RowData::new(
                self.db.clone(),
                self.tb.clone(),
                RowType::Insert,
                None,
                Some(after),
            );
            let position = Position::RdbSnapshot {
                db_type: DbType::Mongo.to_string(),
                schema: self.db.clone(),
                tb: self.tb.clone(),
                order_col: MongoConstants::ID.into(),
                value: object_id,
            };

            self.base_extractor.push_row(row_data, position).await?;
        }

        log_info!(
            "end extracting data from {}.{}, all count: {}",
            self.db,
            self.tb,
            self.base_extractor.monitor.counters.pushed_record_count
        );
        Ok(())
    }

    fn get_object_id(doc: &Document) -> String {
        if let Some(id) = doc.get(MongoConstants::ID) {
            match id {
                Bson::ObjectId(v) => return v.to_string(),
                _ => return String::new(),
            }
        }
        String::new()
    }
}
