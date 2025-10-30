use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use mongodb::{
    bson::{doc, oid::ObjectId, Bson, Document},
    options::FindOptions,
    Client,
};

use crate::{
    extractor::{base_extractor::BaseExtractor, resumer::recovery::Recovery},
    Extractor,
};
use dt_common::{
    config::config_enums::DbType,
    log_error, log_info,
    meta::{
        col_value::ColValue,
        mongo::{mongo_constant::MongoConstants, mongo_key::MongoKey},
        position::Position,
        row_data::RowData,
        row_type::RowType,
    },
};

pub struct MongoSnapshotExtractor {
    pub base_extractor: BaseExtractor,
    pub db: String,
    pub tb: String,
    pub mongo_client: Client,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
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
        Ok(())
    }
}

impl MongoSnapshotExtractor {
    pub async fn extract_internal(&mut self) -> anyhow::Result<()> {
        log_info!("start extracting data from {}.{}", self.db, self.tb);

        let filter = if let Some(handler) = &self.recovery {
            if let Some(value) = handler
                .get_snapshot_resume_position(&self.db, &self.tb, MongoConstants::ID, false)
                .await
            {
                let value = ObjectId::parse_str(&value)?;
                log_info!(
                    "[{}.{}] recovery from [{}]:[{}]",
                    self.db,
                    self.tb,
                    MongoConstants::ID,
                    value
                );
                Some(doc! {MongoConstants::ID: {"$gt": value}})
            } else {
                None
            }
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
            let doc = cursor.deserialize_current().map_err(|e| {
                log_error!(
                    "error deserializing {}.{} document: {}",
                    self.db,
                    self.tb,
                    e
                );
                e
            })?;
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
