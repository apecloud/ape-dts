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
        order_key::OrderKey,
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
    pub sample_rate: Option<u8>,
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
            if let Some(Position::RdbSnapshot {
                order_key: Some(OrderKey::Single((_, Some(value)))),
                ..
            }) = handler
                .get_snapshot_resume_position(&self.db, &self.tb, false)
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
        let mut extracted_count = 0u64;
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
            extracted_count += 1;
            if !Self::should_sample_row(self.sample_rate, extracted_count) {
                continue;
            }

            let object_id = Self::get_object_id(&doc);

            let after = Self::build_after_cols(&doc);
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
                order_key: Some(OrderKey::Single((
                    MongoConstants::ID.into(),
                    Some(object_id),
                ))),
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

    fn build_after_cols(doc: &Document) -> HashMap<String, ColValue> {
        let mut after = HashMap::new();
        let id = MongoKey::from_doc(doc)
            .map(|key| ColValue::String(key.to_string()))
            .unwrap_or(ColValue::None);
        after.insert(MongoConstants::ID.to_string(), id);
        after.insert(
            MongoConstants::DOC.to_string(),
            ColValue::MongoDoc(doc.clone()),
        );
        after
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

    fn should_sample_row(sample_rate: Option<u8>, extracted_count: u64) -> bool {
        let Some(sample_rate) = sample_rate.filter(|rate| *rate < 100) else {
            return true;
        };
        if sample_rate == 0 || extracted_count == 0 {
            return false;
        }

        let sample_rate = u64::from(sample_rate);
        let interval = (100 + sample_rate - 1) / sample_rate;
        extracted_count % interval == 0
    }
}

#[cfg(test)]
mod tests {
    use super::MongoSnapshotExtractor;

    #[test]
    fn standalone_snapshot_sample_rate_uses_simple_interval() {
        let sampled = (1..=10)
            .filter(|count| MongoSnapshotExtractor::should_sample_row(Some(34), *count))
            .collect::<Vec<_>>();

        assert_eq!(sampled, vec![3, 6, 9]);
    }

    #[test]
    fn standalone_snapshot_sample_rate_100_keeps_all_rows() {
        let sampled = (1..=3)
            .filter(|count| MongoSnapshotExtractor::should_sample_row(Some(100), *count))
            .collect::<Vec<_>>();

        assert_eq!(sampled, vec![1, 2, 3]);
    }
}
