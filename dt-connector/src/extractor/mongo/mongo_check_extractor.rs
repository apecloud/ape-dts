use std::collections::HashMap;

use async_trait::async_trait;

use dt_common::log_info;
use dt_common::meta::{
    col_value::ColValue,
    mongo::{mongo_constant::MongoConstants, mongo_key::MongoKey},
    position::Position,
    row_data::RowData,
    row_type::RowType,
};

use mongodb::{
    bson::{doc, Document},
    Client,
};

use crate::{
    check_log::{check_log::CheckLog, log_type::LogType},
    extractor::{base_check_extractor::BaseCheckExtractor, base_extractor::BaseExtractor},
    BatchCheckExtractor, Extractor,
};

pub struct MongoCheckExtractor {
    pub base_extractor: BaseExtractor,
    pub mongo_client: Client,
    pub check_log_dir: String,
    pub batch_size: usize,
}

#[async_trait]
impl Extractor for MongoCheckExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        log_info!("MongoCheckExtractor starts");
        let base_check_extractor = BaseCheckExtractor {
            check_log_dir: self.check_log_dir.clone(),
            batch_size: self.batch_size,
        };
        base_check_extractor.extract(self).await.unwrap();
        self.base_extractor.wait_task_finish().await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.mongo_client.clone().shutdown().await;
        Ok(())
    }
}

#[async_trait]
impl BatchCheckExtractor for MongoCheckExtractor {
    async fn batch_extract(&mut self, check_logs: &[CheckLog]) -> anyhow::Result<()> {
        let log_type = &check_logs[0].log_type;
        let schema = &check_logs[0].schema;
        let tb = &check_logs[0].tb;
        let collection = self
            .mongo_client
            .database(schema)
            .collection::<Document>(tb);

        let mut ids = Vec::new();
        for check_log in check_logs.iter() {
            // check log has only one col: _id
            if let Some(Some(col_value)) = check_log.id_col_values.get(MongoConstants::ID) {
                let keys = MongoKey::parse_fuzzy(col_value);
                for key in keys {
                    ids.push(key.to_mongo_id());
                }
            }
        }

        let filter = doc! {
            MongoConstants::ID: {
                "$in": ids
            }
        };

        let mut cursor = collection.find(filter, None).await.unwrap();
        while cursor.advance().await.unwrap() {
            let doc = cursor.deserialize_current().unwrap();
            let mut after = HashMap::new();
            let id: String = MongoKey::from_doc(&doc).unwrap().to_plain_string();
            after.insert(MongoConstants::ID.to_string(), ColValue::String(id));
            after.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(doc));
            let mut row_data = RowData::new(
                schema.clone(),
                tb.clone(),
                RowType::Insert,
                None,
                Some(after),
            );

            if log_type == &LogType::Diff {
                row_data.row_type = RowType::Update;
                row_data.before = row_data.after.clone();
            }

            self.base_extractor
                .push_row(row_data, Position::None)
                .await
                .unwrap();
        }
        Ok(())
    }
}
