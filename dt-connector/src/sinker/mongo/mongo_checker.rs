use std::{collections::HashMap, sync::Arc, time::Instant};

use async_rwlock::RwLock;
use async_trait::async_trait;
use mongodb::{
    bson::{doc, Document},
    Client,
};

use dt_common::{error::Error, log_error, monitor::monitor::Monitor};

use dt_meta::{
    col_value::ColValue,
    mongo::{mongo_constant::MongoConstants, mongo_key::MongoKey},
    rdb_tb_meta::RdbTbMeta,
    row_data::RowData,
    row_type::RowType,
};

use crate::{
    call_batch_fn,
    rdb_router::RdbRouter,
    sinker::{base_checker::BaseChecker, base_sinker::BaseSinker},
    Sinker,
};

#[derive(Clone)]
pub struct MongoChecker {
    pub router: RdbRouter,
    pub batch_size: usize,
    pub mongo_client: Client,
    pub monitor: Arc<RwLock<Monitor>>,
}

#[async_trait]
impl Sinker for MongoChecker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, _batch: bool) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        call_batch_fn!(self, data, Self::batch_check);
        Ok(())
    }

    fn get_monitor(&self) -> Option<Arc<RwLock<Monitor>>> {
        Some(self.monitor.clone())
    }
}

impl MongoChecker {
    async fn batch_check(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> Result<(), Error> {
        let start_time = Instant::now();

        let schema = &data[0].schema;
        let tb = &data[0].tb;
        let tb_meta = Self::mock_tb_meta(schema, tb);
        let collection = self
            .mongo_client
            .database(schema)
            .collection::<Document>(tb);

        let mut ids = Vec::new();
        let mut src_row_data_map = HashMap::new();

        for row_data in data.iter().skip(start_index).take(batch_size) {
            let after = row_data.after.as_ref().unwrap();
            if let Some(ColValue::MongoDoc(doc)) = after.get(MongoConstants::DOC) {
                let id = doc.get(MongoConstants::ID).unwrap();
                if let Some(key) = MongoKey::from_doc(doc) {
                    src_row_data_map.insert(key, row_data.clone());
                    ids.push(id);
                } else {
                    // this should have a very small chance to happen, and we don't support
                    log_error!("row_data's _id type not supported, _id: {:?}", id);
                }
            }
        }

        let filter = doc! {
            MongoConstants::ID: {
                "$in": ids
            }
        };

        // batch fetch dst
        let mut dst_row_data_map = HashMap::new();
        let mut cursor = collection.find(filter, None).await.unwrap();
        while cursor.advance().await.unwrap() {
            let doc = cursor.deserialize_current().unwrap();
            // key should not be none since we have filtered in ids,
            let key = MongoKey::from_doc(&doc).unwrap();
            let row_data = Self::build_row_data(schema, tb, doc, &key);
            dst_row_data_map.insert(key, row_data);
        }

        // batch check
        let mut miss = Vec::new();
        let mut diff = Vec::new();
        for (key, src_row_data) in src_row_data_map {
            if let Some(dst_row_data) = dst_row_data_map.remove(&key) {
                if src_row_data != dst_row_data {
                    diff.push(src_row_data);
                }
            } else {
                miss.push(src_row_data);
            };
        }

        BaseChecker::log_mongo_dml(&tb_meta, &self.router, miss, diff)
            .await
            .unwrap();

        BaseSinker::update_batch_monitor(&mut self.monitor, batch_size, start_time).await
    }

    fn mock_tb_meta(schema: &str, tb: &str) -> RdbTbMeta {
        RdbTbMeta {
            schema: schema.into(),
            tb: tb.into(),
            id_cols: vec![MongoConstants::ID.into()],
            cols: Vec::new(),
            key_map: HashMap::new(),
            order_col: None,
            partition_col: String::new(),
        }
    }

    fn build_row_data(schema: &str, tb: &str, doc: Document, key: &MongoKey) -> RowData {
        let mut after = HashMap::new();
        after.insert(
            MongoConstants::ID.to_string(),
            ColValue::String(key.to_string()),
        );
        after.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(doc));
        RowData {
            schema: schema.into(),
            tb: tb.into(),
            row_type: RowType::Insert,
            after: Some(after),
            before: None,
        }
    }
}