use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use mongodb::{
    bson::{doc, Document},
    Client,
};
use tokio::time::Instant;

use crate::{
    call_batch_fn,
    rdb_router::RdbRouter,
    sinker::{base_checker::BaseChecker, base_sinker::BaseSinker},
    Sinker,
};
use dt_common::{
    log_error,
    meta::{
        col_value::ColValue,
        mongo::{mongo_constant::MongoConstants, mongo_key::MongoKey},
        rdb_tb_meta::RdbTbMeta,
        row_data::RowData,
        row_type::RowType,
    },
    monitor::monitor::Monitor,
    utils::limit_queue::LimitedQueue,
};

#[derive(Clone)]
pub struct MongoChecker {
    pub reverse_router: RdbRouter,
    pub batch_size: usize,
    pub mongo_client: Client,
    pub monitor: Arc<Monitor>,
    pub output_full_row: bool,
}

#[async_trait]
impl Sinker for MongoChecker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, _batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        call_batch_fn!(self, data, Self::batch_check);
        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.mongo_client.clone().shutdown().await;
        Ok(())
    }
}

impl MongoChecker {
    async fn batch_check(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
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
        let start_time = Instant::now();
        let mut rts = LimitedQueue::new(1);
        let mut cursor = collection.find(filter, None).await?;
        rts.push((start_time.elapsed().as_millis() as u64, 1));
        BaseSinker::update_monitor_rt(&self.monitor, &rts).await?;

        while cursor.advance().await? {
            let doc = cursor.deserialize_current()?;
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
                let diff_col_values = BaseChecker::compare_row_data(&src_row_data, &dst_row_data);
                if !diff_col_values.is_empty() {
                    let diff_log = BaseChecker::build_mongo_diff_log(
                        src_row_data,
                        dst_row_data,
                        diff_col_values,
                        &tb_meta,
                        &self.reverse_router,
                        self.output_full_row,
                    );
                    diff.push(diff_log);
                }
            } else {
                let miss_log = BaseChecker::build_mongo_miss_log(
                    src_row_data,
                    &tb_meta,
                    &self.reverse_router,
                    self.output_full_row,
                );
                miss.push(miss_log);
            };
        }
        BaseChecker::log_dml(miss, diff);

        BaseSinker::update_batch_monitor(&self.monitor, batch_size as u64, 0).await
    }

    fn mock_tb_meta(schema: &str, tb: &str) -> RdbTbMeta {
        RdbTbMeta {
            schema: schema.into(),
            tb: tb.into(),
            id_cols: vec![MongoConstants::ID.into()],
            ..Default::default()
        }
    }

    fn build_row_data(schema: &str, tb: &str, doc: Document, key: &MongoKey) -> RowData {
        let mut after = HashMap::new();
        after.insert(
            MongoConstants::ID.to_string(),
            ColValue::String(key.to_string()),
        );
        after.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(doc));
        RowData::new(schema.into(), tb.into(), RowType::Insert, None, Some(after))
    }
}
