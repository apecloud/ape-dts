use std::{collections::HashMap, sync::Arc};

use async_mutex::Mutex;

use anyhow::Context;
use async_trait::async_trait;
use mongodb::{
    bson::{doc, Document},
    Client,
};
use tokio::time::Instant;

use crate::{
    call_batch_fn,
    check_log::check_log::CheckSummaryLog,
    rdb_router::RdbRouter,
    sinker::{
        base_checker::{BaseChecker, CheckItemContext, ReviseSqlContext},
        base_sinker::BaseSinker,
    },
    Sinker,
};
use dt_common::{
    log_error, log_summary,
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
    pub output_revise_sql: bool,
    pub recheck_interval_secs: u64,
    pub recheck_attempts: u32,
    pub summary: CheckSummaryLog,
    pub global_summary: Option<Arc<Mutex<CheckSummaryLog>>>,
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
        if self.summary.miss_count > 0
            || self.summary.diff_count > 0
            || self.summary.extra_count > 0
        {
            self.summary.end_time = chrono::Local::now().to_rfc3339();
            if let Some(global_summary) = &self.global_summary {
                let mut global_summary = global_summary.lock().await;
                global_summary.merge(&self.summary);
            } else {
                log_summary!("{}", self.summary);
            }
        }
        Ok(())
    }
}

impl MongoChecker {
    async fn batch_check(
        &mut self,
        data: &mut [RowData],
        start: usize,
        batch: usize,
    ) -> anyhow::Result<()> {
        let schema = &data[0].schema;
        let tb = &data[0].tb;
        let collection = self
            .mongo_client
            .database(schema)
            .collection::<Document>(tb);

        let mut ids = Vec::new();
        let mut src_row_data_map: HashMap<MongoKey, &RowData> = HashMap::new();

        for row_data in data.iter().skip(start).take(batch) {
            let after = row_data.require_after()?;

            let doc = after
                .get(MongoConstants::DOC)
                .and_then(|v| match v {
                    ColValue::MongoDoc(doc) => Some(doc),
                    _ => None,
                })
                .with_context(|| {
                    format!(
                        "row_data missing mongo doc, schema: {}, tb: {}",
                        row_data.schema, row_data.tb
                    )
                })?;

            let id = doc.get(MongoConstants::ID).with_context(|| {
                format!(
                    "row_data missing `_id`, schema: {}, tb: {}",
                    row_data.schema, row_data.tb
                )
            })?;

            if let Some(key) = MongoKey::from_doc(doc) {
                src_row_data_map.insert(key, row_data);
                ids.push(id.clone());
            } else {
                // this should have a very small chance to happen, and we don't support
                log_error!("row_data's _id type not supported, _id: {:?}", id);
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

        let tb_meta: Arc<RdbTbMeta> = Arc::new(Self::mock_tb_meta(schema, tb));
        let tb_meta_cloned = tb_meta.clone();
        let mongo_client = self.mongo_client.clone();
        let fetch_latest = move |_: MongoKey, src_row: &RowData| {
            let mongo_client = mongo_client.clone();
            let _tb_meta = tb_meta_cloned.clone();
            let schema = src_row.schema.clone();
            let tb = src_row.tb.clone();
            let doc = src_row
                .after
                .as_ref()
                .and_then(|a| a.get(MongoConstants::DOC));
            let id = doc.and_then(|v| match v {
                ColValue::MongoDoc(doc) => doc.get(MongoConstants::ID).cloned(),
                _ => None,
            });

            async move {
                let id = id.context("missing _id")?;
                let filter = doc! { MongoConstants::ID: id };
                let collection = mongo_client.database(&schema).collection::<Document>(&tb);

                if let Some(doc) = collection.find_one(filter, None).await? {
                    if let Some(key) = MongoKey::from_doc(&doc) {
                        let row_data = Self::build_row_data(&schema, &tb, doc, &key);
                        Ok(Some(row_data))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
        };

        while cursor.advance().await? {
            let doc = cursor.deserialize_current()?;
            // key should not be none since we have filtered in ids,
            if let Some(key) = MongoKey::from_doc(&doc) {
                let row_data = Self::build_row_data(schema, tb, doc, &key);
                dst_row_data_map.insert(key, row_data);
            } else {
                let id = doc.get(MongoConstants::ID);
                log_error!("dst row_data's _id type not supported, _id: {:?}", id);
            }
        }

        let revise_ctx = self
            .output_revise_sql
            .then(|| ReviseSqlContext::mongo(tb_meta.as_ref()));

        let recheck_delay_secs = self.recheck_interval_secs;
        let recheck_attempts = self.recheck_attempts;

        let ctx = CheckItemContext {
            extractor_meta_manager: None,
            reverse_router: &self.reverse_router,
            output_full_row: self.output_full_row,
            revise_ctx: revise_ctx.as_ref(),
            tb_meta: tb_meta.as_ref(),
        };

        let (miss, diff, sql_count) = BaseChecker::batch_compare_row_data_items(
            &data[start..((start + batch).min(data.len()))],
            dst_row_data_map,
            ctx,
            recheck_delay_secs,
            recheck_attempts,
            |r| {
                let doc = r
                    .after
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("missing after"))?
                    .get(MongoConstants::DOC)
                    .ok_or_else(|| anyhow::anyhow!("missing DOC"))?;
                match doc {
                    ColValue::MongoDoc(doc) => MongoKey::from_doc(doc)
                        .ok_or_else(|| anyhow::anyhow!("failed to parse MongoKey")),
                    _ => Err(anyhow::anyhow!("not a MongoDoc")),
                }
            },
            |key, r| fetch_latest(key, r),
        )
        .await?;

        BaseChecker::log_dml(&miss, &diff);

        self.summary.miss_count += miss.len();
        self.summary.diff_count += diff.len();
        if sql_count > 0 {
            self.summary.sql_count = Some(self.summary.sql_count.unwrap_or(0) + sql_count);
        }

        Ok(())
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

#[cfg(test)]
mod tests {
    use mongodb::bson::{oid::ObjectId, Bson, DateTime};

    #[test]
    fn test_bson_display() {
        let oid = ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap();
        let bson_oid = Bson::ObjectId(oid);
        println!("ObjectId: {}", bson_oid);

        let bson_int = Bson::Int32(123);
        println!("Int32: {}", bson_int);

        let bson_str = Bson::String("hello".to_string());
        println!("String: {}", bson_str);

        let date = DateTime::from_millis(1693470874000);
        let bson_date = Bson::DateTime(date);
        println!("DateTime: {}", bson_date);
    }
}
