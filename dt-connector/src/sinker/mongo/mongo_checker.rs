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
        base_checker::{BaseChecker, CheckResult, ReviseSqlContext},
        base_sinker::BaseSinker,
    },
    Sinker,
};
use dt_common::{
    log_error, log_sql, log_summary,
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
        let mut src_row_data_map: HashMap<MongoKey, &RowData> = HashMap::new();

        for row_data in data.iter().skip(start_index).take(batch_size) {
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
                ids.push(id);
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

        // batch check
        let mut miss = Vec::new();
        let mut diff = Vec::new();
        let mut sql_count = 0;
        let revise_ctx = self.output_revise_sql.then(ReviseSqlContext::mongo);
        let recheck_delay_secs = self.recheck_interval_secs;
        let recheck_times = self.recheck_attempts;

        let collection = collection.clone();
        let schema_clone = schema.to_string();
        let tb_clone = tb.to_string();
        let fetch_latest = |src_row: &RowData| {
            let collection = collection.clone();
            let schema = schema_clone.clone();
            let tb = tb_clone.clone();
            let src_row = src_row.clone();
            async move {
                let after = src_row.require_after()?;
                let doc = after
                    .get(MongoConstants::DOC)
                    .and_then(|v| match v {
                        ColValue::MongoDoc(doc) => Some(doc),
                        _ => None,
                    })
                    .context("missing mongo doc")?;

                let id = doc.get(MongoConstants::ID).context("missing _id")?;
                let filter = doc! { MongoConstants::ID: id };

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

        for (key, src_row_data) in src_row_data_map {
            let dst_row_data = dst_row_data_map.remove(&key);

            let (check_result, final_dst_row) = BaseChecker::check_row_with_retry(
                src_row_data,
                dst_row_data,
                recheck_delay_secs,
                recheck_times,
                fetch_latest,
            )
            .await?;

            match check_result {
                CheckResult::Diff(diff_col_values) => {
                    let dst_row = final_dst_row
                        .as_ref()
                        .expect("diff result should have a dst row");
                    let revise_sql = revise_ctx
                        .as_ref()
                        .map(|ctx| ctx.build_diff_sql(src_row_data, dst_row, &diff_col_values))
                        .transpose()?
                        .flatten();

                    if let Some(revise_sql) = &revise_sql {
                        log_sql!("{}", revise_sql);
                        sql_count += 1;
                    }

                    let diff_log = BaseChecker::build_mongo_diff_log(
                        src_row_data,
                        dst_row,
                        diff_col_values,
                        &tb_meta,
                        &self.reverse_router,
                        self.output_full_row,
                    )?;
                    diff.push(diff_log);
                }
                CheckResult::Miss => {
                    let revise_sql = revise_ctx
                        .as_ref()
                        .map(|ctx| ctx.build_miss_sql(src_row_data))
                        .transpose()?
                        .flatten();

                    if let Some(revise_sql) = &revise_sql {
                        log_sql!("{}", revise_sql);
                        sql_count += 1;
                    }

                    let miss_log = BaseChecker::build_mongo_miss_log(
                        src_row_data,
                        &tb_meta,
                        &self.reverse_router,
                        self.output_full_row,
                    )?;
                    miss.push(miss_log);
                }
                CheckResult::Ok => {}
            }
        }
        BaseChecker::log_dml(&miss, &diff);

        self.summary.end_time = chrono::Local::now().to_rfc3339();
        self.summary.miss_count += miss.len();
        self.summary.diff_count += diff.len();
        if sql_count > 0 {
            self.summary.sql_count = Some(self.summary.sql_count.unwrap_or(0) + sql_count);
        }

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
