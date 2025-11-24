use std::{cmp, sync::Arc};

use anyhow::Context;
use async_trait::async_trait;
use mongodb::{
    bson::{doc, Document},
    options::UpdateOptions,
    Client, Collection,
};
use tokio::time::Instant;

use crate::{call_batch_fn, rdb_router::RdbRouter, sinker::base_sinker::BaseSinker, Sinker};
use dt_common::{
    log_error,
    meta::{
        col_value::ColValue, mongo::mongo_constant::MongoConstants, row_data::RowData,
        row_type::RowType,
    },
    monitor::monitor::Monitor,
    utils::limit_queue::LimitedQueue,
};

#[derive(Clone)]
pub struct MongoSinker {
    pub router: RdbRouter,
    pub batch_size: usize,
    pub mongo_client: Client,
    pub monitor: Arc<Monitor>,
    pub monitor_interval: u64,
}

#[async_trait]
impl Sinker for MongoSinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        if !batch {
            self.serial_sink(data).await?;
        } else {
            match data[0].row_type {
                RowType::Insert => {
                    call_batch_fn!(self, data, Self::batch_insert);
                }
                RowType::Delete => {
                    call_batch_fn!(self, data, Self::batch_delete);
                }
                _ => self.serial_sink(data).await?,
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl MongoSinker {
    async fn serial_sink(&mut self, mut data: Vec<RowData>) -> anyhow::Result<()> {
        let mut rts = LimitedQueue::new(cmp::min(100, data.len()));
        let monitor_interval = if self.monitor_interval > 0 {
            self.monitor_interval
        } else {
            10
        };
        let mut data_size = 0;
        let mut data_len = 0;
        let mut last_monitor_time = Instant::now();

        for row_data in data.iter_mut() {
            data_size += row_data.data_size;
            data_len += 1;

            let collection = self
                .mongo_client
                .database(&row_data.schema)
                .collection::<Document>(&row_data.tb);

            let start_time = Instant::now();
            match row_data.row_type {
                RowType::Insert => {
                    let after = row_data.require_after_mut()?;
                    if let Some(ColValue::MongoDoc(doc)) = after.remove(MongoConstants::DOC) {
                        let id = doc
                            .get(MongoConstants::ID)
                            .context("mongo doc missing `_id`")?;
                        let query_doc = doc! {MongoConstants::ID: id};
                        let update_doc = doc! {MongoConstants::SET: doc};
                        self.upsert(&collection, query_doc, update_doc).await?;
                        rts.push((start_time.elapsed().as_millis() as u64, 1));
                    }
                }

                RowType::Delete => {
                    let before = row_data.require_before_mut()?;
                    if let Some(ColValue::MongoDoc(doc)) = before.remove(MongoConstants::DOC) {
                        let id = doc
                            .get(MongoConstants::ID)
                            .context("mongo doc missing `_id`")?;
                        let query_doc = doc! {MongoConstants::ID: id};
                        collection.delete_one(query_doc, None).await?;
                        rts.push((start_time.elapsed().as_millis() as u64, 1));
                    }
                }

                RowType::Update => {
                    let query_doc = {
                        let before = row_data.require_before_mut()?;
                        if let Some(ColValue::MongoDoc(doc)) = before.remove(MongoConstants::DOC) {
                            let id = doc
                                .get(MongoConstants::ID)
                                .context("mongo doc missing `_id`")?;
                            Some(doc! {MongoConstants::ID: id})
                        } else {
                            None
                        }
                    };

                    let update_doc = {
                        let after = row_data.require_after_mut()?;
                        if let Some(ColValue::MongoDoc(doc)) = after.remove(MongoConstants::DOC) {
                            Some(doc)
                        } else if let Some(ColValue::MongoDoc(doc)) =
                            after.remove(MongoConstants::DIFF_DOC)
                        {
                            // for Update row_data from oplog (NOT change stream), after contains diff_doc instead of doc
                            Some(doc)
                        } else {
                            None
                        }
                    };

                    if query_doc.is_some() && update_doc.is_some() {
                        self.upsert(&collection, query_doc.unwrap(), update_doc.unwrap())
                            .await?;
                        rts.push((start_time.elapsed().as_millis() as u64, 1));
                    }
                }
            }

            if last_monitor_time.elapsed().as_secs() >= monitor_interval {
                BaseSinker::update_serial_monitor(&self.monitor, data_len as u64, data_size as u64)
                    .await?;
                BaseSinker::update_monitor_rt(&self.monitor, &rts).await?;
                rts.clear();
                data_size = 0;
                data_len = 0;
                last_monitor_time = Instant::now();
            }
        }

        if data_len > 0 || data_size > 0 {
            BaseSinker::update_serial_monitor(&self.monitor, data_len as u64, data_size as u64)
                .await?;
            BaseSinker::update_monitor_rt(&self.monitor, &rts).await?;
        }
        Ok(())
    }

    async fn batch_delete(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let mut data_size = 0;

        let collection = self
            .mongo_client
            .database(&data[0].schema)
            .collection::<Document>(&data[0].tb);

        let mut ids = Vec::new();
        for rd in data.iter().skip(start_index).take(batch_size) {
            data_size += rd.data_size;

            let before = rd.require_before()?;
            if let Some(ColValue::MongoDoc(doc)) = before.get(MongoConstants::DOC) {
                let id = doc
                    .get(MongoConstants::ID)
                    .context("mongo doc missing `_id`")?;
                ids.push(id);
            }
        }

        let query = doc! {
            MongoConstants::ID: {
                "$in": ids
            }
        };
        let start_time = Instant::now();
        let mut rts = LimitedQueue::new(1);
        collection.delete_many(query, None).await?;
        rts.push((start_time.elapsed().as_millis() as u64, 1));

        BaseSinker::update_batch_monitor(&self.monitor, batch_size as u64, data_size as u64)
            .await?;
        BaseSinker::update_monitor_rt(&self.monitor, &rts).await
    }

    async fn batch_insert(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let mut data_size = 0;

        let db = &data[0].schema;
        let tb = &data[0].tb;
        let collection = self.mongo_client.database(db).collection::<Document>(tb);

        let mut docs = Vec::new();
        for rd in data.iter().skip(start_index).take(batch_size) {
            data_size += rd.data_size;

            let after = rd.require_after()?;
            if let Some(ColValue::MongoDoc(doc)) = after.get(MongoConstants::DOC) {
                docs.push(doc);
            }
        }

        if let Err(error) = collection.insert_many(docs, None).await {
            log_error!(
                "batch insert failed, will insert one by one, schema: {}, tb: {}, error: {}",
                db,
                tb,
                error.to_string()
            );
            let sub_data = &data[start_index..start_index + batch_size];
            self.serial_sink(sub_data.to_vec()).await?;
        }

        BaseSinker::update_batch_monitor(&self.monitor, batch_size as u64, data_size as u64).await
    }

    async fn upsert(
        &mut self,
        collection: &Collection<Document>,
        query_doc: Document,
        update_doc: Document,
    ) -> anyhow::Result<()> {
        let options = UpdateOptions::builder().upsert(true).build();
        collection
            .update_one(query_doc, update_doc, Some(options))
            .await?;
        Ok(())
    }
}
