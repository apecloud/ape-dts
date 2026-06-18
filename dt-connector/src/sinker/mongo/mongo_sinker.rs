use std::{cmp, collections::HashMap};

use anyhow::Context;
use async_trait::async_trait;
use mongodb::{
    bson::{doc, Document},
    Client, Collection,
};
use tokio::time::Instant;

use crate::sinker::checkable_sinker::CheckableSink;
use crate::{call_batch_fn, rdb_router::RdbRouter, sinker::base_sinker::BaseSinker, Sinker};
use dt_common::{
    log_error,
    meta::{
        col_value::ColValue,
        ddl_meta::{ddl_data::DdlData, ddl_type::DdlType},
        mongo::{
            mongo_constant::MongoConstants,
            mongo_ddl::query_to_command,
            mongo_shard::{list_shard_collections, MongoShardCollection},
        },
        row_data::RowData,
        row_type::RowType,
    },
    utils::limit_queue::LimitedQueue,
};

#[derive(Clone)]
pub struct MongoSinker {
    pub router: Option<RdbRouter>,
    pub batch_size: usize,
    pub mongo_client: Client,
    pub base_sinker: BaseSinker,
    pub target_shard_collections: HashMap<String, MongoShardCollection>,
    pub require_shard_key_filter: bool,
    pub is_target_mongos: bool,
}

#[async_trait]
impl Sinker for MongoSinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        if !batch {
            self.serial_sink(&data).await?;
        } else {
            match data[0].row_type {
                RowType::Insert => {
                    call_batch_fn!(self, data, Self::batch_insert);
                }
                RowType::Delete => {
                    call_batch_fn!(self, data, Self::batch_delete);
                }
                _ => self.serial_sink(&data).await?,
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn sink_ddl(&mut self, data: Vec<DdlData>, _batch: bool) -> anyhow::Result<()> {
        for ddl_data in data {
            if !self.is_target_mongos && ddl_data.ddl_type.is_mongo_shard_ddl() {
                continue;
            }
            self.run_ddl(&ddl_data).await?;
        }
        (_, self.target_shard_collections) = list_shard_collections(&self.mongo_client).await?;
        Ok(())
    }
}

#[async_trait]
impl CheckableSink for MongoSinker {
    async fn sink_dml_borrowed(&mut self, data: &mut [RowData], batch: bool) -> anyhow::Result<()> {
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
}

impl MongoSinker {
    fn target_ns(&self, row_data: &RowData) -> String {
        format!("{}.{}", row_data.schema, row_data.tb)
    }

    fn target_shard_collection(&self, row_data: &RowData) -> Option<&MongoShardCollection> {
        self.target_shard_collections.get(&self.target_ns(row_data))
    }

    fn is_target_sharded(&self, row_data: &RowData) -> bool {
        self.target_shard_collection(row_data).is_some()
    }

    fn mongo_doc<'a>(fields: &'a HashMap<String, ColValue>, key: &str) -> Option<&'a Document> {
        match fields.get(key) {
            Some(ColValue::MongoDoc(doc)) => Some(doc),
            _ => None,
        }
    }

    fn complete_shard_filter(
        &self,
        row_data: &RowData,
        document_key: Option<&Document>,
        full_doc: Option<&Document>,
    ) -> anyhow::Result<Document> {
        self.complete_shard_filter_with_priority(row_data, document_key, full_doc, false)
    }

    fn complete_shard_filter_prefer_full_doc(
        &self,
        row_data: &RowData,
        document_key: Option<&Document>,
        full_doc: Option<&Document>,
    ) -> anyhow::Result<Document> {
        self.complete_shard_filter_with_priority(row_data, document_key, full_doc, true)
    }

    fn complete_shard_filter_with_priority(
        &self,
        row_data: &RowData,
        document_key: Option<&Document>,
        full_doc: Option<&Document>,
        prefer_full_doc_shard_keys: bool,
    ) -> anyhow::Result<Document> {
        let shard_collection = match self.target_shard_collection(row_data) {
            Some(shard_collection) => shard_collection,
            None => {
                let doc = document_key
                    .or(full_doc)
                    .context("mongo doc missing for filter")?;
                let id = doc
                    .get(MongoConstants::ID)
                    .context("mongo doc missing `_id`")?;
                return Ok(doc! { MongoConstants::ID: id.clone() });
            }
        };

        let mut filter = Document::new();
        if prefer_full_doc_shard_keys {
            for key in shard_collection.key.keys() {
                if let Some(value) = full_doc.and_then(|doc| doc.get(key)) {
                    filter.insert(key, value.clone());
                }
            }
        }

        if let Some(document_key) = document_key {
            for (key, value) in document_key {
                if !filter.contains_key(key) {
                    filter.insert(key, value.clone());
                }
            }
        }

        for key in shard_collection.key.keys() {
            if !filter.contains_key(key) {
                if let Some(value) = full_doc.and_then(|doc| doc.get(key)) {
                    filter.insert(key, value.clone());
                }
            }
        }

        if !filter.contains_key(MongoConstants::ID) {
            if let Some(value) = full_doc
                .and_then(|doc| doc.get(MongoConstants::ID))
                .or_else(|| document_key.and_then(|doc| doc.get(MongoConstants::ID)))
            {
                filter.insert(MongoConstants::ID, value.clone());
            }
        }

        let missing_keys: Vec<_> = shard_collection
            .key
            .keys()
            .filter(|key| !filter.contains_key(*key))
            .cloned()
            .collect();
        if self.require_shard_key_filter && !missing_keys.is_empty() {
            anyhow::bail!(
                "mongo target collection [{}] is sharded, but row filter is missing shard key field(s): {:?}",
                shard_collection.ns,
                missing_keys
            );
        }

        if filter.is_empty() {
            anyhow::bail!(
                "mongo target collection [{}] is sharded, but row filter is empty",
                shard_collection.ns
            );
        }
        Ok(filter)
    }

    fn shard_key_changed(
        &self,
        row_data: &RowData,
        old_doc: Option<&Document>,
        old_doc_is_pre_image: bool,
        full_doc: Option<&Document>,
    ) -> bool {
        let Some(shard_collection) = self.target_shard_collection(row_data) else {
            return false;
        };
        let (Some(old_doc), Some(full_doc)) = (old_doc, full_doc) else {
            return false;
        };

        shard_collection.key.keys().any(|key| {
            let old_value = old_doc.get(key);
            let new_value = full_doc.get(key);
            if old_doc_is_pre_image {
                old_value != new_value
            } else {
                old_value.is_some() && old_value != new_value
            }
        })
    }

    fn id_filter(document_key: Option<&Document>, full_doc: Option<&Document>) -> Option<Document> {
        let id = document_key
            .and_then(|doc| doc.get(MongoConstants::ID))
            .or_else(|| full_doc.and_then(|doc| doc.get(MongoConstants::ID)))?;
        Some(doc! { MongoConstants::ID: id.clone() })
    }

    async fn run_ddl(&mut self, ddl_data: &DdlData) -> anyhow::Result<()> {
        let mut command = query_to_command(&ddl_data.query)?;
        self.rewrite_ddl_command_namespace(ddl_data, &mut command);

        match ddl_data.ddl_type {
            DdlType::MongoDropDatabase => {
                let (db, _) = ddl_data.get_schema_tb();
                self.mongo_client.database(&db).drop().await?;
            }

            DdlType::MongoShardCollection => {
                if self.ensure_shard_collection_command(&command).await? {
                    self.run_admin_command(command).await?;
                }
            }

            DdlType::MongoReshardCollection | DdlType::MongoRefineCollectionShardKey => {
                self.run_admin_command(command).await?;
            }

            DdlType::MongoRenameCollection => {
                self.run_admin_command(command).await?;
            }

            DdlType::MongoCreateCollection
            | DdlType::MongoDropCollection
            | DdlType::MongoCreateIndex
            | DdlType::MongoDropIndex
            | DdlType::MongoCollMod => {
                let (db, _) = ddl_data.get_schema_tb();
                self.mongo_client.database(&db).run_command(command).await?;
            }

            _ => {}
        }
        Ok(())
    }

    async fn run_admin_command(&self, command: Document) -> anyhow::Result<()> {
        self.mongo_client
            .database("admin")
            .run_command(command)
            .await?;
        Ok(())
    }

    async fn ensure_shard_collection_command(&self, command: &Document) -> anyhow::Result<bool> {
        let ns = command
            .get_str("shardCollection")
            .context("mongo shardCollection command missing namespace")?;
        let (db, _) = ns
            .split_once('.')
            .context("mongo shardCollection namespace missing db")?;
        if let Some(existing) = self.target_shard_collections.get(ns) {
            let key = command
                .get_document("key")
                .context("mongo shardCollection command missing key")?;
            let unique = command.get_bool("unique").unwrap_or(false);
            if existing.key != *key || existing.unique != unique {
                anyhow::bail!(
                    "mongo target collection [{}] shard key mismatch, source key: {:?}, source unique: {}, target key: {:?}, target unique: {}",
                    ns,
                    key,
                    unique,
                    existing.key,
                    existing.unique,
                );
            }
            return Ok(false);
        }

        self.mongo_client
            .database("admin")
            .run_command(doc! { "enableSharding": db })
            .await?;
        Ok(true)
    }

    fn rewrite_ddl_command_namespace(&self, ddl_data: &DdlData, command: &mut Document) {
        let (db, tb) = ddl_data.get_schema_tb();
        let (new_db, new_tb) = ddl_data.get_rename_to_schema_tb();
        for command_name in ["create", "drop", "createIndexes", "dropIndexes", "collMod"] {
            if command.contains_key(command_name) && !tb.is_empty() {
                command.insert(command_name, tb.clone());
                return;
            }
        }

        if command.contains_key("renameCollection") {
            command.insert("renameCollection", format!("{}.{}", db, tb));
            command.insert("to", format!("{}.{}", new_db, new_tb));
            return;
        }

        for command_name in [
            "shardCollection",
            "reshardCollection",
            "refineCollectionShardKey",
        ] {
            if command.contains_key(command_name) && !tb.is_empty() {
                command.insert(command_name, format!("{}.{}", db, tb));
                return;
            }
        }
    }

    async fn serial_sink(&mut self, data: &[RowData]) -> anyhow::Result<()> {
        let task_id = self.base_sinker.source_task_id_for_rows(data, &self.router);
        self.base_sinker.ensure_monitor_for(&task_id);
        let mut rts = LimitedQueue::new(cmp::min(100, data.len()));
        let monitor_interval = self.base_sinker.monitor_interval_secs();
        let mut data_size = 0;
        let mut data_len = 0;
        let mut last_monitor_time = Instant::now();

        for row_data in data.iter() {
            data_size += row_data.get_data_size() as usize;
            data_len += 1;

            let collection = self
                .mongo_client
                .database(&row_data.schema)
                .collection::<Document>(&row_data.tb);

            let start_time = Instant::now();
            match row_data.row_type {
                RowType::Insert => {
                    let after = row_data.require_after()?;
                    if let Some(doc) = Self::mongo_doc(after, MongoConstants::DOC) {
                        let query_doc = self.complete_shard_filter(
                            row_data,
                            Self::mongo_doc(after, MongoConstants::DOCUMENT_KEY),
                            Some(doc),
                        )?;
                        let update_doc = doc! {MongoConstants::SET: doc.clone()};
                        self.upsert(&collection, query_doc, update_doc).await?;
                        rts.push((start_time.elapsed().as_millis() as u64, 1));
                    }
                }

                RowType::Delete => {
                    let before = row_data.require_before()?;
                    if let Some(doc) = Self::mongo_doc(before, MongoConstants::DOC) {
                        let query_doc = self.complete_shard_filter(
                            row_data,
                            Self::mongo_doc(before, MongoConstants::DOCUMENT_KEY).or(Some(doc)),
                            None,
                        )?;
                        collection.delete_one(query_doc).await?;
                        rts.push((start_time.elapsed().as_millis() as u64, 1));
                    }
                }

                RowType::Update => {
                    let before = row_data.require_before()?;
                    let before_doc =
                        before
                            .get(MongoConstants::DOC)
                            .and_then(|value| match value {
                                ColValue::MongoDoc(doc) => Some(doc),
                                _ => None,
                            });
                    let pre_image = Self::mongo_doc(before, MongoConstants::PRE_IMAGE);
                    let document_key = Self::mongo_doc(before, MongoConstants::DOCUMENT_KEY);
                    let old_shard_doc = pre_image.or(document_key).or(before_doc);
                    let after_full_doc = row_data
                        .after
                        .as_ref()
                        .and_then(|after| Self::mongo_doc(after, MongoConstants::DOC));

                    if self.shard_key_changed(
                        row_data,
                        old_shard_doc,
                        pre_image.is_some(),
                        after_full_doc,
                    ) {
                        let old_filter = if let Some(pre_image) = pre_image {
                            self.complete_shard_filter_prefer_full_doc(
                                row_data,
                                document_key,
                                Some(pre_image),
                            )?
                        } else {
                            self.complete_shard_filter(row_data, document_key.or(before_doc), None)?
                        };
                        let new_doc = after_full_doc
                            .context("mongo shard key update requires full document after image")?;
                        let new_filter =
                            self.complete_shard_filter(row_data, None, Some(new_doc))?;
                        if !self
                            .replace_existing(&collection, old_filter, new_doc.clone())
                            .await?
                        {
                            self.replace(&collection, new_filter, new_doc.clone())
                                .await?;
                        }
                        rts.push((start_time.elapsed().as_millis() as u64, 1));
                        continue;
                    }

                    let query_doc =
                        {
                            if let Some(pre_image) = pre_image {
                                Some(self.complete_shard_filter_prefer_full_doc(
                                    row_data,
                                    document_key,
                                    Some(pre_image),
                                )?)
                            } else if let Some(doc) = before_doc {
                                Some(self.complete_shard_filter(
                                    row_data,
                                    document_key.or(Some(doc)),
                                    row_data.after.as_ref().and_then(|after| {
                                        Self::mongo_doc(after, MongoConstants::DOC)
                                    }),
                                )?)
                            } else if let Some(document_key) = document_key {
                                Some(self.complete_shard_filter(
                                    row_data,
                                    Some(document_key),
                                    row_data.after.as_ref().and_then(|after| {
                                        Self::mongo_doc(after, MongoConstants::DOC)
                                    }),
                                )?)
                            } else {
                                None
                            }
                        };

                    if let Some(query_doc) = query_doc {
                        let after = row_data.require_after()?;
                        if let Some(doc) = Self::mongo_doc(after, MongoConstants::DIFF_DOC) {
                            let after_full_doc = Self::mongo_doc(after, MongoConstants::DOC);
                            if let Some(after_full_doc) = after_full_doc {
                                self.update_existing_with_fallback(
                                    &collection,
                                    row_data,
                                    query_doc,
                                    doc.clone(),
                                    document_key,
                                    after_full_doc,
                                )
                                .await?;
                            } else {
                                self.upsert(&collection, query_doc, doc.clone()).await?;
                            }
                            rts.push((start_time.elapsed().as_millis() as u64, 1));
                        } else if let Some(doc) = Self::mongo_doc(after, MongoConstants::DOC) {
                            self.replace(&collection, query_doc, doc.clone()).await?;
                            rts.push((start_time.elapsed().as_millis() as u64, 1));
                        }
                    }
                }
            }

            if last_monitor_time.elapsed().as_secs() >= monitor_interval {
                self.base_sinker
                    .update_serial_monitor_for(&task_id, data_len as u64, data_size as u64)
                    .await?;
                self.base_sinker
                    .update_monitor_rt_for(&task_id, &rts)
                    .await?;
                rts.clear();
                data_size = 0;
                data_len = 0;
                last_monitor_time = Instant::now();
            }
        }

        if data_len > 0 || data_size > 0 {
            self.base_sinker
                .update_serial_monitor_for(&task_id, data_len as u64, data_size as u64)
                .await?;
            self.base_sinker
                .update_monitor_rt_for(&task_id, &rts)
                .await?;
        }
        Ok(())
    }

    async fn batch_delete(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let task_id = self
            .base_sinker
            .source_task_id_for_rows(&data[start_index..start_index + batch_size], &self.router);
        self.base_sinker.ensure_monitor_for(&task_id);
        let mut data_size = 0;

        let collection = self
            .mongo_client
            .database(&data[0].schema)
            .collection::<Document>(&data[0].tb);

        if data
            .iter()
            .skip(start_index)
            .take(batch_size)
            .any(|row_data| self.is_target_sharded(row_data))
        {
            return self
                .serial_sink(&data[start_index..start_index + batch_size])
                .await;
        }

        let mut ids = Vec::new();
        for rd in data.iter().skip(start_index).take(batch_size) {
            data_size += rd.get_data_size() as usize;

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
        collection.delete_many(query).await?;
        rts.push((start_time.elapsed().as_millis() as u64, 1));

        self.base_sinker
            .update_batch_monitor_for(&task_id, batch_size as u64, data_size as u64)
            .await?;
        self.base_sinker.update_monitor_rt_for(&task_id, &rts).await
    }

    async fn batch_insert(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let task_id = self
            .base_sinker
            .source_task_id_for_rows(&data[start_index..start_index + batch_size], &self.router);
        self.base_sinker.ensure_monitor_for(&task_id);
        let mut data_size = 0;

        let db = &data[0].schema;
        let tb = &data[0].tb;
        let collection = self.mongo_client.database(db).collection::<Document>(tb);

        let mut docs = Vec::new();
        for rd in data.iter().skip(start_index).take(batch_size) {
            data_size += rd.get_data_size() as usize;

            let after = rd.require_after()?;
            if let Some(ColValue::MongoDoc(doc)) = after.get(MongoConstants::DOC) {
                docs.push(doc.clone());
            }
        }

        if let Err(error) = collection.insert_many(docs).await {
            log_error!(
                "batch insert failed, will insert one by one, schema: {}, tb: {}, error: {}",
                db,
                tb,
                error.to_string()
            );
            let sub_data = &data[start_index..start_index + batch_size];
            self.serial_sink(sub_data).await?;
        }

        self.base_sinker
            .update_batch_monitor_for(&task_id, batch_size as u64, data_size as u64)
            .await
    }

    async fn upsert(
        &mut self,
        collection: &Collection<Document>,
        query_doc: Document,
        update_doc: Document,
    ) -> anyhow::Result<()> {
        collection
            .update_one(query_doc, update_doc)
            .upsert(true)
            .await?;
        Ok(())
    }

    async fn update_existing(
        &mut self,
        collection: &Collection<Document>,
        query_doc: Document,
        update_doc: Document,
    ) -> anyhow::Result<bool> {
        let result = collection.update_one(query_doc, update_doc).await?;
        Ok(result.matched_count > 0)
    }

    async fn update_existing_with_fallback(
        &mut self,
        collection: &Collection<Document>,
        row_data: &RowData,
        query_doc: Document,
        update_doc: Document,
        document_key: Option<&Document>,
        full_doc: &Document,
    ) -> anyhow::Result<()> {
        if self
            .update_existing(collection, query_doc, update_doc.clone())
            .await?
        {
            return Ok(());
        }

        if self.is_target_sharded(row_data) {
            if let Some(id_filter) = Self::id_filter(document_key, Some(full_doc)) {
                if let Some(target_doc) = collection.find_one(id_filter).await? {
                    let retry_filter = self.complete_shard_filter_prefer_full_doc(
                        row_data,
                        document_key,
                        Some(&target_doc),
                    )?;
                    if self
                        .update_existing(collection, retry_filter, update_doc.clone())
                        .await?
                    {
                        return Ok(());
                    }
                }
            }

            let new_filter = self.complete_shard_filter(row_data, None, Some(full_doc))?;
            if self
                .update_existing(collection, new_filter, update_doc.clone())
                .await?
            {
                return Ok(());
            }

            anyhow::bail!(
                "mongo update matched no target document for sharded collection [{}]",
                self.target_ns(row_data)
            );
        }

        if let Some(id_filter) = Self::id_filter(document_key, Some(full_doc)) {
            self.replace(collection, id_filter, full_doc.clone())
                .await?;
        }
        Ok(())
    }

    async fn replace(
        &mut self,
        collection: &Collection<Document>,
        query_doc: Document,
        replacement_doc: Document,
    ) -> anyhow::Result<()> {
        collection
            .replace_one(query_doc, replacement_doc)
            .upsert(true)
            .await?;
        Ok(())
    }

    async fn replace_existing(
        &mut self,
        collection: &Collection<Document>,
        query_doc: Document,
        replacement_doc: Document,
    ) -> anyhow::Result<bool> {
        let result = collection.replace_one(query_doc, replacement_doc).await?;
        Ok(result.matched_count > 0)
    }
}
