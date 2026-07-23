use std::{cmp, collections::HashMap};

use anyhow::Context;
use async_trait::async_trait;
use mongodb::{
    bson::{doc, raw::RawDocumentBuf, Bson, Document},
    Client, Collection,
};
use tokio::time::Instant;

use crate::{
    call_batch_fn,
    common::mongo::{changestream_parser, oplog_parser},
    rdb_router::RdbRouter,
    sinker::{base_sinker::BaseSinker, checkable_sinker::CheckableSink},
    Sinker,
};
use dt_common::{
    log_error, log_warn,
    meta::{
        col_value::ColValue,
        ddl_meta::{ddl_data::DdlData, ddl_type::DdlType},
        mongo::{
            mongo_constant::MongoConstants,
            mongo_ddl::query_to_command,
            mongo_shard::{get_shard_collection, MongoShardCollection},
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
    pub target_shard_collections: HashMap<String, Option<MongoShardCollection>>,
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
        self.target_shard_collections.clear();
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

    fn prepare_check_data(&self, data: Vec<RowData>) -> Vec<RowData> {
        let mut utf8_skipped = 0usize;
        let mut first_utf8_error = None;
        let mut malformed_skipped = 0usize;
        let mut first_malformed_error = None;
        let mut result = Vec::with_capacity(data.len());
        for mut row in data {
            let converted = row
                .before
                .iter_mut()
                .chain(row.after.iter_mut())
                .try_for_each(Self::convert_raw_doc_for_check);
            match converted {
                Ok(()) => result.push(row),
                Err(error)
                    if matches!(
                        &error.kind,
                        mongodb::bson::raw::ErrorKind::Utf8EncodingError(_)
                    ) =>
                {
                    utf8_skipped += 1;
                    first_utf8_error.get_or_insert_with(|| error.to_string());
                }
                Err(error) => {
                    malformed_skipped += 1;
                    first_malformed_error.get_or_insert_with(|| error.to_string());
                }
            }
        }

        if utf8_skipped > 0 {
            log_warn!(
                "mongo checker skipped {} row(s) containing invalid UTF-8, first error: {}",
                utf8_skipped,
                first_utf8_error.unwrap_or_default()
            );
        }
        if malformed_skipped > 0 {
            log_error!(
                "mongo checker skipped {} malformed raw BSON row(s), first error: {}",
                malformed_skipped,
                first_malformed_error.unwrap_or_default()
            );
        }
        result
    }
}

impl MongoSinker {
    fn target_ns(&self, row_data: &RowData) -> String {
        format!("{}.{}", row_data.schema, row_data.tb)
    }

    async fn target_shard_collection(
        &mut self,
        row_data: &RowData,
    ) -> anyhow::Result<Option<MongoShardCollection>> {
        let ns = self.target_ns(row_data);
        self.target_shard_collection_by_ns(&ns).await
    }

    async fn target_shard_collection_by_ns(
        &mut self,
        ns: &str,
    ) -> anyhow::Result<Option<MongoShardCollection>> {
        if let Some(shard_collection) = self.target_shard_collections.get(ns) {
            return Ok(shard_collection.clone());
        }

        let shard_collection = if self.is_target_mongos {
            get_shard_collection(&self.mongo_client, ns).await?
        } else {
            None
        };
        self.target_shard_collections
            .insert(ns.to_string(), shard_collection.clone());
        Ok(shard_collection)
    }

    async fn is_target_sharded(&mut self, row_data: &RowData) -> anyhow::Result<bool> {
        Ok(self.target_shard_collection(row_data).await?.is_some())
    }

    fn mongo_doc<'a>(fields: &'a HashMap<String, ColValue>, key: &str) -> Option<&'a Document> {
        match fields.get(key) {
            Some(ColValue::MongoDoc(doc)) => Some(doc),
            _ => None,
        }
    }

    fn mongo_raw_doc<'a>(
        fields: &'a HashMap<String, ColValue>,
        key: &str,
    ) -> Option<&'a RawDocumentBuf> {
        match fields.get(key) {
            Some(ColValue::MongoRawDoc(doc)) => Some(doc),
            _ => None,
        }
    }

    fn convert_raw_doc_for_check(
        fields: &mut HashMap<String, ColValue>,
    ) -> mongodb::bson::raw::Result<()> {
        let Some(ColValue::MongoRawDoc(raw_doc)) = fields.get(MongoConstants::DOC) else {
            return Ok(());
        };
        let doc = raw_doc.to_document()?;
        fields.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(doc));
        Ok(())
    }

    async fn complete_raw_shard_filter(
        &mut self,
        row_data: &RowData,
        raw_doc: &RawDocumentBuf,
    ) -> anyhow::Result<Document> {
        let shard_collection = self.target_shard_collection(row_data).await?;
        let mut filter = Document::new();

        // Raw rows keep non-routing fields opaque. Invalid UTF-8 in `_id` or shard-key values is
        // outside the supported scope and may therefore fail while building this filter.
        if let Some(shard_collection) = &shard_collection {
            for key in shard_collection.key.keys() {
                if let Some(value) = raw_doc.get(key)? {
                    filter.insert(key, Bson::try_from(value)?);
                }
            }
        }

        if let Some(value) = raw_doc.get(MongoConstants::ID)? {
            filter.insert(MongoConstants::ID, Bson::try_from(value)?);
        }

        let Some(shard_collection) = shard_collection else {
            if filter.contains_key(MongoConstants::ID) {
                return Ok(filter);
            }
            anyhow::bail!("mongo raw doc missing `_id`");
        };

        let missing_keys: Vec<_> = shard_collection
            .key
            .keys()
            .filter(|key| !filter.contains_key(*key))
            .cloned()
            .collect();
        if self.require_shard_key_filter && !missing_keys.is_empty() {
            anyhow::bail!(
                "mongo target collection [{}] is sharded, but raw row filter is missing shard key field(s): {:?}",
                shard_collection.ns,
                missing_keys
            );
        }
        if filter.is_empty() {
            anyhow::bail!(
                "mongo target collection [{}] is sharded, but raw row filter is empty",
                shard_collection.ns
            );
        }
        Ok(filter)
    }

    async fn complete_shard_filter(
        &mut self,
        row_data: &RowData,
        document_key: Option<&Document>,
        full_doc: Option<&Document>,
    ) -> anyhow::Result<Document> {
        self.complete_shard_filter_with_priority(row_data, document_key, full_doc, false)
            .await
    }

    async fn complete_shard_filter_prefer_full_doc(
        &mut self,
        row_data: &RowData,
        document_key: Option<&Document>,
        full_doc: Option<&Document>,
    ) -> anyhow::Result<Document> {
        self.complete_shard_filter_with_priority(row_data, document_key, full_doc, true)
            .await
    }

    async fn complete_shard_filter_with_priority(
        &mut self,
        row_data: &RowData,
        document_key: Option<&Document>,
        full_doc: Option<&Document>,
        prefer_full_doc_shard_keys: bool,
    ) -> anyhow::Result<Document> {
        let shard_collection = match self.target_shard_collection(row_data).await? {
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

    async fn shard_key_changed(
        &mut self,
        row_data: &RowData,
        old_doc: Option<&Document>,
        old_doc_is_pre_image: bool,
        full_doc: Option<&Document>,
    ) -> anyhow::Result<bool> {
        let Some(shard_collection) = self.target_shard_collection(row_data).await? else {
            return Ok(false);
        };
        let (Some(old_doc), Some(full_doc)) = (old_doc, full_doc) else {
            return Ok(false);
        };

        Ok(shard_collection.key.keys().any(|key| {
            let old_value = old_doc.get(key);
            let new_value = full_doc.get(key);
            if old_doc_is_pre_image {
                old_value != new_value
            } else {
                old_value.is_some() && old_value != new_value
            }
        }))
    }

    fn id_filter(document_key: Option<&Document>, full_doc: Option<&Document>) -> Option<Document> {
        let id = document_key
            .and_then(|doc| doc.get(MongoConstants::ID))
            .or_else(|| full_doc.and_then(|doc| doc.get(MongoConstants::ID)))?;
        Some(doc! { MongoConstants::ID: id.clone() })
    }

    fn raw_value(doc: &RawDocumentBuf, key: &str) -> anyhow::Result<Option<Bson>> {
        doc.get(key)?
            .map(Bson::try_from)
            .transpose()
            .map_err(Into::into)
    }

    fn raw_id_filter(
        document_key: Option<&Document>,
        full_doc: Option<&RawDocumentBuf>,
    ) -> anyhow::Result<Option<Document>> {
        let id = if let Some(id) = document_key.and_then(|doc| doc.get(MongoConstants::ID)) {
            Some(id.clone())
        } else if let Some(full_doc) = full_doc {
            Self::raw_value(full_doc, MongoConstants::ID)?
        } else {
            None
        };
        Ok(id.map(|id| doc! { MongoConstants::ID: id }))
    }

    async fn complete_shard_filter_with_raw_doc(
        &mut self,
        row_data: &RowData,
        document_key: Option<&Document>,
        full_doc: Option<&RawDocumentBuf>,
        prefer_full_doc_shard_keys: bool,
    ) -> anyhow::Result<Document> {
        let Some(shard_collection) = self.target_shard_collection(row_data).await? else {
            return Self::raw_id_filter(document_key, full_doc)?
                .context("mongo raw doc missing `_id`");
        };

        let mut filter = Document::new();
        if prefer_full_doc_shard_keys {
            if let Some(full_doc) = full_doc {
                for key in shard_collection.key.keys() {
                    if let Some(value) = Self::raw_value(full_doc, key)? {
                        filter.insert(key, value);
                    }
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

        if let Some(full_doc) = full_doc {
            for key in shard_collection.key.keys() {
                if !filter.contains_key(key) {
                    if let Some(value) = Self::raw_value(full_doc, key)? {
                        filter.insert(key, value);
                    }
                }
            }
            if !filter.contains_key(MongoConstants::ID) {
                if let Some(value) = Self::raw_value(full_doc, MongoConstants::ID)? {
                    filter.insert(MongoConstants::ID, value);
                }
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
                "mongo target collection [{}] is sharded, but raw row filter is missing shard key field(s): {:?}",
                shard_collection.ns,
                missing_keys
            );
        }
        if filter.is_empty() {
            anyhow::bail!(
                "mongo target collection [{}] is sharded, but raw row filter is empty",
                shard_collection.ns
            );
        }
        Ok(filter)
    }

    async fn raw_shard_key_changed(
        &mut self,
        row_data: &RowData,
        document_key: Option<&Document>,
        pre_image: Option<&RawDocumentBuf>,
        full_doc: Option<&RawDocumentBuf>,
    ) -> anyhow::Result<bool> {
        let Some(shard_collection) = self.target_shard_collection(row_data).await? else {
            return Ok(false);
        };
        let Some(full_doc) = full_doc else {
            return Ok(false);
        };

        for key in shard_collection.key.keys() {
            let old_value = if let Some(pre_image) = pre_image {
                Self::raw_value(pre_image, key)?
            } else {
                document_key.and_then(|doc| doc.get(key)).cloned()
            };
            let new_value = Self::raw_value(full_doc, key)?;
            if (pre_image.is_some() && old_value != new_value)
                || (pre_image.is_none() && old_value.is_some() && old_value != new_value)
            {
                return Ok(true);
            }
        }
        Ok(false)
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

    async fn ensure_shard_collection_command(
        &mut self,
        command: &Document,
    ) -> anyhow::Result<bool> {
        let ns = command
            .get_str("shardCollection")
            .context("mongo shardCollection command missing namespace")?;
        let (db, _) = ns
            .split_once('.')
            .context("mongo shardCollection namespace missing db")?;
        if let Some(existing) = self.target_shard_collection_by_ns(ns).await? {
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
                    if let Some(raw_doc) = Self::mongo_raw_doc(after, MongoConstants::DOC) {
                        if let Some(document_key) =
                            Self::mongo_doc(after, MongoConstants::DOCUMENT_KEY)
                        {
                            let raw_collection = self
                                .mongo_client
                                .database(&row_data.schema)
                                .collection::<RawDocumentBuf>(&row_data.tb);
                            if let Err(insert_error) = raw_collection.insert_one(raw_doc).await {
                                match raw_doc.to_document() {
                                    Ok(doc) => {
                                        // Preserve the previous CDC duplicate-insert fallback:
                                        // valid documents merge with `$set`, retaining target-only
                                        // fields. Parsing only happens after the raw insert fails.
                                        let query_doc = self
                                            .complete_shard_filter(
                                                row_data,
                                                Some(document_key),
                                                Some(&doc),
                                            )
                                            .await?;
                                        let update_doc = doc! {MongoConstants::SET: doc};
                                        self.upsert(&collection, query_doc, update_doc).await?;
                                    }
                                    Err(parse_error) => {
                                        log_warn!(
                                            "mongo CDC raw insert failed and cannot be converted to Document, use raw full-document replacement, schema: {}, table: {}, insert error: {}, parse error: {}",
                                            row_data.schema,
                                            row_data.tb,
                                            insert_error,
                                            parse_error
                                        );
                                        let query_doc = self
                                            .complete_raw_shard_filter(row_data, raw_doc)
                                            .await?;
                                        self.replace_raw(&raw_collection, query_doc, raw_doc)
                                            .await?;
                                    }
                                }
                            }
                        } else {
                            // Snapshot rows have no documentKey marker and represent complete source
                            // state, so duplicate fallback replaces stale target-only fields.
                            let query_doc =
                                self.complete_raw_shard_filter(row_data, raw_doc).await?;
                            let raw_collection = self
                                .mongo_client
                                .database(&row_data.schema)
                                .collection::<RawDocumentBuf>(&row_data.tb);
                            self.replace_raw(&raw_collection, query_doc, raw_doc)
                                .await?;
                        }
                        rts.push((start_time.elapsed().as_millis() as u64, 1));
                    } else if let Some(doc) = Self::mongo_doc(after, MongoConstants::DOC) {
                        let query_doc = self
                            .complete_shard_filter(
                                row_data,
                                Self::mongo_doc(after, MongoConstants::DOCUMENT_KEY),
                                Some(doc),
                            )
                            .await?;
                        let update_doc = doc! {MongoConstants::SET: doc};
                        self.upsert(&collection, query_doc, update_doc).await?;
                        rts.push((start_time.elapsed().as_millis() as u64, 1));
                    }
                }

                RowType::Delete => {
                    let before = row_data.require_before()?;
                    if let Some(doc) = Self::mongo_doc(before, MongoConstants::DOC) {
                        let query_doc = self
                            .complete_shard_filter(
                                row_data,
                                Self::mongo_doc(before, MongoConstants::DOCUMENT_KEY).or(Some(doc)),
                                None,
                            )
                            .await?;
                        collection.delete_one(query_doc).await?;
                        rts.push((start_time.elapsed().as_millis() as u64, 1));
                    } else if let Some(raw_doc) = Self::mongo_raw_doc(before, MongoConstants::DOC) {
                        let query_doc = self
                            .complete_shard_filter_with_raw_doc(
                                row_data,
                                Self::mongo_doc(before, MongoConstants::DOCUMENT_KEY),
                                Some(raw_doc),
                                false,
                            )
                            .await?;
                        collection.delete_one(query_doc).await?;
                        rts.push((start_time.elapsed().as_millis() as u64, 1));
                    }
                }

                RowType::Update
                    if row_data
                        .after
                        .as_ref()
                        .and_then(|after| {
                            Self::mongo_raw_doc(after, MongoConstants::OPLOG_DIFF_DOC)
                        })
                        .is_some() =>
                {
                    let before = row_data.require_before()?;
                    let after = row_data.require_after()?;
                    let raw_oplog_diff = Self::mongo_raw_doc(after, MongoConstants::OPLOG_DIFF_DOC)
                        .expect("raw Oplog update match guard checked the diff");
                    let oplog_doc = raw_oplog_diff.to_document().with_context(|| {
                        format!(
                            "mongo oplog update cannot be converted to Document, schema: {}, table: {}",
                            row_data.schema, row_data.tb
                        )
                    })?;
                    let update_doc = oplog_parser::build_update_doc(&oplog_doc);
                    if update_doc.is_empty() {
                        log_error!(
                            "update op_log is neither $set nor $unset, ignore, schema: {}, table: {}",
                            row_data.schema,
                            row_data.tb
                        );
                    } else {
                        let before_doc = Self::mongo_doc(before, MongoConstants::DOC)
                            .context("mongo raw oplog update missing document o2")?;
                        let query_doc = self
                            .complete_shard_filter(row_data, Some(before_doc), None)
                            .await?;
                        self.upsert(&collection, query_doc, update_doc).await?;
                        rts.push((start_time.elapsed().as_millis() as u64, 1));
                    }
                }

                RowType::Update => {
                    let before = row_data.require_before()?;
                    let after = row_data.require_after()?;
                    let raw_full_doc = Self::mongo_raw_doc(after, MongoConstants::DOC);
                    let raw_update_description =
                        Self::mongo_raw_doc(after, MongoConstants::DIFF_DOC);
                    if raw_full_doc.is_some() || raw_update_description.is_some() {
                        let raw_collection = self
                            .mongo_client
                            .database(&row_data.schema)
                            .collection::<RawDocumentBuf>(&row_data.tb);
                        let document_key = Self::mongo_doc(before, MongoConstants::DOCUMENT_KEY);
                        let pre_image = Self::mongo_raw_doc(before, MongoConstants::PRE_IMAGE);

                        if let Some(raw_update_description) = raw_update_description {
                            let update_description = match raw_update_description.to_document() {
                                Ok(update_description) => update_description,
                                Err(error) => {
                                    let full_doc = raw_full_doc.context(
                                        "mongo raw updateDescription cannot be parsed and fullDocument is missing",
                                    )?;
                                    log_warn!(
                                        "mongo updateDescription cannot be converted to Document, use raw full-document replacement, schema: {}, table: {}, error: {}",
                                        row_data.schema,
                                        row_data.tb,
                                        error
                                    );
                                    self.replace_raw_update(
                                        &raw_collection,
                                        row_data,
                                        document_key,
                                        pre_image,
                                        full_doc,
                                    )
                                    .await?;
                                    rts.push((start_time.elapsed().as_millis() as u64, 1));
                                    continue;
                                }
                            };

                            if changestream_parser::requires_full_document(&update_description) {
                                if let Some(full_doc) = raw_full_doc {
                                    self.replace_raw_update(
                                        &raw_collection,
                                        row_data,
                                        document_key,
                                        pre_image,
                                        full_doc,
                                    )
                                    .await?;
                                    rts.push((start_time.elapsed().as_millis() as u64, 1));
                                } else {
                                    log_error!(
                                        "mongo updateDescription has ambiguous disambiguatedPaths, but fullDocument is missing, ignore, schema: {}, table: {}",
                                        row_data.schema,
                                        row_data.tb
                                    );
                                }
                                continue;
                            }

                            let needs_full_document = update_description
                                .get_array("truncatedArrays")
                                .map(|arrays| !arrays.is_empty())
                                .unwrap_or(false);
                            let parsed_full_doc = if needs_full_document {
                                match raw_full_doc.map(RawDocumentBuf::to_document).transpose() {
                                    Ok(full_doc) => full_doc,
                                    Err(error) => {
                                        let full_doc = raw_full_doc.expect(
                                            "raw fullDocument exists when its conversion fails",
                                        );
                                        log_warn!(
                                            "mongo fullDocument cannot be converted for truncatedArrays, use raw full-document replacement, schema: {}, table: {}, error: {}",
                                            row_data.schema,
                                            row_data.tb,
                                            error
                                        );
                                        self.replace_raw_update(
                                            &raw_collection,
                                            row_data,
                                            document_key,
                                            pre_image,
                                            full_doc,
                                        )
                                        .await?;
                                        rts.push((start_time.elapsed().as_millis() as u64, 1));
                                        continue;
                                    }
                                }
                            } else {
                                None
                            };

                            let update_doc = changestream_parser::build_update_doc(
                                &update_description,
                                parsed_full_doc.as_ref(),
                            );
                            if update_doc.is_empty() {
                                if let Some(full_doc) = raw_full_doc {
                                    log_warn!(
                                        "mongo updateDescription is empty or unsupported, use raw full-document replacement, schema: {}, table: {}",
                                        row_data.schema,
                                        row_data.tb
                                    );
                                    self.replace_raw_update(
                                        &raw_collection,
                                        row_data,
                                        document_key,
                                        pre_image,
                                        full_doc,
                                    )
                                    .await?;
                                    rts.push((start_time.elapsed().as_millis() as u64, 1));
                                } else {
                                    log_error!(
                                        "mongo updateDescription is empty or unsupported and fullDocument is missing, ignore, schema: {}, table: {}",
                                        row_data.schema,
                                        row_data.tb
                                    );
                                }
                                continue;
                            }

                            if self
                                .raw_shard_key_changed(
                                    row_data,
                                    document_key,
                                    pre_image,
                                    raw_full_doc,
                                )
                                .await?
                            {
                                let full_doc = raw_full_doc.context(
                                    "mongo shard key update requires full document after image",
                                )?;
                                self.replace_raw_update(
                                    &raw_collection,
                                    row_data,
                                    document_key,
                                    pre_image,
                                    full_doc,
                                )
                                .await?;
                                rts.push((start_time.elapsed().as_millis() as u64, 1));
                                continue;
                            }

                            let query_doc = if let Some(pre_image) = pre_image {
                                Some(self.complete_raw_shard_filter(row_data, pre_image).await?)
                            } else if document_key.is_some() {
                                Some(
                                    self.complete_shard_filter_with_raw_doc(
                                        row_data,
                                        document_key,
                                        raw_full_doc,
                                        false,
                                    )
                                    .await?,
                                )
                            } else {
                                None
                            };

                            if let Some(query_doc) = query_doc {
                                if let Some(full_doc) = raw_full_doc {
                                    self.update_existing_with_raw_fallback(
                                        row_data,
                                        query_doc,
                                        update_doc,
                                        document_key,
                                        full_doc,
                                    )
                                    .await?;
                                } else {
                                    self.upsert(&collection, query_doc, update_doc).await?;
                                }
                                rts.push((start_time.elapsed().as_millis() as u64, 1));
                            }
                            continue;
                        }

                        if let Some(full_doc) = raw_full_doc {
                            self.replace_raw_update(
                                &raw_collection,
                                row_data,
                                document_key,
                                pre_image,
                                full_doc,
                            )
                            .await?;
                            rts.push((start_time.elapsed().as_millis() as u64, 1));
                            continue;
                        }

                        anyhow::bail!(
                            "mongo raw update row missing both updateDescription and fullDocument"
                        );
                    }

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

                    if self
                        .shard_key_changed(
                            row_data,
                            old_shard_doc,
                            pre_image.is_some(),
                            after_full_doc,
                        )
                        .await?
                    {
                        let old_filter = if let Some(pre_image) = pre_image {
                            self.complete_shard_filter_prefer_full_doc(
                                row_data,
                                document_key,
                                Some(pre_image),
                            )
                            .await?
                        } else {
                            self.complete_shard_filter(row_data, document_key.or(before_doc), None)
                                .await?
                        };
                        let new_doc = after_full_doc
                            .context("mongo shard key update requires full document after image")?;
                        let new_filter = self
                            .complete_shard_filter(row_data, None, Some(new_doc))
                            .await?;
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

                    let query_doc = {
                        if let Some(pre_image) = pre_image {
                            Some(
                                self.complete_shard_filter_prefer_full_doc(
                                    row_data,
                                    document_key,
                                    Some(pre_image),
                                )
                                .await?,
                            )
                        } else if let Some(doc) = before_doc {
                            Some(
                                self.complete_shard_filter(
                                    row_data,
                                    document_key.or(Some(doc)),
                                    row_data.after.as_ref().and_then(|after| {
                                        Self::mongo_doc(after, MongoConstants::DOC)
                                    }),
                                )
                                .await?,
                            )
                        } else if let Some(document_key) = document_key {
                            Some(
                                self.complete_shard_filter(
                                    row_data,
                                    Some(document_key),
                                    row_data.after.as_ref().and_then(|after| {
                                        Self::mongo_doc(after, MongoConstants::DOC)
                                    }),
                                )
                                .await?,
                            )
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

        for row_data in data.iter().skip(start_index).take(batch_size) {
            if self.is_target_sharded(row_data).await? {
                return self
                    .serial_sink(&data[start_index..start_index + batch_size])
                    .await;
            }
        }

        let mut ids: Vec<Bson> = Vec::new();
        for rd in data.iter().skip(start_index).take(batch_size) {
            data_size += rd.get_data_size() as usize;

            let before = rd.require_before()?;
            if let Some(ColValue::MongoDoc(doc)) = before.get(MongoConstants::DOC) {
                let id = doc
                    .get(MongoConstants::ID)
                    .context("mongo doc missing `_id`")?;
                ids.push(id.clone());
            } else if let Some(ColValue::MongoRawDoc(doc)) = before.get(MongoConstants::DOC) {
                let id = Self::raw_value(doc, MongoConstants::ID)?
                    .context("mongo raw doc missing `_id`")?;
                ids.push(id);
            } else {
                anyhow::bail!("mongo delete row missing document");
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
        let mut docs = Vec::new();
        let mut raw_docs = Vec::new();
        for rd in data.iter().skip(start_index).take(batch_size) {
            data_size += rd.get_data_size() as usize;

            let after = rd.require_after()?;
            if let Some(ColValue::MongoDoc(doc)) = after.get(MongoConstants::DOC) {
                docs.push(doc);
            } else if let Some(ColValue::MongoRawDoc(doc)) = after.get(MongoConstants::DOC) {
                raw_docs.push(doc);
            }
        }

        let insert_result = if !raw_docs.is_empty() {
            if !docs.is_empty() || raw_docs.len() != batch_size {
                anyhow::bail!("mongo insert batch contains mixed or missing document values");
            }
            self.mongo_client
                .database(db)
                .collection::<RawDocumentBuf>(tb)
                .insert_many(raw_docs)
                .await
        } else {
            if docs.len() != batch_size {
                anyhow::bail!("mongo insert batch contains missing document values");
            }
            self.mongo_client
                .database(db)
                .collection::<Document>(tb)
                .insert_many(docs)
                .await
        };

        if let Err(error) = insert_result {
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

        if self.is_target_sharded(row_data).await? {
            if let Some(id_filter) = Self::id_filter(document_key, Some(full_doc)) {
                if let Some(target_doc) = collection.find_one(id_filter).await? {
                    let retry_filter = self
                        .complete_shard_filter_prefer_full_doc(
                            row_data,
                            document_key,
                            Some(&target_doc),
                        )
                        .await?;
                    if self
                        .update_existing(collection, retry_filter, update_doc.clone())
                        .await?
                    {
                        return Ok(());
                    }
                }
            }

            let new_filter = self
                .complete_shard_filter(row_data, None, Some(full_doc))
                .await?;
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

    async fn update_existing_with_raw_fallback(
        &mut self,
        row_data: &RowData,
        query_doc: Document,
        update_doc: Document,
        document_key: Option<&Document>,
        full_doc: &RawDocumentBuf,
    ) -> anyhow::Result<()> {
        let database = self.mongo_client.database(&row_data.schema);
        let collection = database.collection::<Document>(&row_data.tb);
        let raw_collection = database.collection::<RawDocumentBuf>(&row_data.tb);
        if self
            .update_existing(&collection, query_doc, update_doc.clone())
            .await?
        {
            return Ok(());
        }

        if self.is_target_sharded(row_data).await? {
            if let Some(id_filter) = Self::raw_id_filter(document_key, Some(full_doc))? {
                if let Some(target_doc) = raw_collection.find_one(id_filter).await? {
                    let retry_filter = self
                        .complete_raw_shard_filter(row_data, &target_doc)
                        .await?;
                    if self
                        .update_existing(&collection, retry_filter, update_doc.clone())
                        .await?
                    {
                        return Ok(());
                    }
                }
            }

            let new_filter = self.complete_raw_shard_filter(row_data, full_doc).await?;
            if self
                .update_existing(&collection, new_filter, update_doc)
                .await?
            {
                return Ok(());
            }

            anyhow::bail!(
                "mongo update matched no target document for sharded collection [{}]",
                self.target_ns(row_data)
            );
        }

        if let Some(id_filter) = Self::raw_id_filter(document_key, Some(full_doc))? {
            self.replace_raw(&raw_collection, id_filter, full_doc)
                .await?;
        }
        Ok(())
    }

    async fn replace_raw_update(
        &mut self,
        raw_collection: &Collection<RawDocumentBuf>,
        row_data: &RowData,
        document_key: Option<&Document>,
        pre_image: Option<&RawDocumentBuf>,
        full_doc: &RawDocumentBuf,
    ) -> anyhow::Result<()> {
        let old_filter = if let Some(pre_image) = pre_image {
            self.complete_raw_shard_filter(row_data, pre_image).await?
        } else {
            self.complete_shard_filter_with_raw_doc(row_data, document_key, Some(full_doc), false)
                .await?
        };

        if self
            .replace_raw_existing(raw_collection, old_filter, full_doc)
            .await?
        {
            return Ok(());
        }

        if self.is_target_sharded(row_data).await? {
            if let Some(id_filter) = Self::raw_id_filter(document_key, Some(full_doc))? {
                if let Some(target_doc) = raw_collection.find_one(id_filter).await? {
                    let retry_filter = self
                        .complete_raw_shard_filter(row_data, &target_doc)
                        .await?;
                    if self
                        .replace_raw_existing(raw_collection, retry_filter, full_doc)
                        .await?
                    {
                        return Ok(());
                    }
                }
            }
        }

        let new_filter = self.complete_raw_shard_filter(row_data, full_doc).await?;
        self.replace_raw(raw_collection, new_filter, full_doc).await
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

    async fn replace_raw(
        &mut self,
        collection: &Collection<RawDocumentBuf>,
        query_doc: Document,
        replacement_doc: &RawDocumentBuf,
    ) -> anyhow::Result<()> {
        collection
            .replace_one(query_doc, replacement_doc)
            .upsert(true)
            .await?;
        Ok(())
    }

    async fn replace_raw_existing(
        &mut self,
        collection: &Collection<RawDocumentBuf>,
        query_doc: Document,
        replacement_doc: &RawDocumentBuf,
    ) -> anyhow::Result<bool> {
        let result = collection.replace_one(query_doc, replacement_doc).await?;
        Ok(result.matched_count > 0)
    }
}

#[cfg(test)]
mod tests {
    use mongodb::bson::{doc, raw::RawDocumentBuf};

    use super::*;

    fn raw_doc_with_invalid_utf8() -> RawDocumentBuf {
        let mut bytes = RawDocumentBuf::from_document(&doc! {
            MongoConstants::ID: 1,
            "invalid": "ok",
        })
        .unwrap()
        .into_bytes();
        let value_offset = bytes
            .windows(3)
            .position(|window| window == b"ok\0")
            .unwrap();
        bytes[value_offset] = 0xff;
        RawDocumentBuf::from_bytes(bytes).unwrap()
    }

    #[test]
    fn checker_conversion_rejects_invalid_utf8_without_changing_raw_doc() {
        let raw_doc = raw_doc_with_invalid_utf8();
        let mut fields = HashMap::from([(
            MongoConstants::DOC.to_string(),
            ColValue::MongoRawDoc(raw_doc.clone()),
        )]);

        assert!(MongoSinker::convert_raw_doc_for_check(&mut fields).is_err());
        assert_eq!(
            fields.get(MongoConstants::DOC),
            Some(&ColValue::MongoRawDoc(raw_doc))
        );
    }

    #[test]
    fn checker_conversion_turns_valid_raw_doc_into_document() {
        let raw_doc = RawDocumentBuf::from_document(&doc! {
            MongoConstants::ID: 1,
            "value": "valid",
        })
        .unwrap();
        let mut fields = HashMap::from([(
            MongoConstants::DOC.to_string(),
            ColValue::MongoRawDoc(raw_doc),
        )]);

        MongoSinker::convert_raw_doc_for_check(&mut fields).unwrap();
        assert!(matches!(
            fields.get(MongoConstants::DOC),
            Some(ColValue::MongoDoc(_))
        ));
    }
}
