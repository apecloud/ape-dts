use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use mongodb::{
    bson::{doc, Document},
    options::FindOptions,
    Client,
};

use crate::{
    extractor::{
        base_extractor::{BaseExtractor, ExtractState},
        estimated_sample_limit,
        resumer::recovery::Recovery,
        snapshot_chunk_id_generator::SnapshotChunkIdGenerator,
        snapshot_dispatcher::SnapshotDispatcher,
    },
    Extractor,
};
use dt_common::{
    config::config_enums::{DbType, RdbParallelType},
    log_error, log_info,
    meta::{
        col_value::ColValue,
        ddl_meta::ddl_type::DdlType,
        mongo::{
            mongo_constant::MongoConstants, mongo_ddl::build_shard_collection_ddl,
            mongo_key::MongoKey, mongo_shard::list_shard_collections,
        },
        order_key::OrderKey,
        position::Position,
        row_data::RowData,
        row_type::RowType,
    },
    rdb_filter::RdbFilter,
};

pub struct MongoSnapshotExtractor {
    pub base_extractor: BaseExtractor,
    pub extract_state: ExtractState,
    pub db_tbs: HashMap<String, Vec<String>>,
    pub parallel_type: RdbParallelType,
    pub parallel_size: usize,
    pub batch_size: u32,
    pub mongo_client: Client,
    pub sample_rate: Option<u8>,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
    pub filter: RdbFilter,
}

#[async_trait]
impl Extractor for MongoSnapshotExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        if self.parallel_size < 1 {
            bail!("parallel_size must be greater than 0");
        }
        if matches!(self.parallel_type, RdbParallelType::Chunk) {
            bail!("mongo snapshot extractor does not support parallel_type=chunk");
        }

        self.prepare_mongo_sharding().await?;

        let tables = self.collect_tables();
        let this = self.clone_for_dispatch();
        SnapshotDispatcher::dispatch_table_work_source(
            tables,
            self.parallel_size,
            "mongo table worker",
            move |(db, tb)| {
                let this = this.clone_for_dispatch();
                async move { this.run_table_worker(db, tb).await }
            },
        )
        .await?;

        self.base_extractor
            .wait_task_finish(&mut self.extract_state)
            .await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl MongoSnapshotExtractor {
    fn collect_tables(&self) -> Vec<(String, String)> {
        let mut tables = Vec::new();
        for (db, tbs) in &self.db_tbs {
            for tb in tbs {
                tables.push((db.clone(), tb.clone()));
            }
        }
        tables
    }

    fn clone_for_dispatch(&self) -> Self {
        Self {
            base_extractor: self.base_extractor.clone(),
            extract_state: SnapshotDispatcher::fork_extract_state(&self.extract_state),
            db_tbs: self.db_tbs.clone(),
            parallel_type: self.parallel_type.clone(),
            parallel_size: self.parallel_size,
            batch_size: self.batch_size,
            mongo_client: self.mongo_client.clone(),
            sample_rate: self.sample_rate,
            recovery: self.recovery.clone(),
            filter: self.filter.clone(),
        }
    }

    async fn prepare_mongo_sharding(&mut self) -> anyhow::Result<()> {
        let (_, source_shard_collections) = list_shard_collections(&self.mongo_client).await?;
        if source_shard_collections.is_empty() {
            return Ok(());
        }

        for source_shard_collection in source_shard_collections.values() {
            let Some((db, tb)) = source_shard_collection.ns.split_once('.') else {
                continue;
            };
            if !self.db_tbs.get(db).is_some_and(|selected_tbs| {
                selected_tbs.iter().any(|selected_tb| selected_tb == tb)
            }) {
                continue;
            }
            if self
                .filter
                .filter_ddl(db, tb, &DdlType::MongoShardCollection)
            {
                continue;
            }

            let Some(ddl_data) = build_shard_collection_ddl(
                &source_shard_collection.ns,
                source_shard_collection.key.clone(),
                source_shard_collection.unique,
            ) else {
                continue;
            };
            self.base_extractor
                .push_ddl(&mut self.extract_state, ddl_data, Position::None)
                .await?;
        }
        Ok(())
    }

    async fn run_table_worker(&self, db: String, tb: String) -> anyhow::Result<()> {
        let (mut extract_state, _guard) =
            SnapshotDispatcher::fork_table_extract_state(&self.extract_state, &db, &tb).await;
        let base_extractor = self.base_extractor.clone();

        log_info!(
            "MongoSnapshotExtractor starts, schema: {}, tb: {}, batch_size: {}",
            db,
            tb,
            self.batch_size
        );

        let resume_key = if let Some(handler) = &self.recovery {
            if let Some(Position::RdbSnapshot {
                order_key: Some(OrderKey::Single((_, Some(value)))),
                ..
            }) = handler.get_snapshot_resume_position(&db, &tb, false).await
            {
                let key = Self::parse_resume_key(&value)?;
                log_info!(
                    "[{}.{}] recovery from [{}]:[{}]",
                    db,
                    tb,
                    MongoConstants::ID,
                    key
                );
                Some(key)
            } else {
                None
            }
        } else {
            None
        };

        let collection = self.mongo_client.database(&db).collection::<Document>(&tb);
        let estimated_count = if self
            .sample_rate
            .filter(|rate| (1..100).contains(rate))
            .is_some()
        {
            collection.estimated_document_count().await?
        } else {
            0
        };
        let sample_limit = estimated_sample_limit(self.sample_rate, estimated_count);
        let mut find_options = FindOptions::builder()
            .sort(doc! {MongoConstants::ID: 1})
            .batch_size(self.batch_size)
            .build();
        if let Some(limit) = sample_limit.and_then(|limit| i64::try_from(limit).ok()) {
            find_options.limit = Some(limit);
        }
        let filter = resume_key
            .as_ref()
            .map(Self::build_resume_filter)
            .unwrap_or_default();
        let mut find = collection
            .find(filter)
            .sort(doc! {MongoConstants::ID: 1})
            .batch_size(self.batch_size);
        if let Some(limit) = find_options.limit {
            find = find.limit(limit);
        }
        let mut cursor = find.await?;
        let mut chunk_id_generator = SnapshotChunkIdGenerator::new(self.batch_size as usize);
        while cursor.advance().await? {
            let doc = cursor.deserialize_current().map_err(|e| {
                log_error!("error deserializing {}.{} document: {}", db, tb, e);
                e
            })?;

            let key = MongoKey::from_doc(&doc).ok_or(anyhow!(
                "skip {}.{} document without `_id`: {:?}",
                db,
                tb,
                doc
            ))?;

            let after = Self::build_after_cols(&doc);
            let row_data = RowData::new(
                db.clone(),
                tb.clone(),
                chunk_id_generator.next_row_chunk_id(),
                RowType::Insert,
                None,
                Some(after),
            );
            let position = Position::RdbSnapshot {
                db_type: DbType::Mongo.to_string(),
                schema: db.clone(),
                tb: tb.clone(),
                order_key: Some(OrderKey::Single((
                    MongoConstants::ID.into(),
                    Some(key.to_string()),
                ))),
            };

            base_extractor
                .push_row(&mut extract_state, row_data, position)
                .await?;
        }

        log_info!(
            "end extracting data from {}.{}, all count: {}",
            db,
            tb,
            extract_state.monitor.counters.pushed_record_count
        );
        // push schema and table info without routing.
        base_extractor
            .push_snapshot_finished(
                &mut extract_state,
                Position::RdbSnapshotFinished {
                    db_type: DbType::Mongo.to_string(),
                    schema: db.clone(),
                    tb: tb.clone(),
                },
            )
            .await?;
        extract_state.monitor.try_flush(true).await;
        Ok(())
    }

    fn build_resume_filter(key: &MongoKey) -> Document {
        // use $expr to order multiple types of _id.
        // for single type of _id, this has the same performance as filter like {"_id": {"$gt": key}}.
        // ref https://www.mongodb.com/docs/manual/reference/operator/query/expr/
        doc! {
            "$expr": {
                "$gt": [
                    format!("${}", MongoConstants::ID),
                    key.to_mongo_id(),
                ],
            },
        }
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

    fn parse_resume_key(value: &str) -> anyhow::Result<MongoKey> {
        serde_json::from_str::<MongoKey>(value).or_else(|_| {
            mongodb::bson::oid::ObjectId::parse_str(value)
                .map(MongoKey::ObjectId)
                .map_err(Into::into)
        })
    }
}
