use std::{collections::HashMap, sync::Arc};

use anyhow::bail;
use async_trait::async_trait;
use mongodb::{
    bson::{doc, oid::ObjectId, Bson, Document},
    options::FindOptions,
    Client,
};

use crate::{
    extractor::{
        base_extractor::{BaseExtractor, ExtractState},
        resumer::recovery::Recovery,
        snapshot_dispatcher::SnapshotDispatcher,
    },
    Extractor,
};
use dt_common::{
    config::config_enums::{DbType, RdbParallelType},
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
    pub extract_state: ExtractState,
    pub db_tbs: HashMap<String, Vec<String>>,
    pub parallel_type: RdbParallelType,
    pub parallel_size: usize,
    pub mongo_client: Client,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
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
            mongo_client: self.mongo_client.clone(),
            recovery: self.recovery.clone(),
        }
    }

    async fn run_table_worker(&self, db: String, tb: String) -> anyhow::Result<()> {
        let (mut extract_state, _guard) =
            SnapshotDispatcher::fork_table_extract_state(&self.extract_state, &db, &tb).await;
        let base_extractor = self.base_extractor.clone();

        log_info!("MongoSnapshotExtractor starts, schema: {}, tb: {}", db, tb);

        let filter = if let Some(handler) = &self.recovery {
            if let Some(Position::RdbSnapshot {
                order_key: Some(OrderKey::Single((_, Some(value)))),
                ..
            }) = handler.get_snapshot_resume_position(&db, &tb, false).await
            {
                let value = ObjectId::parse_str(&value)?;
                log_info!(
                    "[{}.{}] recovery from [{}]:[{}]",
                    db,
                    tb,
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

        let find_options = FindOptions::builder()
            .sort(doc! {MongoConstants::ID: 1})
            .build();

        let collection = self.mongo_client.database(&db).collection::<Document>(&tb);
        let mut cursor = collection.find(filter, find_options).await?;
        while cursor.advance().await? {
            let doc = cursor.deserialize_current().map_err(|e| {
                log_error!("error deserializing {}.{} document: {}", db, tb, e);
                e
            })?;
            let object_id = Self::get_object_id(&doc);

            let after = Self::build_after_cols(&doc);
            let row_data = RowData::new(
                db.clone(),
                tb.clone(),
                0,
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
                    Some(object_id),
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
        base_extractor.push_snapshot_finished(
            &db,
            &tb,
            Position::RdbSnapshotFinished {
                db_type: DbType::Mongo.to_string(),
                schema: db.clone(),
                tb: tb.clone(),
            },
        )?;
        extract_state.monitor.try_flush(true).await;
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
}
