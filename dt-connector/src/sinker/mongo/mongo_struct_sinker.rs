use std::{cmp, collections::HashMap};

use anyhow::Context;
use async_trait::async_trait;
use mongodb::{bson::doc, Client};
use tokio::time::Instant;

use crate::{sinker::base_sinker::BaseSinker, Sinker};
use dt_common::{
    config::config_enums::ConflictPolicyEnum,
    error::Error,
    log_error, log_info,
    meta::{
        mongo::mongo_shard::{list_shard_collections, MongoShardCollection},
        struct_meta::{
            statement::{
                mongo_create_collection_statement::MongoCreateCollectionStatement,
                mongo_shard_key_statement::MongoShardKeyStatement,
                struct_statement::StructStatement,
            },
            struct_data::StructData,
            structure::structure_type::StructureType,
        },
    },
    rdb_filter::RdbFilter,
    utils::limit_queue::LimitedQueue,
};

#[derive(Clone)]
pub struct MongoStructSinker {
    pub mongo_client: Client,
    pub conflict_policy: ConflictPolicyEnum,
    pub filter: RdbFilter,
    pub base_sinker: BaseSinker,
    pub target_shard_collections: HashMap<String, MongoShardCollection>,
    pub is_target_mongos: bool,
}

#[async_trait]
impl Sinker for MongoStructSinker {
    async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        let monitor_interval_secs = self.base_sinker.monitor_interval_secs();
        let mut rts = LimitedQueue::new(cmp::min(100, data.len()));
        let mut last_monitor_time = Instant::now();
        let mut data_len = 0;

        for struct_data in data {
            data_len += 1;
            let start_time = Instant::now();
            let result = self.sink_one(struct_data).await;
            match result {
                Ok(()) => {}
                Err(error) => {
                    log_error!("mongo struct failed, error: {}", error);
                    match self.conflict_policy {
                        ConflictPolicyEnum::Interrupt => return Err(error),
                        ConflictPolicyEnum::Ignore => {}
                    }
                }
            }

            rts.push((start_time.elapsed().as_millis() as u64, 1));
            if last_monitor_time.elapsed().as_secs() >= monitor_interval_secs {
                self.base_sinker
                    .update_serial_monitor(data_len as u64, 0)
                    .await?;
                self.base_sinker.update_monitor_rt(&rts).await?;
                rts.clear();
                data_len = 0;
                last_monitor_time = Instant::now();
            }
        }

        if data_len > 0 {
            self.base_sinker
                .update_serial_monitor(data_len as u64, 0)
                .await?;
            self.base_sinker.update_monitor_rt(&rts).await?;
        }

        (_, self.target_shard_collections) = list_shard_collections(&self.mongo_client).await?;
        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl MongoStructSinker {
    async fn sink_one(&mut self, struct_data: StructData) -> anyhow::Result<()> {
        match struct_data.statement {
            StructStatement::MongoCreateCollection(statement) => {
                self.sink_collection(statement).await
            }
            StructStatement::MongoShardKey(statement) => self.sink_shard_key(statement).await,
            _ => Ok(()),
        }
    }

    async fn sink_collection(
        &mut self,
        statement: MongoCreateCollectionStatement,
    ) -> anyhow::Result<()> {
        if self.filter.filter_structure(&StructureType::Collection) {
            return Ok(());
        }

        self.create_collection(&statement).await?;
        self.create_indexes(&statement).await?;
        Ok(())
    }

    async fn create_collection(
        &self,
        statement: &MongoCreateCollectionStatement,
    ) -> anyhow::Result<()> {
        let database = self.mongo_client.database(&statement.database_name);
        let mut command = doc! { "create": statement.collection_name.clone() };
        for (key, value) in &statement.options {
            command.insert(key, value.clone());
        }
        log_info!(
            "mongo create collection begin: {}.{}",
            statement.database_name,
            statement.collection_name
        );
        database
            .run_command(command)
            .await
            .map_err(Error::MongodbError)?;
        log_info!("mongo create collection succeed");
        Ok(())
    }

    async fn create_indexes(
        &self,
        statement: &MongoCreateCollectionStatement,
    ) -> anyhow::Result<()> {
        if statement.indexes.is_empty() {
            return Ok(());
        }

        let command = doc! {
            "createIndexes": statement.collection_name.clone(),
            "indexes": statement.indexes.clone(),
        };
        log_info!(
            "mongo create indexes begin: {}.{}",
            statement.database_name,
            statement.collection_name
        );
        self.mongo_client
            .database(&statement.database_name)
            .run_command(command)
            .await
            .map_err(Error::MongodbError)?;
        log_info!("mongo create indexes succeed");
        Ok(())
    }

    async fn sink_shard_key(&self, statement: MongoShardKeyStatement) -> anyhow::Result<()> {
        if self.filter.filter_structure(&StructureType::ShardKey) {
            return Ok(());
        }

        let shard_collection = &statement.shard_collection;
        if !self.is_target_mongos {
            log_info!(
                "mongo target is not mongos, skip shard key for {}",
                shard_collection.ns
            );
            return Ok(());
        }

        if let Some(existing) = self.target_shard_collections.get(&shard_collection.ns) {
            if existing.key != shard_collection.key || existing.unique != shard_collection.unique {
                anyhow::bail!(
                    "mongo target collection [{}] shard key mismatch, source key: {:?}, source unique: {}, target key: {:?}, target unique: {}",
                    shard_collection.ns,
                    shard_collection.key,
                    shard_collection.unique,
                    existing.key,
                    existing.unique,
                );
            }
            return Ok(());
        }

        let (db, _) = shard_collection
            .ns
            .split_once('.')
            .context("mongo shard collection namespace missing db")?;
        self.mongo_client
            .database("admin")
            .run_command(doc! { "enableSharding": db })
            .await
            .map_err(Error::MongodbError)?;

        let command = doc! {
            "shardCollection": shard_collection.ns.clone(),
            "key": shard_collection.key.clone(),
            "unique": shard_collection.unique,
        };
        log_info!("mongo shard collection begin: {}", shard_collection.ns);
        self.mongo_client
            .database("admin")
            .run_command(command)
            .await
            .map_err(Error::MongodbError)?;
        log_info!("mongo shard collection succeed");
        Ok(())
    }
}
