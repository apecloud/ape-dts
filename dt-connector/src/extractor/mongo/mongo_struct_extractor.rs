use std::collections::HashSet;

use async_trait::async_trait;
use mongodb::Client;

use crate::{
    extractor::base_extractor::{BaseExtractor, ExtractState},
    meta_fetcher::mongo::mongo_struct_fetcher::MongoStructFetcher,
    Extractor,
};
use dt_common::{
    config::task_config::DEFAULT_DB_BATCH_SIZE,
    log_info, log_warn,
    meta::struct_meta::{
        statement::struct_statement::StructStatement, struct_data::StructData,
        structure::structure_type::StructureType,
    },
    rdb_filter::RdbFilter,
};

pub struct MongoStructExtractor {
    pub base_extractor: BaseExtractor,
    pub extract_state: ExtractState,
    pub mongo_client: Client,
    pub dbs: Vec<String>,
    pub filter: RdbFilter,
    pub db_batch_size: usize,
}

#[async_trait]
impl Extractor for MongoStructExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        log_info!("MongoStructExtractor starts...");
        let db_chunks: Vec<Vec<String>> = self
            .dbs
            .chunks(self.db_batch_size)
            .map(|chunk| chunk.to_vec())
            .collect();
        for db_chunk in db_chunks.into_iter() {
            log_info!("MongoStructExtractor extracts dbs: {}", db_chunk.join(","));
            self.extract_internal(db_chunk.into_iter().collect())
                .await?;
        }
        self.base_extractor
            .wait_task_finish(&mut self.extract_state)
            .await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl MongoStructExtractor {
    pub async fn extract_internal(&mut self, dbs: HashSet<String>) -> anyhow::Result<()> {
        let fetcher = MongoStructFetcher {
            mongo_client: self.mongo_client.clone(),
            dbs,
            filter: self.filter.clone(),
        };

        if !self.filter.filter_structure(&StructureType::Collection) {
            for statement in fetcher.get_create_collection_statements().await? {
                self.push_dt_data(StructStatement::MongoCreateCollection(statement))
                    .await?;
            }
        }

        if !self.filter.filter_structure(&StructureType::ShardKey) {
            for statement in fetcher.get_shard_key_statements().await? {
                self.push_dt_data(StructStatement::MongoShardKey(statement))
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn push_dt_data(&mut self, statement: StructStatement) -> anyhow::Result<()> {
        let struct_data = StructData {
            schema: String::new(),
            statement,
        };
        self.base_extractor
            .push_struct(&mut self.extract_state, struct_data)
            .await
    }

    pub fn validate_db_batch_size(db_batch_size: usize) -> anyhow::Result<usize> {
        if db_batch_size < 1 || db_batch_size > 1000 {
            log_warn!(
                "db_batch_size {} is not valid, using default value: {}",
                db_batch_size,
                DEFAULT_DB_BATCH_SIZE
            );
            Ok(DEFAULT_DB_BATCH_SIZE)
        } else {
            Ok(db_batch_size)
        }
    }
}
