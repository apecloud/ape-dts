use std::collections::HashSet;

use async_trait::async_trait;
use dt_common::{
    config::task_config::DEFAULT_DB_BATCH_SIZE,
    log_info, log_warn,
    meta::{
        mssql::mssql_connection_pool::MssqlConnectionPool,
        struct_meta::{statement::struct_statement::StructStatement, struct_data::StructData},
    },
    rdb_filter::RdbFilter,
};

use crate::{
    extractor::base_extractor::{BaseExtractor, ExtractState},
    meta_fetcher::mssql::mssql_struct_fetcher::MssqlStructFetcher,
    Extractor,
};

pub struct MssqlStructExtractor {
    pub base_extractor: BaseExtractor,
    pub extract_state: ExtractState,
    pub connection_pool: MssqlConnectionPool,
    pub schemas: Vec<String>,
    pub filter: RdbFilter,
    pub db_batch_size: usize,
}

#[async_trait]
impl Extractor for MssqlStructExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        log_info!("MssqlStructExtractor starts...");
        let schema_chunks: Vec<Vec<String>> = self
            .schemas
            .chunks(self.db_batch_size)
            .map(|chunk| chunk.to_vec())
            .collect();

        for schema_chunk in schema_chunks {
            log_info!(
                "MssqlStructExtractor extracts schemas: {}",
                schema_chunk.join(",")
            );
            self.extract_internal(schema_chunk.into_iter().collect())
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

impl MssqlStructExtractor {
    pub async fn extract_internal(&mut self, schemas: HashSet<String>) -> anyhow::Result<()> {
        let mut fetcher = MssqlStructFetcher {
            connection_pool: self.connection_pool.clone(),
            schemas,
            filter: Some(self.filter.clone()),
        };

        for statement in fetcher.get_create_schema_statements("").await? {
            self.push_dt_data(StructStatement::MssqlCreateSchema(statement))
                .await?;
        }
        for statement in fetcher.get_create_table_statements("", "").await? {
            self.push_dt_data(StructStatement::MssqlCreateTable(statement))
                .await?;
        }
        Ok(())
    }

    pub async fn push_dt_data(&mut self, statement: StructStatement) -> anyhow::Result<()> {
        self.base_extractor
            .push_struct(
                &mut self.extract_state,
                StructData {
                    schema: String::new(),
                    statement,
                },
            )
            .await
    }

    pub fn validate_db_batch_size(db_batch_size: usize) -> anyhow::Result<usize> {
        if !(1..=1000).contains(&db_batch_size) {
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
