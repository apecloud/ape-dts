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
    rdb_struct_filter::RdbStructFilter,
    Extractor,
};

pub struct MssqlStructExtractor {
    pub base_extractor: BaseExtractor,
    pub extract_state: ExtractState,
    pub connection_pool: MssqlConnectionPool,
    pub dbs: Vec<String>,
    pub filter: RdbFilter,
    pub db_batch_size: usize,
}

#[async_trait]
impl Extractor for MssqlStructExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        log_info!("MssqlStructExtractor starts...");
        let db_chunks: Vec<Vec<String>> = self
            .dbs
            .chunks(self.db_batch_size)
            .map(|chunk| chunk.to_vec())
            .collect();

        for db_chunk in db_chunks {
            log_info!(
                "MssqlStructExtractor extracts databases: {}",
                db_chunk.join(",")
            );
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

impl MssqlStructExtractor {
    pub async fn extract_internal(&mut self, dbs: HashSet<String>) -> anyhow::Result<()> {
        let mut fetcher = MssqlStructFetcher {
            connection_pool: self.connection_pool.clone(),
            dbs,
            filter: RdbStructFilter::for_source(self.filter.clone()),
            allow_missing_databases: false,
        };

        for statement in fetcher.get_create_database_statements("").await? {
            let db = statement.database.name.clone();
            self.push_dt_data(
                db,
                String::new(),
                String::new(),
                StructStatement::MssqlCreateDatabase(statement),
            )
            .await?;
        }
        for statement in fetcher.get_create_schema_statements("", "").await? {
            let db = statement.database_name.clone();
            let schema = statement.schema.name.clone();
            self.push_dt_data(
                db,
                schema,
                String::new(),
                StructStatement::MssqlCreateSchema(statement),
            )
            .await?;
        }
        for statement in fetcher.get_create_table_statements("", "", "").await? {
            let db = statement.table.database_name.clone();
            let schema = statement.table.schema_name.clone();
            let tb = statement.table.table_name.clone();
            self.push_dt_data(db, schema, tb, StructStatement::MssqlCreateTable(statement))
                .await?;
        }
        Ok(())
    }

    pub async fn push_dt_data(
        &mut self,
        db: String,
        schema: String,
        tb: String,
        statement: StructStatement,
    ) -> anyhow::Result<()> {
        self.base_extractor
            .push_struct(
                &mut self.extract_state,
                StructData {
                    db,
                    schema,
                    tb,
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
