use std::collections::HashSet;

use async_trait::async_trait;
use sqlx::{MySql, Pool};

use crate::close_conn_pool;
use crate::{
    extractor::base_extractor::BaseExtractor,
    meta_fetcher::mysql::mysql_struct_fetcher::MysqlStructFetcher, Extractor,
};
use dt_common::{
    config::task_config::DEFAULT_DB_BATCH_SIZE,
    log_info,
    meta::{
        mysql::mysql_meta_manager::MysqlMetaManager,
        struct_meta::{statement::struct_statement::StructStatement, struct_data::StructData},
    },
    rdb_filter::RdbFilter,
};

pub struct MysqlStructExtractor {
    pub base_extractor: BaseExtractor,
    pub conn_pool: Pool<MySql>,
    pub dbs: Vec<String>,
    pub filter: RdbFilter,
    pub db_batch_size: usize,
}

#[async_trait]
impl Extractor for MysqlStructExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        log_info!("MysqlStructExtractor starts...");
        let db_chunks: Vec<Vec<String>> = self
            .dbs
            .chunks(self.db_batch_size)
            .map(|chunk| chunk.to_vec())
            .collect();
        for db_chunk in db_chunks.into_iter() {
            log_info!("MysqlStructExtractor extracts dbs: {}", db_chunk.join(","));
            self.extract_internal(db_chunk.into_iter().collect())
                .await?;
        }
        self.base_extractor.wait_task_finish().await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        close_conn_pool!(self)
    }
}

impl MysqlStructExtractor {
    pub async fn extract_internal(&mut self, dbs: HashSet<String>) -> anyhow::Result<()> {
        let meta_manager = MysqlMetaManager::new(self.conn_pool.clone()).await?;
        let mut fetcher = MysqlStructFetcher {
            conn_pool: self.conn_pool.to_owned(),
            dbs: dbs,
            filter: Some(self.filter.to_owned()),
            meta_manager,
        };

        // database
        let database_statements = fetcher.get_create_database_statements("").await?;
        for database_statement in database_statements {
            self.push_dt_data(StructStatement::MysqlCreateDatabase(database_statement))
                .await?;
        }

        // tables
        for table_statement in fetcher.get_create_table_statements("", "").await? {
            self.push_dt_data(StructStatement::MysqlCreateTable(table_statement))
                .await?;
        }
        Ok(())
    }

    pub async fn push_dt_data(&mut self, statement: StructStatement) -> anyhow::Result<()> {
        let struct_data = StructData {
            schema: "".to_string(),
            statement,
        };
        self.base_extractor.push_struct(struct_data).await
    }

    pub fn validate_db_batch_size(db_batch_size: usize) -> anyhow::Result<usize> {
        if db_batch_size < 1 || db_batch_size > 1000 {
            Ok(DEFAULT_DB_BATCH_SIZE)
        } else {
            Ok(db_batch_size)
        }
    }
}
