use async_trait::async_trait;
use dt_common::meta::struct_meta::struct_data::StructData;
use dt_common::{log_info, rdb_filter::RdbFilter};

use dt_common::meta::{
    mysql::mysql_meta_manager::MysqlMetaManager,
    struct_meta::statement::struct_statement::StructStatement,
};
use sqlx::{MySql, Pool};

use crate::close_conn_pool;
use crate::{
    extractor::base_extractor::BaseExtractor,
    meta_fetcher::mysql::mysql_struct_fetcher::MysqlStructFetcher, Extractor,
};

pub struct MysqlStructExtractor {
    pub base_extractor: BaseExtractor,
    pub conn_pool: Pool<MySql>,
    pub db: String,
    pub dbs: Vec<String>,
    pub filter: RdbFilter,
}

#[async_trait]
impl Extractor for MysqlStructExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        log_info!(
            "MysqlStructExtractor starts, schemas: {}",
            self.dbs.join(",")
        );
        for db in self.dbs.clone().iter() {
            self.extract_internal(db).await?;
        }
        self.base_extractor.wait_task_finish().await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        close_conn_pool!(self)
    }
}

impl MysqlStructExtractor {
    pub async fn extract_internal(&mut self, db: &String) -> anyhow::Result<()> {
        let meta_manager = MysqlMetaManager::new(self.conn_pool.clone()).await?;
        let mut fetcher = MysqlStructFetcher {
            conn_pool: self.conn_pool.to_owned(),
            db: db.clone(),
            filter: Some(self.filter.to_owned()),
            meta_manager,
        };

        // database
        let database_statement = fetcher.get_create_database_statement().await?;
        self.push_dt_data(StructStatement::MysqlCreateDatabase(database_statement))
            .await?;

        // tables
        for table_statement in fetcher.get_create_table_statements("").await? {
            self.push_dt_data(StructStatement::MysqlCreateTable(table_statement))
                .await?;
        }
        Ok(())
    }

    pub async fn push_dt_data(&mut self, statement: StructStatement) -> anyhow::Result<()> {
        let struct_data = StructData {
            schema: self.db.clone(),
            statement,
        };
        self.base_extractor.push_struct(struct_data).await
    }
}
