use async_trait::async_trait;
use dt_common::{
    log_info,
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
}

#[async_trait]
impl Extractor for MssqlStructExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        log_info!("MssqlStructExtractor starts...");
        for db in self.dbs.clone() {
            log_info!("MssqlStructExtractor extracts database: {}", db);
            self.extract_internal(db).await?;
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
    pub async fn extract_internal(&mut self, db: String) -> anyhow::Result<()> {
        let mut fetcher = MssqlStructFetcher {
            connection_pool: self.connection_pool.clone(),
            db,
            filter: RdbStructFilter::for_source(self.filter.clone()),
            allow_missing_database: false,
        };

        for statement in fetcher.get_create_database_statements().await? {
            let db = statement.database_name.clone();
            self.push_dt_data(
                db,
                String::new(),
                String::new(),
                StructStatement::MssqlCreateDatabase(statement),
            )
            .await?;
        }
        for statement in fetcher.get_create_schema_statements("").await? {
            let db = statement.database_name.clone();
            let schema = statement.schema_name.clone();
            self.push_dt_data(
                db,
                schema,
                String::new(),
                StructStatement::MssqlCreateSchema(statement),
            )
            .await?;
        }
        for statement in fetcher.get_create_table_statements("", "").await? {
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
}
