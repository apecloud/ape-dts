use std::collections::{BTreeSet, HashSet};

use async_trait::async_trait;
use dt_common::{
    config::task_config::DEFAULT_DB_BATCH_SIZE,
    log_info, log_warn,
    meta::{
        dt_data::DtData,
        mssql::mssql_connection_pool::MssqlConnectionPool,
        position::Position,
        struct_meta::{
            statement::{
                mssql_create_schema_statement::MssqlCreateSchemaStatement,
                struct_statement::StructStatement,
            },
            struct_data::StructData,
            structure::{schema::Schema, table::Table},
        },
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

        let statements = fetcher.get_create_table_statements("", "").await?;
        let target_schemas = statements
            .iter()
            .map(|statement| self.route_table(&statement.table))
            .map(|(db, schema, _)| (db.to_string(), schema.to_string()))
            .collect::<BTreeSet<_>>();

        // These namespaces are already routed from their owning tables, so bypass routing here.
        for (db, schema) in target_schemas {
            self.push_routed_schema(db, schema).await?;
        }
        for statement in statements {
            let db = statement.table.database_name.clone();
            let schema = statement.table.schema_name.clone();
            let tb = statement.table.table_name.clone();
            self.push_dt_data(db, schema, tb, StructStatement::MssqlCreateTable(statement))
                .await?;
        }
        Ok(())
    }

    fn route_table<'a>(&'a self, table: &'a Table) -> (&'a str, &'a str, &'a str) {
        self.base_extractor.router.as_ref().map_or(
            (
                table.database_name.as_str(),
                table.schema_name.as_str(),
                table.table_name.as_str(),
            ),
            |router| {
                router.get_tb_map_with_db(
                    &table.database_name,
                    &table.schema_name,
                    &table.table_name,
                )
            },
        )
    }

    async fn push_routed_schema(&mut self, db: String, schema: String) -> anyhow::Result<()> {
        let statement = MssqlCreateSchemaStatement {
            database_name: db.clone(),
            schema: Schema {
                name: schema.clone(),
            },
        };
        self.base_extractor
            .push_dt_data(
                &mut self.extract_state,
                DtData::Struct {
                    struct_data: StructData {
                        db,
                        schema,
                        tb: String::new(),
                        statement: StructStatement::MssqlCreateSchema(statement),
                    },
                },
                Position::None,
            )
            .await
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
