use std::collections::HashSet;

use anyhow::bail;
use async_trait::async_trait;
use dt_common::error::Error;
use dt_common::meta::struct_meta::struct_data::StructData;
use dt_common::{log_info, rdb_filter::RdbFilter};

use dt_common::meta::struct_meta::statement::struct_statement::StructStatement;
use dt_common::meta::struct_meta::structure::structure_type::StructureType;

use sqlx::{Pool, Postgres};

use crate::close_conn_pool;
use crate::{
    extractor::base_extractor::BaseExtractor, meta_fetcher::pg::pg_struct_fetcher::PgStructFetcher,
    Extractor,
};

pub struct PgStructExtractor {
    pub base_extractor: BaseExtractor,
    pub conn_pool: Pool<Postgres>,
    pub schemas: Vec<String>,
    pub do_global_structs: bool,
    pub filter: RdbFilter,
    pub db_batch_size: usize,
}

#[async_trait]
impl Extractor for PgStructExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        log_info!("PgStructExtractor starts...");
        let schema_chunks: Vec<Vec<String>> = self
            .schemas
            .chunks(self.db_batch_size)
            .map(|chunk| chunk.to_vec())
            .collect();
        let do_global_structs = schema_chunks.len() - 1;
        for (flag, schema_chunk) in schema_chunks.into_iter().enumerate() {
            log_info!(
                "PgStructExtractor extracts schemas: {}",
                schema_chunk.join(",")
            );
            self.extract_internal(
                schema_chunk.into_iter().collect(),
                flag == do_global_structs,
            )
            .await?;
        }
        self.base_extractor.wait_task_finish().await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        close_conn_pool!(self)
    }
}

impl PgStructExtractor {
    pub async fn extract_internal(
        &mut self,
        schemas: HashSet<String>,
        do_global_structs: bool,
    ) -> anyhow::Result<()> {
        let mut pg_fetcher = PgStructFetcher {
            conn_pool: self.conn_pool.to_owned(),
            schemas: schemas,
            filter: Some(self.filter.to_owned()),
        };

        // schemas
        for schema_statement in pg_fetcher.get_create_schema_statements("").await? {
            self.push_dt_data(StructStatement::PgCreateSchema(schema_statement))
                .await?;
        }

        // tables
        for table_statement in pg_fetcher.get_create_table_statements("", "").await? {
            self.push_dt_data(StructStatement::PgCreateTable(table_statement))
                .await?;
        }

        if do_global_structs && !self.filter.filter_structure(&StructureType::Rbac) {
            // do rbac init
            let rbac_statements = pg_fetcher.get_create_rbac_statements().await?;
            for statement in rbac_statements {
                self.push_dt_data(StructStatement::PgCreateRbac(statement))
                    .await?;
            }
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

    pub fn validate_db_batch_size(db_batch_size: usize) -> anyhow::Result<()> {
        let max_db_batch_size = 1000;
        let min_db_batch_size = 1;
        if db_batch_size < min_db_batch_size || db_batch_size > max_db_batch_size {
            bail! {Error::ConfigError(format!(r#"db_batch_size {} is not valid, should be in range ({}, {})"#, db_batch_size, min_db_batch_size, max_db_batch_size))}
        } else {
            Ok(())
        }
    }
}
