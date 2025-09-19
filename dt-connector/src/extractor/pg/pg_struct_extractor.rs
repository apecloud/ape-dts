use async_trait::async_trait;
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
    pub schema: String,
    pub schemas: Vec<String>,
    pub do_global_structs: bool,
    pub filter: RdbFilter,
}

#[async_trait]
impl Extractor for PgStructExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        log_info!(
            "PgStructExtractor starts, schemas: {}",
            self.schemas.join(",")
        );
        self.extract_internal().await?;
        self.base_extractor.wait_task_finish().await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        close_conn_pool!(self)
    }
}

impl PgStructExtractor {
    pub async fn extract_internal(&mut self) -> anyhow::Result<()> {
        let mut pg_fetcher = PgStructFetcher {
            conn_pool: self.conn_pool.to_owned(),
            schema: self.schema.clone(),
            schemas: self.schemas.iter().cloned().collect(),
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

        if self.do_global_structs && !self.filter.filter_structure(&StructureType::Rbac) {
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
            schema: self.schema.clone(),
            statement,
        };
        self.base_extractor.push_struct(struct_data).await
    }
}
