use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{Pool, Postgres};
use std::collections::{HashMap, HashSet};

use crate::{
    meta_fetcher::pg::pg_struct_fetcher::PgStructFetcher,
    rdb_query_builder::RdbQueryBuilder,
    sinker::base_checker::{BaseChecker, CheckerBackend, CheckerCommon},
    Sinker,
};
use dt_common::meta::{
    pg::pg_meta_manager::PgMetaManager,
    row_data::RowData,
    struct_meta::{statement::struct_statement::StructStatement, struct_data::StructData},
};

#[derive(Clone)]
pub struct PgChecker {
    pub conn_pool: Pool<Postgres>,
    pub meta_manager: PgMetaManager,
    pub common: CheckerCommon,
}

#[async_trait]
impl Sinker for PgChecker {
    async fn sink_dml(&mut self, data: Vec<RowData>, batch: bool) -> anyhow::Result<()> {
        let mut backend = self.clone();
        BaseChecker::standard_sink_dml(&mut backend, &mut self.common, data, batch).await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        BaseChecker::standard_close(&mut self.common).await
    }

    async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        let mut backend = self.clone();
        BaseChecker::standard_sink_struct(&mut backend, &mut self.common, data).await
    }
}

#[async_trait]
impl CheckerBackend for PgChecker {
    async fn get_tb_meta_by_row(
        &mut self,
        row: &RowData,
    ) -> anyhow::Result<crate::sinker::base_checker::CheckerTbMeta> {
        let meta = self
            .meta_manager
            .get_tb_meta_by_row_data(row)
            .await?
            .clone();
        Ok(crate::sinker::base_checker::CheckerTbMeta::Pg(meta))
    }

    async fn fetch_single(
        &self,
        tb_meta: &crate::sinker::base_checker::CheckerTbMeta,
        row: &RowData,
    ) -> anyhow::Result<Option<RowData>> {
        match tb_meta {
            crate::sinker::base_checker::CheckerTbMeta::Pg(pg_meta) => {
                let qb = RdbQueryBuilder::new_for_pg(pg_meta, None);
                let q_info = qb.get_select_query(row)?;
                let query = qb.create_pg_query(&q_info)?;
                let mut rows = query.fetch(&self.conn_pool);
                if let Some(row) = rows.try_next().await? {
                    let row_data = RowData::from_pg_row(&row, pg_meta, &None);
                    Ok(Some(row_data))
                } else {
                    Ok(None)
                }
            }
            _ => anyhow::bail!("Expected Pg metadata for PgChecker"),
        }
    }

    async fn fetch_batch(
        &self,
        tb_meta: &crate::sinker::base_checker::CheckerTbMeta,
        _schema: &str,
        _tb: &str,
        data: &[RowData],
    ) -> anyhow::Result<HashMap<u128, RowData>> {
        match tb_meta {
            crate::sinker::base_checker::CheckerTbMeta::Pg(pg_meta) => {
                let qb = RdbQueryBuilder::new_for_pg(pg_meta, None);
                let query_info = qb.get_batch_select_query(data, 0, data.len())?;
                let query = qb.create_pg_query(&query_info)?;
                let mut rows = query.fetch(&self.conn_pool);

                let mut map = HashMap::new();
                while let Some(row) = rows.try_next().await? {
                    let row_data = RowData::from_pg_row(&row, pg_meta, &None);
                    let hash_code = row_data.get_hash_code(&pg_meta.basic)?;
                    map.insert(hash_code, row_data);
                }
                Ok(map)
            }
            _ => anyhow::bail!("Expected Pg metadata for PgChecker"),
        }
    }

    async fn fetch_dst_struct(&self, src: &StructStatement) -> anyhow::Result<StructStatement> {
        self.fetch_dst_struct_statement(src).await
    }
}

impl PgChecker {
    pub async fn fetch_dst_struct_statement(
        &self,
        src_statement: &StructStatement,
    ) -> anyhow::Result<StructStatement> {
        let schema = match src_statement {
            StructStatement::PgCreateSchema(s) => s.schema.name.clone(),
            StructStatement::PgCreateTable(s) => s.table.schema_name.clone(),
            _ => return Ok(StructStatement::Unknown),
        };

        let mut struct_fetcher = PgStructFetcher {
            conn_pool: self.conn_pool.to_owned(),
            schemas: HashSet::from([schema.clone()]),
            filter: None,
        };

        match src_statement {
            StructStatement::PgCreateSchema(_) => {
                let mut statements = struct_fetcher.get_create_schema_statements(&schema).await?;
                if statements.is_empty() {
                    Ok(StructStatement::Unknown)
                } else {
                    Ok(StructStatement::PgCreateSchema(statements.remove(0)))
                }
            }
            StructStatement::PgCreateTable(statement) => {
                let mut statements = struct_fetcher
                    .get_create_table_statements(&schema, &statement.table.table_name)
                    .await?;
                if statements.is_empty() {
                    Ok(StructStatement::Unknown)
                } else {
                    Ok(StructStatement::PgCreateTable(statements.remove(0)))
                }
            }
            _ => Ok(StructStatement::Unknown),
        }
    }
}
