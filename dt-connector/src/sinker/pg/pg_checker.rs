use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{Pool, Postgres};
use std::collections::HashSet;

use crate::{
    meta_fetcher::pg::pg_struct_fetcher::PgStructFetcher,
    rdb_query_builder::RdbQueryBuilder,
    sinker::base_checker::{Checker, CheckerCommon, CheckerTbMeta},
};
use dt_common::meta::{
    col_value::ColValue, pg::pg_meta_manager::PgMetaManager, row_data::RowData,
    struct_meta::statement::struct_statement::StructStatement,
};

#[derive(Clone)]
pub struct PgChecker {
    pub conn_pool: Pool<Postgres>,
    pub meta_manager: PgMetaManager,
    pub common: CheckerCommon,
}

#[async_trait]
impl Checker for PgChecker {
    fn common_mut(&mut self) -> &mut CheckerCommon {
        &mut self.common
    }

    async fn get_tb_meta_by_row(&mut self, row: &RowData) -> anyhow::Result<CheckerTbMeta> {
        Ok(CheckerTbMeta::Pg(
            self.meta_manager
                .get_tb_meta_by_row_data(row)
                .await?
                .clone(),
        ))
    }

    async fn fetch_batch(
        &self,
        tb_meta: &CheckerTbMeta,
        data: &[RowData],
    ) -> anyhow::Result<Vec<RowData>> {
        let pg_meta = tb_meta.pg()?;
        let qb = RdbQueryBuilder::new_for_pg(pg_meta, None);

        let mut res = Vec::with_capacity(data.len());
        let mut batch_rows = Vec::with_capacity(data.len());

        for row in data {
            let has_null_key = row.require_after().ok().map_or(false, |after| {
                pg_meta
                    .basic
                    .id_cols
                    .iter()
                    .any(|col| matches!(after.get(col), Some(ColValue::None) | None))
            });

            if has_null_key {
                let query_info = qb.get_select_query(row)?;
                let query = qb.create_pg_query(&query_info)?;
                let mut rows = query.fetch(&self.conn_pool);
                while let Some(r) = rows.try_next().await? {
                    res.push(RowData::from_pg_row(&r, pg_meta, &None));
                }
            } else {
                batch_rows.push(row.clone());
            }
        }

        if !batch_rows.is_empty() {
            let query_info = qb.get_batch_select_query(&batch_rows, 0, batch_rows.len())?;
            let query = qb.create_pg_query(&query_info)?;
            let mut rows = query.fetch(&self.conn_pool);
            while let Some(row) = rows.try_next().await? {
                res.push(RowData::from_pg_row(&row, pg_meta, &None));
            }
        }

        Ok(res)
    }

    async fn fetch_dst_struct(&self, src: &StructStatement) -> anyhow::Result<StructStatement> {
        let schema = match src {
            StructStatement::PgCreateSchema(s) => s.schema.name.clone(),
            StructStatement::PgCreateTable(s) => s.table.schema_name.clone(),
            _ => return Ok(StructStatement::Unknown),
        };

        let mut struct_fetcher = PgStructFetcher {
            conn_pool: self.conn_pool.to_owned(),
            schemas: HashSet::from([schema.clone()]),
            filter: None,
        };

        match src {
            StructStatement::PgCreateSchema(_) => {
                let statement = struct_fetcher
                    .get_create_schema_statements(&schema)
                    .await?
                    .into_iter()
                    .next();
                Ok(statement
                    .map(StructStatement::PgCreateSchema)
                    .unwrap_or(StructStatement::Unknown))
            }
            StructStatement::PgCreateTable(statement) => {
                let statement = struct_fetcher
                    .get_create_table_statements(&schema, &statement.table.table_name)
                    .await?
                    .into_iter()
                    .next();
                Ok(statement
                    .map(StructStatement::PgCreateTable)
                    .unwrap_or(StructStatement::Unknown))
            }
            _ => Ok(StructStatement::Unknown),
        }
    }
}
