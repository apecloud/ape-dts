use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{Pool, Postgres};

use dt_common::meta::{
    ddl_meta::ddl_data::DdlData, pg::pg_meta_manager::PgMetaManager, row_data::RowData,
};

use crate::checker::base_checker::{
    has_null_key, Checker, CheckerTbMeta, FetchResult, CHECKER_MAX_QUERY_BATCH,
};
use crate::rdb_query_builder::RdbQueryBuilder;

pub struct PgChecker {
    conn_pool: Pool<Postgres>,
    meta_manager: PgMetaManager,
}

#[async_trait]
impl Checker for PgChecker {
    async fn fetch(&mut self, src_rows: &[&RowData]) -> anyhow::Result<FetchResult> {
        let first_row = src_rows
            .first()
            .context("fetch called with empty src rows")?;

        let tb_meta = Arc::new(CheckerTbMeta::Pg(
            self.meta_manager
                .get_tb_meta_by_row_data(first_row)
                .await?
                .clone(),
        ));
        let CheckerTbMeta::Pg(pg_meta) = tb_meta.as_ref() else {
            unreachable!()
        };
        let qb = RdbQueryBuilder::new_for_pg(pg_meta, None);

        let mut res = Vec::with_capacity(src_rows.len());
        let (null_key_rows, queryable): (Vec<&RowData>, Vec<&RowData>) = src_rows
            .iter()
            .copied()
            .partition(|row| has_null_key(row, &pg_meta.basic.id_cols));

        for row in null_key_rows {
            let query_info = qb.get_select_query(row)?;
            let query = qb.create_pg_query(&query_info)?;
            let mut rows = query.fetch(&self.conn_pool);
            while let Some(row) = rows.try_next().await? {
                res.push(RowData::from_pg_row(&row, pg_meta, &None));
            }
        }

        for chunk in queryable.chunks(CHECKER_MAX_QUERY_BATCH) {
            let query_info = qb.get_batch_select_query(chunk, 0, chunk.len())?;
            let query = qb.create_pg_query(&query_info)?;
            let mut rows = query.fetch(&self.conn_pool);
            while let Some(row) = rows.try_next().await? {
                res.push(RowData::from_pg_row(&row, pg_meta, &None));
            }
        }

        Ok(FetchResult {
            tb_meta,
            dst_rows: res,
        })
    }

    async fn refresh_meta(&mut self, data: &[DdlData]) -> anyhow::Result<()> {
        for ddl_data in data {
            self.meta_manager.invalidate_cache_by_ddl_data(ddl_data);
        }
        Ok(())
    }
}

impl PgChecker {
    pub fn new(conn_pool: Pool<Postgres>, meta_manager: PgMetaManager) -> Self {
        Self {
            conn_pool,
            meta_manager,
        }
    }
}
