use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{Pool, Postgres};

use dt_common::meta::{pg::pg_meta_manager::PgMetaManager, row_data::RowData};

use crate::checker::base_checker::{
    has_null_key, CheckContext, Checker, CheckerTbMeta, DataCheckerHandle, FetchResult,
    CHECKER_MAX_QUERY_BATCH,
};
use crate::rdb_query_builder::RdbQueryBuilder;

pub struct PgChecker {
    conn_pool: Pool<Postgres>,
    meta_manager: PgMetaManager,
}

#[async_trait]
impl Checker for PgChecker {
    async fn fetch(&mut self, src_rows: &[Arc<RowData>]) -> anyhow::Result<FetchResult> {
        let first_row = src_rows
            .first()
            .context("fetch called with empty src rows")?;

        let tb_meta = Arc::new(CheckerTbMeta::Pg(
            self.meta_manager
                .get_tb_meta_by_row_data(first_row)
                .await?
                .clone(),
        ));
        let pg_meta = tb_meta.pg()?;
        let qb = RdbQueryBuilder::new_for_pg(pg_meta, None);

        let mut res = Vec::with_capacity(src_rows.len());
        let mut batch_rows = Vec::with_capacity(src_rows.len());

        for row in src_rows {
            if has_null_key(row, &pg_meta.basic.id_cols) {
                continue;
            }
            batch_rows.push(row.clone());
        }

        if batch_rows.is_empty() {
            return Ok(FetchResult {
                tb_meta,
                src_rows: Vec::new(),
                dst_rows: res,
            });
        }

        let src_rows = batch_rows.clone();
        for chunk in batch_rows.chunks(CHECKER_MAX_QUERY_BATCH) {
            let batch_refs: Vec<&Arc<RowData>> = chunk.iter().collect();
            let query_info = qb.get_batch_select_query(&batch_refs, 0, batch_refs.len())?;
            let query = qb.create_pg_query(&query_info)?;
            let mut rows = query.fetch(&self.conn_pool);
            while let Some(row) = rows.try_next().await? {
                res.push(RowData::from_pg_row(&row, pg_meta, &None));
            }
        }

        Ok(FetchResult {
            tb_meta,
            src_rows,
            dst_rows: res,
        })
    }
}

impl PgChecker {
    pub fn spawn(
        conn_pool: Pool<Postgres>,
        meta_manager: PgMetaManager,
        ctx: CheckContext,
        buffer_size: usize,
        drop_on_full: bool,
    ) -> DataCheckerHandle {
        DataCheckerHandle::spawn(
            Self {
                conn_pool,
                meta_manager,
            },
            ctx,
            buffer_size,
            drop_on_full,
            "PgChecker",
        )
    }
}
