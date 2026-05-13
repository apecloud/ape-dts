use std::sync::Arc;

use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{Pool, Postgres};

use dt_common::meta::{
    ddl_meta::ddl_data::DdlData, pg::pg_meta_manager::PgMetaManager, row_data::RowData,
};

use crate::checker::base_checker::{Checker, CheckerTbMeta, CHECKER_MAX_QUERY_BATCH};
use crate::rdb_query_builder::RdbQueryBuilder;

pub struct PgChecker {
    conn_pool: Pool<Postgres>,
    meta_manager: PgMetaManager,
}

#[async_trait]
impl Checker for PgChecker {
    async fn load_table_meta(
        &mut self,
        lookup_row: &RowData,
    ) -> anyhow::Result<Arc<CheckerTbMeta>> {
        Ok(Arc::new(CheckerTbMeta::Pg(
            self.meta_manager
                .get_tb_meta_by_row_data(lookup_row)
                .await?
                .clone(),
        )))
    }

    async fn fetch_rows_by_keys(
        &mut self,
        table_meta: Arc<CheckerTbMeta>,
        lookup_rows: &[&RowData],
    ) -> anyhow::Result<Vec<RowData>> {
        let CheckerTbMeta::Pg(pg_meta) = table_meta.as_ref() else {
            unreachable!()
        };
        let qb = RdbQueryBuilder::new_for_pg(pg_meta, None);

        let mut res = Vec::with_capacity(lookup_rows.len());
        for chunk in lookup_rows.chunks(CHECKER_MAX_QUERY_BATCH) {
            let query_info = qb.get_batch_select_query(chunk, 0, chunk.len())?;
            let query = qb.create_pg_query(&query_info)?;
            let mut rows = query.fetch(&self.conn_pool);
            while let Some(row) = rows.try_next().await? {
                res.push(RowData::from_pg_row(&row, pg_meta, &None));
            }
        }

        Ok(res)
    }

    async fn invalidate_meta_cache(&mut self, data: &[DdlData]) -> anyhow::Result<()> {
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
