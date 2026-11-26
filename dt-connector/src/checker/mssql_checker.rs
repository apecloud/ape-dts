use std::sync::Arc;

use anyhow::{ensure, Result};
use async_trait::async_trait;
use dt_common::meta::{
    mssql::{
        mssql_connection_pool::MssqlConnectionPool, mssql_meta_manager::MssqlMetaManager,
        mssql_query_builder::MssqlTableSqlBuilder,
    },
    row_data::RowData,
};
use futures::TryStreamExt;

use super::base_checker::{Checker, CheckerTbMeta, CHECKER_MAX_QUERY_BATCH};

pub struct MssqlChecker {
    pool: MssqlConnectionPool,
    meta_manager: MssqlMetaManager,
}

impl MssqlChecker {
    pub async fn new(pool: MssqlConnectionPool) -> Result<Self> {
        let meta_manager = MssqlMetaManager::new(pool.clone()).await?;
        Ok(Self { pool, meta_manager })
    }
}

#[async_trait]
impl Checker for MssqlChecker {
    async fn load_table_meta(&mut self, row: &RowData) -> Result<Arc<CheckerTbMeta>> {
        let mut meta = self
            .meta_manager
            .get_tb_meta_by_row_data(row)
            .await?
            .clone();
        if meta
            .basic
            .id_cols
            .iter()
            .any(|col| meta.is_server_generated(col))
        {
            // A partial composite key cannot identify a row across the two databases.
            meta.basic.id_cols = meta
                .basic
                .cols
                .iter()
                .filter(|col| !meta.is_server_generated(col))
                .cloned()
                .collect();
        }
        ensure!(
            !meta.basic.id_cols.is_empty(),
            "no comparable MSSQL lookup columns for {}.{}.{}",
            row.db,
            row.schema,
            row.tb
        );
        Ok(Arc::new(CheckerTbMeta::Mssql(meta)))
    }

    async fn fetch_rows_by_keys(
        &mut self,
        table_meta: Arc<CheckerTbMeta>,
        rows: &[&RowData],
    ) -> Result<Vec<RowData>> {
        let CheckerTbMeta::Mssql(meta) = table_meta.as_ref() else {
            anyhow::bail!("MSSQL checker requires MSSQL table metadata");
        };
        let builder = MssqlTableSqlBuilder::new(meta, None);
        let batch_size = builder.max_lookup_rows()?.min(CHECKER_MAX_QUERY_BATCH);
        let mut connection = self.pool.get().await?;
        let mut result = Vec::with_capacity(rows.len());
        for chunk in rows.chunks(batch_size) {
            let info = builder.get_lookup_query(chunk)?;
            let query = builder.create_query(&info)?;
            let mut stream = query
                .query(connection.client_mut())
                .await?
                .into_row_stream();
            while let Some(row) = stream.try_next().await? {
                result.push(RowData::from_mssql_row(&row, meta, &None, None)?);
            }
        }
        Ok(result)
    }

    async fn invalidate_meta_cache(&mut self, db: &str, schema: &str, tb: &str) -> Result<()> {
        self.meta_manager.invalidate_cache_for_table(db, schema, tb);
        Ok(())
    }
}
