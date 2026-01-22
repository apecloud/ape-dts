use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{MySql, Pool};

use dt_common::meta::{mysql::mysql_meta_manager::MysqlMetaManager, row_data::RowData};

use crate::checker::base_checker::{
    has_null_key, CheckContext, Checker, CheckerTbMeta, DataCheckerHandle, FetchResult,
    CHECKER_MAX_QUERY_BATCH,
};
use crate::rdb_query_builder::RdbQueryBuilder;

pub struct MysqlChecker {
    conn_pool: Pool<MySql>,
    meta_manager: MysqlMetaManager,
}

#[async_trait]
impl Checker for MysqlChecker {
    async fn fetch(&mut self, src_rows: &[Arc<RowData>]) -> anyhow::Result<FetchResult> {
        let first_row = src_rows
            .first()
            .context("fetch called with empty src rows")?;

        let tb_meta = Arc::new(CheckerTbMeta::Mysql(
            self.meta_manager
                .get_tb_meta_by_row_data(first_row)
                .await?
                .clone(),
        ));
        let mysql_meta = tb_meta.mysql()?;
        let qb = RdbQueryBuilder::new_for_mysql(mysql_meta, None);

        let mut res = Vec::with_capacity(src_rows.len());
        let mut check_rows = Vec::with_capacity(src_rows.len());
        let mut batch_rows = Vec::with_capacity(src_rows.len());

        for row in src_rows {
            check_rows.push(row.clone());
            if has_null_key(row, &mysql_meta.basic.id_cols) {
                continue;
            }
            batch_rows.push(row.clone());
        }

        if batch_rows.is_empty() {
            return Ok(FetchResult {
                tb_meta,
                src_rows: check_rows,
                dst_rows: res,
            });
        }

        let src_rows = check_rows;
        for chunk in batch_rows.chunks(CHECKER_MAX_QUERY_BATCH) {
            let batch_refs: Vec<&Arc<RowData>> = chunk.iter().collect();
            let query_info = qb.get_batch_select_query(&batch_refs, 0, batch_refs.len())?;
            let query = qb.create_mysql_query(&query_info)?;
            let mut rows = query.fetch(&self.conn_pool);
            while let Some(row) = rows.try_next().await? {
                res.push(RowData::from_mysql_row(&row, mysql_meta, &None));
            }
        }

        Ok(FetchResult {
            tb_meta,
            src_rows,
            dst_rows: res,
        })
    }
}

impl MysqlChecker {
    pub fn spawn(
        conn_pool: Pool<MySql>,
        meta_manager: MysqlMetaManager,
        ctx: CheckContext,
        buffer_size: usize,
    ) -> DataCheckerHandle {
        DataCheckerHandle::spawn(
            Self {
                conn_pool,
                meta_manager,
            },
            ctx,
            buffer_size,
            "MysqlChecker",
        )
    }
}
