use std::sync::Arc;

use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{MySql, Pool};

use dt_common::meta::{
    ddl_meta::ddl_data::DdlData, mysql::mysql_meta_manager::MysqlMetaManager, row_data::RowData,
};

use crate::checker::base_checker::{Checker, CheckerTbMeta, CHECKER_MAX_QUERY_BATCH};
use crate::rdb_query_builder::RdbQueryBuilder;

pub struct MysqlChecker {
    conn_pool: Pool<MySql>,
    meta_manager: MysqlMetaManager,
}

#[async_trait]
impl Checker for MysqlChecker {
    async fn fetch_meta(&mut self, src_row: &RowData) -> anyhow::Result<Arc<CheckerTbMeta>> {
        Ok(Arc::new(CheckerTbMeta::Mysql(
            self.meta_manager
                .get_tb_meta_by_row_data(src_row)
                .await?
                .clone(),
        )))
    }

    async fn fetch(
        &mut self,
        tb_meta: Arc<CheckerTbMeta>,
        src_rows: &[&RowData],
    ) -> anyhow::Result<Vec<RowData>> {
        let CheckerTbMeta::Mysql(mysql_meta) = tb_meta.as_ref() else {
            unreachable!()
        };
        let qb = RdbQueryBuilder::new_for_mysql(mysql_meta, None);

        let mut res = Vec::with_capacity(src_rows.len());
        for chunk in src_rows.chunks(CHECKER_MAX_QUERY_BATCH) {
            let query_info = qb.get_batch_select_query(chunk, 0, chunk.len())?;
            let query = qb.create_mysql_query(&query_info)?;
            let mut rows = query.fetch(&self.conn_pool);
            while let Some(row) = rows.try_next().await? {
                res.push(RowData::from_mysql_row(&row, mysql_meta, &None));
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

impl MysqlChecker {
    pub fn new(conn_pool: Pool<MySql>, meta_manager: MysqlMetaManager) -> Self {
        Self {
            conn_pool,
            meta_manager,
        }
    }
}
