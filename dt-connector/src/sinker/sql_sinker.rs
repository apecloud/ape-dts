use std::sync::Arc;

use async_trait::async_trait;

use crate::{rdb_query_builder::RdbQueryBuilder, rdb_router::RdbRouter, Sinker};
use dt_common::{
    log_sql,
    meta::{rdb_meta_manager::RdbMetaManager, row_data::RowData},
    monitor::monitor::Monitor,
};

#[derive(Clone)]
pub struct SqlSinker {
    pub meta_manager: RdbMetaManager,
    pub router: RdbRouter,
    pub reverse: bool,
    pub monitor: Arc<Monitor>,
}

#[async_trait]
impl Sinker for SqlSinker {
    async fn sink_dml(&mut self, data: Vec<Arc<RowData>>, _batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        for row_data in data {
            let row_data = if self.reverse {
                row_data.as_ref().reverse()
            } else {
                (*row_data).clone()
            };

            let query_builder =
                if let Some(meta_manager) = self.meta_manager.mysql_meta_manager.as_mut() {
                    let tb_meta = meta_manager.get_tb_meta_by_row_data(&row_data).await?;
                    RdbQueryBuilder::new_for_mysql(tb_meta, None)
                } else if let Some(meta_manager) = self.meta_manager.pg_meta_manager.as_mut() {
                    let tb_meta = meta_manager.get_tb_meta_by_row_data(&row_data).await?;
                    RdbQueryBuilder::new_for_pg(tb_meta, None)
                } else {
                    continue;
                };

            let sql = query_builder.get_query_sql(&row_data, false)?;
            log_sql!("{}", sql);
        }

        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.meta_manager.close().await
    }
}
