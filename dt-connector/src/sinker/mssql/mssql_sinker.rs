use std::{cmp, time::Instant};

use crate::{
    rdb_query_builder::RdbQueryBuilder, rdb_router::RdbRouter, sinker::base_sinker::BaseSinker,
    Sinker,
};
use anyhow::{bail, Context};
use async_trait::async_trait;
use dt_common::{
    meta::{
        ddl_meta::ddl_data::DdlData,
        mssql::{mssql_connection_pool::MssqlConnectionPool, mssql_meta_manager::MssqlMetaManager},
        row_data::RowData,
        row_type::RowType,
    },
    utils::limit_queue::LimitedQueue,
};

#[derive(Clone)]
pub struct MssqlSinker {
    pub connection_pool: MssqlConnectionPool,
    pub meta_manager: MssqlMetaManager,
    pub router: Option<RdbRouter>,
    pub batch_size: usize,
    pub replace: bool,
    pub base_sinker: BaseSinker,
}

impl MssqlSinker {
    pub fn new(
        connection_pool: MssqlConnectionPool,
        meta_manager: MssqlMetaManager,
        router: Option<RdbRouter>,
        batch_size: usize,
        replace: bool,
        base_sinker: BaseSinker,
    ) -> Self {
        Self {
            connection_pool,
            meta_manager,
            router,
            batch_size,
            replace,
            base_sinker,
        }
    }

    async fn batch_insert(
        &mut self,
        data: &[RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let rows = &data[start_index..start_index + batch_size];
        let task_id = self.base_sinker.source_task_id_for_rows(rows, &self.router);
        self.base_sinker.ensure_monitor_for(&task_id);
        let tb_meta = self
            .meta_manager
            .get_tb_meta_by_row_data(&rows[0])
            .await?
            .clone();
        if rows
            .iter()
            .any(|row| row.schema != tb_meta.basic.schema || row.tb != tb_meta.basic.tb)
        {
            bail!("MSSQL snapshot insert batch contains rows from different tables");
        }

        let query_builder = RdbQueryBuilder::new_for_mssql(&tb_meta, None);
        let (query_info, data_size) =
            query_builder.get_batch_insert_query(data, start_index, batch_size, self.replace)?;
        let query = query_builder.create_mssql_query(&query_info)?;

        let start_time = Instant::now();
        let mut transaction = self.connection_pool.begin().await?;
        if let Err(error) = query.execute(transaction.client_mut()).await {
            let error = anyhow::Error::from(error);
            if let Err(rollback_error) = transaction.rollback().await {
                return Err(error).context(format!(
                    "MSSQL snapshot insert rollback also failed: {rollback_error}"
                ));
            }
            return Err(error);
        }
        transaction.commit().await?;

        let mut rts = LimitedQueue::new(1);
        rts.push((start_time.elapsed().as_millis() as u64, 1));
        self.base_sinker
            .update_batch_monitor_for(&task_id, batch_size as u64, data_size as u64)
            .await?;
        self.base_sinker.update_monitor_rt_for(&task_id, &rts).await
    }
}

#[async_trait]
impl Sinker for MssqlSinker {
    async fn sink_dml(&mut self, data: Vec<RowData>, _batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        if self.batch_size == 0 {
            bail!("MSSQL sinker batch_size must be greater than 0");
        }
        if data
            .iter()
            .any(|row| !matches!(row.row_type, RowType::Insert))
        {
            bail!("MSSQL snapshot sinker only supports INSERT rows");
        }

        let mut start = 0;
        while start < data.len() {
            let first = &data[start];
            if first.require_after()?.is_empty() {
                bail!("MSSQL snapshot insert row has no columns");
            }
            let same_table_count = data[start..]
                .iter()
                .take_while(|row| row.schema == first.schema && row.tb == first.tb)
                .count();
            // TODO: Split MSSQL batches by both server limits:
            // - 2,100 procedure parameters; Tiberius's sp_executesql RPC also consumes two.
            //   https://learn.microsoft.com/en-us/sql/relational-databases/stored-procedures/specify-parameters
            // - 1,000 rows in an INSERT ... VALUES table value constructor.
            //   https://learn.microsoft.com/en-us/sql/t-sql/queries/table-value-constructor-transact-sql
            let batch_size = cmp::min(self.batch_size, same_table_count);
            self.batch_insert(&data, start, batch_size).await?;
            start += batch_size;
        }
        Ok(())
    }

    async fn sink_ddl(&mut self, _data: Vec<DdlData>, _batch: bool) -> anyhow::Result<()> {
        bail!("MSSQL snapshot sinker does not support DDL")
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
