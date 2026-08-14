use std::{cmp, time::Instant};

use crate::{
    rdb_query_builder::RdbQueryBuilder, rdb_router::RdbRouter, sinker::base_sinker::BaseSinker,
    Sinker,
};
use anyhow::{bail, Context};
use async_trait::async_trait;
use dt_common::{
    log_error,
    meta::{
        ddl_meta::ddl_data::DdlData,
        mssql::{
            mssql_connection_pool::MssqlConnectionPool, mssql_meta_manager::MssqlMetaManager,
            mssql_tb_meta::MssqlTbMeta,
        },
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

        let query_builder = RdbQueryBuilder::new_for_mssql(&tb_meta, None);
        // Like the PostgreSQL sinker, always try the cheapest multi-row insert
        // first. The serial fallback applies the configured insert semantics.
        let (query_info, data_size) =
            query_builder.get_batch_insert_query(data, start_index, batch_size, false)?;
        let query = query_builder.create_mssql_query(&query_info)?;

        let start_time = Instant::now();
        let mut session = self
            .connection_pool
            .get_table_sink_session(&tb_meta)
            .await?;
        let mut rts = LimitedQueue::new(1);
        match query.execute(session.client_mut()).await {
            Ok(_) => {
                session.post().await?;
                rts.push((start_time.elapsed().as_millis() as u64, 1));
            }
            Err(batch_error) => {
                let batch_error = anyhow::Error::from(batch_error);
                let post_error = session.post().await.err();
                drop(session);

                let batch_error = Self::with_session_cleanup_errors(batch_error, None, post_error);
                log_error!(
                    "MSSQL batch insert failed, will sink one by one in one transaction, schema: {}, tb: {}, replace: {}, error: {:#}",
                    tb_meta.basic.schema,
                    tb_meta.basic.tb,
                    self.replace,
                    batch_error
                );
                self.serial_sink(rows, &tb_meta).await?;
            }
        }

        self.base_sinker
            .update_batch_monitor_for(&task_id, batch_size as u64, data_size as u64)
            .await?;
        if !rts.is_empty() {
            self.base_sinker
                .update_monitor_rt_for(&task_id, &rts)
                .await?;
        }
        Ok(())
    }

    async fn serial_sink(&mut self, rows: &[RowData], tb_meta: &MssqlTbMeta) -> anyhow::Result<()> {
        let task_id = self.base_sinker.source_task_id_for_rows(rows, &self.router);
        self.base_sinker.ensure_monitor_for(&task_id);
        let query_builder = RdbQueryBuilder::new_for_mssql(tb_meta, None);
        let mut session = self.connection_pool.get_table_sink_session(tb_meta).await?;
        if let Err(begin_error) = session.begin().await {
            let rollback_error = session.rollback().await.err();
            let post_error = session.post().await.err();
            return Err(Self::with_session_cleanup_errors(
                begin_error,
                rollback_error,
                post_error,
            ));
        }

        let mut rts = LimitedQueue::new(cmp::min(100, rows.len()));
        let sink_result: anyhow::Result<()> = async {
            for row in rows {
                let query_info = query_builder.get_query_info(row, self.replace)?;
                let query = query_builder.create_mssql_query(&query_info)?;
                let start_time = Instant::now();
                query
                    .execute(session.client_mut())
                    .await
                    .map_err(anyhow::Error::from)
                    .with_context(|| {
                        format!(
                            "MSSQL serial sink failed, sql: [{}], row_data: [{}]",
                            query_info.sql, row
                        )
                    })?;
                rts.push((start_time.elapsed().as_millis() as u64, 1));
            }
            Ok(())
        }
        .await;
        if let Err(error) = sink_result {
            let rollback_error = session.rollback().await.err();
            let post_error = session.post().await.err();
            return Err(Self::with_session_cleanup_errors(
                error,
                rollback_error,
                post_error,
            ));
        }

        if let Err(commit_error) = session.commit().await {
            let rollback_error = session.rollback().await.err();
            let post_error = session.post().await.err();
            return Err(Self::with_session_cleanup_errors(
                commit_error,
                rollback_error,
                post_error,
            ));
        }
        session.post().await?;

        let data_size = rows.iter().map(RowData::get_data_size).sum::<u64>();
        self.base_sinker
            .update_serial_monitor_for(&task_id, rows.len() as u64, data_size)
            .await?;
        self.base_sinker.update_monitor_rt_for(&task_id, &rts).await
    }

    fn with_session_cleanup_errors(
        error: anyhow::Error,
        rollback_error: Option<anyhow::Error>,
        post_error: Option<anyhow::Error>,
    ) -> anyhow::Error {
        let mut cleanup_errors = Vec::with_capacity(2);
        if let Some(rollback_error) = rollback_error {
            cleanup_errors.push(format!("rollback also failed: {rollback_error:#}"));
        }
        if let Some(post_error) = post_error {
            cleanup_errors.push(format!("post also failed: {post_error:#}"));
        }
        if cleanup_errors.is_empty() {
            error
        } else {
            error.context(format!(
                "MSSQL table sink session cleanup failed: {}",
                cleanup_errors.join("; ")
            ))
        }
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
            // TODO: Split MSSQL batches by both server limits:
            // - 2,100 procedure parameters; Tiberius's sp_executesql RPC also consumes two.
            //   https://learn.microsoft.com/en-us/sql/relational-databases/stored-procedures/specify-parameters
            // - 1,000 rows in an INSERT ... VALUES table value constructor.
            //   https://learn.microsoft.com/en-us/sql/t-sql/queries/table-value-constructor-transact-sql
            let batch_size = cmp::min(self.batch_size, data.len() - start);
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
