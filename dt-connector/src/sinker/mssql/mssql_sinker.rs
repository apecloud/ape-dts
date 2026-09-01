use std::{cmp, time::Instant};

use anyhow::{bail, Context};
use async_trait::async_trait;
use dt_common::{
    log_error,
    meta::{
        ddl_meta::ddl_data::DdlData,
        dt_data::{DtData, DtItem},
        mssql::{
            mssql_connection_pool::MssqlConnectionPool, mssql_meta_manager::MssqlMetaManager,
            mssql_query_builder::MssqlTableSqlBuilder,
            mssql_table_sink_session::MssqlTableSinkSession, mssql_tb_meta::MssqlTbMeta,
        },
        position::Position,
        row_data::RowData,
        row_type::RowType,
    },
    utils::limit_queue::LimitedQueue,
};

use crate::{rdb_router::RdbRouter, sinker::base_sinker::BaseSinker, Sinker};

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
        tb_meta: &MssqlTbMeta,
        session: &mut MssqlTableSinkSession<'_, '_>,
    ) -> anyhow::Result<()> {
        let rows = &data[start_index..start_index + batch_size];
        let task_id = self.base_sinker.source_task_id_for_rows(rows, &self.router);
        self.base_sinker.ensure_monitor_for(&task_id);
        let data_size = rows.iter().map(|row| row.data_size).sum::<usize>();
        let use_bulk_insert = session.can_bulk_insert(rows);

        let start_time = Instant::now();
        let mut rts = LimitedQueue::new(1);
        let batch_result = if use_bulk_insert {
            session.bulk_insert(rows).await
        } else {
            let query_builder = MssqlTableSqlBuilder::new(tb_meta, None);
            let (query_info, _) = query_builder.get_batch_insert_query(rows)?;
            let query = query_builder.create_query(&query_info)?;
            query
                .execute(session.client_mut())
                .await
                .map(|_| ())
                .map_err(anyhow::Error::from)
        };
        match batch_result {
            Ok(_) => {
                rts.push((start_time.elapsed().as_millis() as u64, 1));
            }
            Err(batch_error) => {
                log_error!(
                    "MSSQL batch insert failed, will sink one by one in one transaction, schema: {}, tb: {}, bulk_insert: {}, upsert: {}, error: {:#}",
                    tb_meta.basic.schema,
                    tb_meta.basic.tb,
                    use_bulk_insert,
                    self.replace,
                    batch_error
                );
                self.serial_insert(rows, tb_meta, session).await?;
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

    async fn serial_insert(
        &mut self,
        rows: &[RowData],
        tb_meta: &MssqlTbMeta,
        session: &mut MssqlTableSinkSession<'_, '_>,
    ) -> anyhow::Result<()> {
        let task_id = self.base_sinker.source_task_id_for_rows(rows, &self.router);
        self.base_sinker.ensure_monitor_for(&task_id);
        let query_builder = MssqlTableSqlBuilder::new(tb_meta, None);
        if let Err(begin_error) = session.begin().await {
            let transaction_cleanup_error = session.rollback().await.err();
            return Err(Self::with_transaction_cleanup_error(
                begin_error,
                transaction_cleanup_error,
            ));
        }

        let mut rts = LimitedQueue::new(cmp::min(100, rows.len()));
        let sink_result: anyhow::Result<()> = async {
            for row in rows {
                let query_info = query_builder.get_insert_query(row, self.replace)?;
                let query = query_builder.create_query(&query_info)?;
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
            let transaction_cleanup_error = session.rollback().await.err();
            return Err(Self::with_transaction_cleanup_error(
                error,
                transaction_cleanup_error,
            ));
        }

        if let Err(commit_error) = session.commit().await {
            let transaction_cleanup_error = session.rollback().await.err();
            return Err(Self::with_transaction_cleanup_error(
                commit_error,
                transaction_cleanup_error,
            ));
        }

        let data_size = rows.iter().map(RowData::get_data_size).sum::<u64>();
        self.base_sinker
            .update_serial_monitor_for(&task_id, rows.len() as u64, data_size)
            .await?;
        self.base_sinker.update_monitor_rt_for(&task_id, &rts).await
    }

    fn with_transaction_cleanup_error(
        error: anyhow::Error,
        cleanup_error: Option<anyhow::Error>,
    ) -> anyhow::Error {
        match cleanup_error {
            Some(cleanup_error) => error.context(format!(
                "MSSQL transaction cleanup also failed: {cleanup_error:#}"
            )),
            None => error,
        }
    }
}

#[async_trait]
impl Sinker for MssqlSinker {
    async fn sink_dml(&mut self, data: Vec<RowData>, _batch: bool) -> anyhow::Result<()> {
        let Some(first) = data.first() else {
            return Ok(());
        };
        if self.batch_size == 0 {
            bail!("MSSQL sinker batch_size must be greater than 0");
        }
        if !matches!(first.row_type, RowType::Insert) {
            bail!("MSSQL snapshot sinker only supports INSERT rows");
        }

        if first.require_after()?.is_empty() {
            bail!("MSSQL snapshot insert row has no columns");
        }

        let tb_meta = self
            .meta_manager
            .get_tb_meta_by_row_data(first)
            .await?
            .clone();
        // The session borrows this local pool clone, allowing batch helpers to
        // keep borrowing the sinker while all writes use one physical connection.
        let connection_pool = self.connection_pool.clone();
        let mut session = connection_pool.get_table_sink_session(&tb_meta).await?;
        let sink_result: anyhow::Result<()> = async {
            let mut start = 0;
            while start < data.len() {
                let first = &data[start];
                if first.require_after()?.is_empty() {
                    bail!("MSSQL snapshot insert row has no columns");
                }
                // TODO: When using parameter binding, split MSSQL batches by both server limits:
                // - 2,100 procedure parameters; Tiberius's sp_executesql RPC also consumes two.
                //   https://learn.microsoft.com/en-us/sql/relational-databases/stored-procedures/specify-parameters
                // - 1,000 rows in an INSERT ... VALUES table value constructor.
                //   https://learn.microsoft.com/en-us/sql/t-sql/queries/table-value-constructor-transact-sql
                let batch_size = cmp::min(self.batch_size, data.len() - start);
                self.batch_insert(&data, start, batch_size, &tb_meta, &mut session)
                    .await?;
                start += batch_size;
            }
            Ok(())
        }
        .await;

        match sink_result {
            Ok(()) => session.finalize().await,
            Err(error) => match session.finalize().await {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(error.context(format!(
                    "MSSQL table sink session cleanup also failed: {cleanup_error:#}"
                ))),
            },
        }
    }

    async fn sink_ddl(&mut self, _data: Vec<DdlData>, _batch: bool) -> anyhow::Result<()> {
        bail!("MSSQL snapshot sinker does not support DDL")
    }

    async fn handle_control_item(&mut self, item: &DtItem) -> anyhow::Result<()> {
        if let (DtData::Commit { .. }, Position::RdbSnapshotFinished { db, schema, tb, .. }) =
            (&item.dt_data, &item.position)
        {
            let (routed_db, routed_schema, routed_tb) = if let Some(router) = &self.router {
                router.get_tb_map_with_db(db, schema, tb)
            } else {
                (db.as_str(), schema.as_str(), tb.as_str())
            };
            self.meta_manager
                .invalidate_cache_for_table(routed_db, routed_schema, routed_tb);
        }
        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
