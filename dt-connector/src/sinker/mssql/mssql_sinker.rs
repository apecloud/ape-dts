use std::{cmp, time::Instant};

use anyhow::{bail, Context};
use async_trait::async_trait;
use dt_common::{
    config::config_enums::DbType,
    meta::{
        ddl_meta::ddl_data::DdlData,
        mssql::{
            mssql_connection_pool::MssqlConnectionPool, mssql_meta_manager::MssqlMetaManager,
            MssqlTransaction,
        },
        row_data::RowData,
        row_type::RowType,
    },
    utils::{limit_queue::LimitedQueue, sql_util::SqlUtil},
};
use tiberius::Query as MssqlQuery;

use crate::{
    rdb_query_builder::RdbQueryBuilder, rdb_router::RdbRouter, sinker::base_sinker::BaseSinker,
    Sinker,
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

struct MssqlSinkSession<'pool> {
    transaction: MssqlTransaction<'pool>,
    identity_insert_table: Option<(String, String)>,
}

impl<'pool> MssqlSinkSession<'pool> {
    async fn begin(pool: &'pool MssqlConnectionPool) -> anyhow::Result<Self> {
        Ok(Self {
            transaction: pool.begin().await?,
            identity_insert_table: None,
        })
    }

    async fn enable_identity_insert(&mut self, schema: &str, tb: &str) -> anyhow::Result<()> {
        self.execute_identity_insert(schema, tb, true).await?;
        self.identity_insert_table = Some((schema.to_string(), tb.to_string()));
        Ok(())
    }

    async fn disable_identity_insert(&mut self) -> anyhow::Result<()> {
        let Some((schema, tb)) = self.identity_insert_table.take() else {
            return Ok(());
        };
        self.execute_identity_insert(&schema, &tb, false).await
    }

    async fn execute(&mut self, query: MssqlQuery<'_>) -> anyhow::Result<u64> {
        Ok(query.execute(self.transaction.client_mut()).await?.total())
    }

    async fn commit(mut self) -> anyhow::Result<()> {
        self.disable_identity_insert().await?;
        self.transaction.commit().await
    }

    async fn rollback(mut self) -> anyhow::Result<()> {
        let identity_result = self.disable_identity_insert().await;
        let rollback_result = self.transaction.rollback().await;
        identity_result?;
        rollback_result
    }

    async fn execute_identity_insert(
        &mut self,
        schema: &str,
        tb: &str,
        enable: bool,
    ) -> anyhow::Result<()> {
        let state = if enable { "ON" } else { "OFF" };
        let sql = format!(
            "SET IDENTITY_INSERT {}.{} {state}",
            MssqlSinker::quote(schema),
            MssqlSinker::quote(tb)
        );
        self.transaction
            .client_mut()
            .simple_query(&sql)
            .await
            .with_context(|| format!("failed to execute {sql}"))?
            .into_results()
            .await
            .with_context(|| format!("failed to consume response for {sql}"))?;
        Ok(())
    }
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

        let identity_col = self
            .get_identity_col(&tb_meta.basic.schema, &tb_meta.basic.tb)
            .await?;
        let query_builder = RdbQueryBuilder::new_for_mssql_with_identity_col(
            &tb_meta,
            None,
            identity_col.as_deref(),
        );
        let (query_info, data_size) =
            query_builder.get_batch_insert_query(data, start_index, batch_size, self.replace)?;
        let uses_replace_merge = self.replace
            && !tb_meta.basic.order_cols.is_empty()
            && tb_meta
                .basic
                .order_cols
                .iter()
                .all(|col| query_info.cols.contains(col));
        let allows_noop_matches = uses_replace_merge
            && query_info.cols.iter().all(|col| {
                tb_meta.basic.order_cols.contains(col)
                    || identity_col
                        .as_ref()
                        .is_some_and(|identity| identity == col)
            });
        let replaces_identity_by_reinsert = uses_replace_merge
            && identity_col.as_ref().is_some_and(|identity| {
                query_info.cols.contains(identity) && !tb_meta.basic.order_cols.contains(identity)
            });
        let identity_insert = identity_col
            .as_ref()
            .is_some_and(|identity_col| query_info.cols.contains(identity_col));
        let query = query_builder.create_mssql_query(&query_info)?;

        let start_time = Instant::now();
        let mut session = MssqlSinkSession::begin(&self.connection_pool).await?;
        let execute_result = async {
            if identity_insert {
                session
                    .enable_identity_insert(&tb_meta.basic.schema, &tb_meta.basic.tb)
                    .await?;
            }
            let affected = session.execute(query).await?;
            let affected_is_valid = if replaces_identity_by_reinsert {
                affected >= batch_size as u64 && affected <= (batch_size * 2) as u64
            } else {
                affected == batch_size as u64
                    || (allows_noop_matches && affected < batch_size as u64)
            };
            if !affected_is_valid {
                bail!(
                    "MSSQL snapshot insert affected {affected} rows, expected {batch_size} for {}.{}",
                    tb_meta.basic.schema,
                    tb_meta.basic.tb
                );
            }
            anyhow::Ok(())
        }
        .await;

        if let Err(error) = execute_result {
            if let Err(rollback_error) = session.rollback().await {
                return Err(error).context(format!(
                    "MSSQL snapshot insert rollback also failed: {rollback_error}"
                ));
            }
            return Err(error);
        }
        session.commit().await?;

        let mut rts = LimitedQueue::new(1);
        rts.push((start_time.elapsed().as_millis() as u64, 1));
        self.base_sinker
            .update_batch_monitor_for(&task_id, batch_size as u64, data_size as u64)
            .await?;
        self.base_sinker.update_monitor_rt_for(&task_id, &rts).await
    }

    async fn get_identity_col(&self, schema: &str, tb: &str) -> anyhow::Result<Option<String>> {
        let mut query = MssqlQuery::new(
            "SELECT c.name AS identity_col \
             FROM sys.identity_columns AS c \
             JOIN sys.tables AS t ON t.object_id = c.object_id \
             JOIN sys.schemas AS s ON s.schema_id = t.schema_id \
             WHERE s.name = @P1 AND t.name = @P2",
        );
        query.bind(schema);
        query.bind(tb);
        let mut connection = self.connection_pool.get().await?;
        let Some(row) = query
            .query(connection.client_mut())
            .await?
            .into_row()
            .await?
        else {
            return Ok(None);
        };
        let identity_col = dt_common::meta::adaptor::mssql_col_value_convertor::MssqlColValueConvertor::from_query_required_string(
            &row,
            "identity_col",
        )?;
        Ok(Some(identity_col))
    }

    fn quote(identifier: &str) -> String {
        SqlUtil::escape_by_db_type(identifier, &DbType::Mssql)
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
            let insert_col_count = first.require_after()?.len();
            if insert_col_count == 0 {
                bail!("MSSQL snapshot insert row has no columns");
            }
            if insert_col_count > 2100 {
                bail!(
                    "MSSQL snapshot insert row has {insert_col_count} columns, exceeding the 2100 parameter limit"
                );
            }
            let max_rows_per_statement = cmp::min(1000, 2100 / insert_col_count);
            let same_table_count = data[start..]
                .iter()
                .take_while(|row| row.schema == first.schema && row.tb == first.tb)
                .count();
            let batch_size = cmp::min(
                cmp::min(self.batch_size, same_table_count),
                max_rows_per_statement,
            );
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
