use std::{
    cmp,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_mutex::Mutex;
use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{Pool, Postgres};
use tokio::time::Instant;

use crate::{
    call_batch_fn,
    check_log::check_log::CheckSummaryLog,
    meta_fetcher::pg::pg_struct_fetcher::PgStructFetcher,
    rdb_query_builder::RdbQueryBuilder,
    rdb_router::RdbRouter,
    sinker::{
        base_checker::{BaseChecker, BatchCompareContext, BatchCompareRange, ReviseSqlContext},
        base_sinker::BaseSinker,
    },
    Sinker,
};
use dt_common::{
    log_sql, log_summary,
    meta::{
        pg::pg_meta_manager::PgMetaManager,
        rdb_meta_manager::RdbMetaManager,
        row_data::RowData,
        struct_meta::{statement::struct_statement::StructStatement, struct_data::StructData},
    },
    monitor::monitor::Monitor,
    rdb_filter::RdbFilter,
    utils::limit_queue::LimitedQueue,
};

#[derive(Clone)]
pub struct PgChecker {
    pub conn_pool: Pool<Postgres>,
    pub meta_manager: PgMetaManager,
    pub extractor_meta_manager: RdbMetaManager,
    pub reverse_router: RdbRouter,
    pub batch_size: usize,
    pub monitor: Arc<Monitor>,
    pub filter: RdbFilter,
    pub output_full_row: bool,
    pub output_revise_sql: bool,
    pub revise_match_full_row: bool,
    pub summary: CheckSummaryLog,
    pub global_summary: Option<Arc<Mutex<CheckSummaryLog>>>,
}

#[async_trait]
impl Sinker for PgChecker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        if !batch {
            self.serial_check(data).await?;
        } else {
            call_batch_fn!(self, data, Self::batch_check);
        }
        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        if self.summary.miss_count > 0
            || self.summary.diff_count > 0
            || self.summary.extra_count > 0
        {
            self.summary.end_time = chrono::Local::now().to_rfc3339();
            if let Some(global_summary) = &self.global_summary {
                let mut global_summary = global_summary.lock().await;
                global_summary.merge(&self.summary);
            } else {
                log_summary!("{}", self.summary);
            }
        }

        self.meta_manager.close().await?;
        self.extractor_meta_manager.close().await
    }

    async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        self.serial_check_struct(data).await?;
        Ok(())
    }
}

impl PgChecker {
    async fn serial_check(&mut self, data: Vec<RowData>) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let tb_meta = self.meta_manager.get_tb_meta_by_row_data(&data[0]).await?;
        let revise_ctx = self
            .output_revise_sql
            .then_some(ReviseSqlContext::pg(tb_meta, self.revise_match_full_row));

        let mut miss = Vec::new();
        let mut diff = Vec::new();
        let mut rts = LimitedQueue::new(cmp::min(100, data.len()));
        for src_row_data in data.iter() {
            let query_builder = RdbQueryBuilder::new_for_pg(tb_meta, None);
            let query_info = query_builder.get_select_query(src_row_data)?;
            let query = query_builder.create_pg_query(&query_info)?;

            let start_time = Instant::now();
            let mut rows = query.fetch(&self.conn_pool);
            rts.push((start_time.elapsed().as_millis() as u64, 1));

            if let Some(row) = rows.try_next().await.unwrap() {
                let dst_row_data = RowData::from_pg_row(&row, tb_meta, &None);
                let diff_col_values = BaseChecker::compare_row_data(src_row_data, &dst_row_data)?;
                if !diff_col_values.is_empty() {
                    let revise_sql = revise_ctx
                        .as_ref()
                        .map(|ctx| {
                            ctx.build_diff_sql(src_row_data, &dst_row_data, &diff_col_values)
                        })
                        .transpose()?
                        .flatten();

                    let diff_log = BaseChecker::build_diff_log(
                        src_row_data,
                        &dst_row_data,
                        diff_col_values,
                        &mut self.extractor_meta_manager,
                        &self.reverse_router,
                        self.output_full_row,
                    )
                    .await?;
                    if let Some(revise_sql) = revise_sql {
                        log_sql!("{}", revise_sql);
                    }
                    diff.push(diff_log);
                }
            } else {
                let revise_sql = revise_ctx
                    .as_ref()
                    .map(|ctx| ctx.build_miss_sql(src_row_data))
                    .transpose()?
                    .flatten();

                let miss_log = BaseChecker::build_miss_log(
                    src_row_data,
                    &mut self.extractor_meta_manager,
                    &self.reverse_router,
                    self.output_full_row,
                )
                .await?;
                if let Some(revise_sql) = revise_sql {
                    log_sql!("{}", revise_sql);
                }
                miss.push(miss_log);
            }
        }
        BaseChecker::log_dml(&miss, &diff);

        BaseSinker::update_serial_monitor(&self.monitor, data.len() as u64, 0).await?;
        BaseSinker::update_monitor_rt(&self.monitor, &rts).await
    }

    async fn batch_check(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let tb_meta = self.meta_manager.get_tb_meta_by_row_data(&data[0]).await?;
        let query_builder = RdbQueryBuilder::new_for_pg(tb_meta, None);

        // build fetch dst sql
        let query_info = query_builder.get_batch_select_query(data, start_index, batch_size)?;
        let query = query_builder.create_pg_query(&query_info)?;

        // fetch dst
        let mut dst_row_data_map = HashMap::new();
        let start_time = Instant::now();
        let mut rts = LimitedQueue::new(1);
        let mut rows = query.fetch(&self.conn_pool);
        rts.push((start_time.elapsed().as_millis() as u64, 1));

        while let Some(row) = rows.try_next().await.unwrap() {
            let row_data = RowData::from_pg_row(&row, tb_meta, &None);
            let hash_code = row_data.get_hash_code(&tb_meta.basic)?;
            dst_row_data_map.insert(hash_code, row_data);
        }

        let compare_range = BatchCompareRange {
            start_index,
            batch_size,
        };
        let revise_ctx = self
            .output_revise_sql
            .then(|| ReviseSqlContext::pg(tb_meta, self.revise_match_full_row));

        let ctx = BatchCompareContext {
            dst_tb_meta: &tb_meta.basic,
            extractor_meta_manager: &mut self.extractor_meta_manager,
            reverse_router: &self.reverse_router,
            output_full_row: self.output_full_row,
            revise_ctx: revise_ctx.as_ref(),
        };

        let (miss, diff, sql_count) =
            BaseChecker::batch_compare_row_data_items(data, &dst_row_data_map, compare_range, ctx)
                .await?;

        BaseChecker::log_dml(&miss, &diff);

        self.summary.end_time = chrono::Local::now().to_rfc3339();
        self.summary.miss_count += miss.len();
        self.summary.diff_count += diff.len();
        if sql_count > 0 {
            self.summary.sql_count = Some(self.summary.sql_count.unwrap_or(0) + sql_count);
        }

        BaseSinker::update_batch_monitor(&self.monitor, batch_size as u64, 0).await?;
        BaseSinker::update_monitor_rt(&self.monitor, &rts).await
    }

    async fn serial_check_struct(&mut self, mut data: Vec<StructData>) -> anyhow::Result<()> {
        for src_data in data.iter_mut() {
            let src_statement = &mut src_data.statement;
            let schema = match src_statement {
                StructStatement::PgCreateSchema(s) => s.schema.name.clone(),
                StructStatement::PgCreateTable(s) => s.table.schema_name.clone(),
                _ => String::new(),
            };

            let mut struct_fetcher = PgStructFetcher {
                conn_pool: self.conn_pool.to_owned(),
                schemas: HashSet::from([schema.clone()]),
                filter: None,
            };

            let mut dst_statement = match &src_statement {
                StructStatement::PgCreateSchema(_) => {
                    let mut dst_statements =
                        struct_fetcher.get_create_schema_statements(&schema).await?;
                    if dst_statements.is_empty() {
                        StructStatement::Unknown
                    } else {
                        StructStatement::PgCreateSchema(dst_statements.remove(0))
                    }
                }

                StructStatement::PgCreateTable(statement) => {
                    let mut dst_statements = struct_fetcher
                        .get_create_table_statements(&schema, &statement.table.table_name)
                        .await?;
                    if dst_statements.is_empty() {
                        StructStatement::Unknown
                    } else {
                        StructStatement::PgCreateTable(dst_statements.remove(0))
                    }
                }

                _ => StructStatement::Unknown,
            };

            let (miss_count, diff_count, extra_count) =
                BaseChecker::compare_struct(src_statement, &mut dst_statement, &self.filter)?;
            self.summary.miss_count += miss_count;
            self.summary.diff_count += diff_count;
            self.summary.extra_count += extra_count;
        }
        Ok(())
    }
}
