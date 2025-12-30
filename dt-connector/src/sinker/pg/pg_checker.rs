use anyhow::Context;
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
        base_checker::{BaseChecker, CheckInconsistency, CheckItemContext, ReviseSqlContext},
        base_sinker::BaseSinker,
    },
    Sinker,
};
use dt_common::{
    log_summary,
    meta::{
        pg::pg_meta_manager::PgMetaManager,
        rdb_meta_manager::RdbMetaManager,
        row_data::RowData,
        row_type::RowType,
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
    pub recheck_interval_secs: u64,
    pub recheck_attempts: u32,
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
    async fn fetch_dst_struct_statement(
        &self,
        src_statement: &StructStatement,
    ) -> anyhow::Result<StructStatement> {
        let schema = match src_statement {
            StructStatement::PgCreateSchema(s) => s.schema.name.clone(),
            StructStatement::PgCreateTable(s) => s.table.schema_name.clone(),
            _ => return Ok(StructStatement::Unknown),
        };

        let mut struct_fetcher = PgStructFetcher {
            conn_pool: self.conn_pool.to_owned(),
            schemas: HashSet::from([schema.clone()]),
            filter: None,
        };

        match src_statement {
            StructStatement::PgCreateSchema(_) => {
                let mut statements = struct_fetcher.get_create_schema_statements(&schema).await?;
                if statements.is_empty() {
                    Ok(StructStatement::Unknown)
                } else {
                    Ok(StructStatement::PgCreateSchema(statements.remove(0)))
                }
            }
            StructStatement::PgCreateTable(statement) => {
                let mut statements = struct_fetcher
                    .get_create_table_statements(&schema, &statement.table.table_name)
                    .await?;
                if statements.is_empty() {
                    Ok(StructStatement::Unknown)
                } else {
                    Ok(StructStatement::PgCreateTable(statements.remove(0)))
                }
            }
            _ => Ok(StructStatement::Unknown),
        }
    }

    async fn serial_check(&mut self, data: Vec<RowData>) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let tb_meta = Arc::new(self.meta_manager.get_tb_meta_by_row_data(&data[0]).await?);
        let revise_ctx = self
            .output_revise_sql
            .then(|| ReviseSqlContext::pg(tb_meta.as_ref(), self.revise_match_full_row));

        let recheck_delay_secs = self.recheck_interval_secs;
        let recheck_attempts = self.recheck_attempts;

        let mut miss = Vec::new();
        let mut diff = Vec::new();
        let mut sql_count = 0;
        let mut rts = LimitedQueue::new(cmp::min(100, data.len()));
        let pool = self.conn_pool.clone();
        let tb_meta_cloned = tb_meta.clone();
        let fetch_latest = move |src_row: &RowData| {
            let pool = pool.clone();
            let tb_meta = tb_meta_cloned.clone();
            let src_row = src_row.clone();

            async move {
                let qb = RdbQueryBuilder::new_for_pg(&tb_meta, None);
                let q_info = qb.get_select_query(&src_row)?;
                let query = qb.create_pg_query(&q_info)?;
                let mut rows = query.fetch(&pool);
                if let Some(row) = rows.try_next().await? {
                    let row_data = RowData::from_pg_row(&row, &tb_meta, &None);
                    Ok(Some(row_data))
                } else {
                    Ok(None)
                }
            }
        };

        let mut ctx = CheckItemContext {
            extractor_meta_manager: Some(&mut self.extractor_meta_manager),
            reverse_router: &self.reverse_router,
            output_full_row: self.output_full_row,
            revise_ctx: revise_ctx.as_ref(),
            tb_meta: &tb_meta.basic,
        };

        for src_row_data in data.iter() {
            let query_builder = RdbQueryBuilder::new_for_pg(&tb_meta, None);
            let query_info = query_builder.get_select_query(src_row_data)?;
            let query = query_builder.create_pg_query(&query_info)?;

            let start_time = Instant::now();
            let mut rows = query.fetch(&self.conn_pool);
            rts.push((start_time.elapsed().as_millis() as u64, 1));

            let dst_row = if let Some(row) = rows.try_next().await? {
                let row_data = RowData::from_pg_row(&row, &tb_meta, &None);
                Some(row_data)
            } else {
                None
            };

            let (check_result, final_dst_row, item_sql_count) =
                BaseChecker::check_and_process_item(
                    src_row_data,
                    dst_row,
                    recheck_delay_secs,
                    recheck_attempts,
                    &fetch_latest,
                    &mut ctx,
                )
                .await?;

            sql_count += item_sql_count;
            if let Some(res) = check_result {
                match res {
                    CheckInconsistency::Miss => {
                        let log = BaseChecker::build_any_miss_log(src_row_data, &mut ctx).await?;
                        miss.push(log);
                    }
                    CheckInconsistency::Diff(diff_col_values) => {
                        let dst_row = final_dst_row.as_ref().context("missing dst row in diff")?;
                        let log = BaseChecker::build_any_diff_log(
                            src_row_data,
                            dst_row,
                            diff_col_values,
                            &mut ctx,
                        )
                        .await?;
                        diff.push(log);
                    }
                }
            }
        }
        BaseChecker::log_dml(&miss, &diff);

        self.summary.miss_count += miss.len();
        self.summary.diff_count += diff.len();
        if sql_count > 0 {
            self.summary.sql_count = Some(self.summary.sql_count.unwrap_or(0) + sql_count);
        }

        BaseSinker::update_serial_monitor(&self.monitor, data.len() as u64, 0).await?;
        BaseSinker::update_monitor_rt(&self.monitor, &rts).await
    }

    async fn batch_check(
        &mut self,
        data: &mut [RowData],
        start: usize,
        batch: usize,
    ) -> anyhow::Result<()> {
        let tb_meta = Arc::new(
            self.meta_manager
                .get_tb_meta_by_row_data(&data[0])
                .await?
                .clone(),
        );
        let tb_meta = &tb_meta;
        let query_builder = RdbQueryBuilder::new_for_pg(tb_meta, None);

        // build fetch dst sql
        let query_info = query_builder.get_batch_select_query(data, start, batch)?;
        let query = query_builder.create_pg_query(&query_info)?;

        // fetch dst
        let mut dst_row_data_map = HashMap::new();
        let start_time = Instant::now();
        let mut rts = LimitedQueue::new(1);
        let mut rows = query.fetch(&self.conn_pool);
        rts.push((start_time.elapsed().as_millis() as u64, 1));

        while let Some(row) = rows.try_next().await? {
            let row_data = RowData::from_pg_row(&row, tb_meta, &None);
            let hash_code = row_data.get_hash_code(&tb_meta.basic)?;
            dst_row_data_map.insert(hash_code, row_data);
        }

        let revise_ctx = self
            .output_revise_sql
            .then(|| ReviseSqlContext::pg(tb_meta.as_ref(), self.revise_match_full_row));

        let recheck_delay_secs = self.recheck_interval_secs;
        let recheck_attempts = self.recheck_attempts;

        let ctx = CheckItemContext {
            extractor_meta_manager: Some(&mut self.extractor_meta_manager),
            reverse_router: &self.reverse_router,
            output_full_row: self.output_full_row,
            revise_ctx: revise_ctx.as_ref(),
            tb_meta: &tb_meta.basic,
        };

        let pool = self.conn_pool.clone();
        let tb_meta_cloned = tb_meta.clone();
        let schema = data[0].schema.clone();
        let tb = data[0].tb.clone();
        let fetch_latest = move |_, src_row: &RowData| {
            let pool = pool.clone();
            let tb_meta = tb_meta_cloned.clone();
            let schema = schema.clone();
            let tb = tb.clone();
            let row_type = src_row.row_type.clone();
            let col_values = match &row_type {
                RowType::Insert | RowType::Update => src_row.after.clone(),
                RowType::Delete => src_row.before.clone(),
            };
            async move {
                let (before, after) = match &row_type {
                    RowType::Insert | RowType::Update => (None, col_values),
                    RowType::Delete => (col_values, None),
                };
                let src_row = RowData::new(schema, tb, row_type, before, after);

                let qb = RdbQueryBuilder::new_for_pg(&tb_meta, None);
                let q_info = qb.get_select_query(&src_row)?;
                let query = qb.create_pg_query(&q_info)?;
                let mut rows = query.fetch(&pool);
                if let Some(row) = rows.try_next().await? {
                    let row_data = RowData::from_pg_row(&row, &tb_meta, &None);
                    Ok(Some(row_data))
                } else {
                    Ok(None)
                }
            }
        };

        let dst_tb_meta_for_key = &tb_meta.basic;
        let (miss, diff, sql_count) = BaseChecker::batch_compare_row_data_items(
            &data[start..((start + batch).min(data.len()))],
            dst_row_data_map,
            ctx,
            recheck_delay_secs,
            recheck_attempts,
            |r| r.get_hash_code(dst_tb_meta_for_key),
            fetch_latest,
        )
        .await?;

        BaseChecker::log_dml(&miss, &diff);

        self.summary.end_time = chrono::Local::now().to_rfc3339();
        self.summary.miss_count += miss.len();
        self.summary.diff_count += diff.len();
        if sql_count > 0 {
            self.summary.sql_count = Some(self.summary.sql_count.unwrap_or(0) + sql_count);
        }

        BaseSinker::update_batch_monitor(&self.monitor, batch as u64, 0).await?;
        BaseSinker::update_monitor_rt(&self.monitor, &rts).await
    }

    async fn serial_check_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        let recheck_delay_secs = self.recheck_interval_secs;
        let recheck_attempts = self.recheck_attempts;

        let (miss_count, diff_count, extra_count, sql_count) =
            BaseChecker::check_struct_with_retry(
                data,
                recheck_delay_secs,
                recheck_attempts,
                &self.filter,
                self.output_revise_sql,
                |src_statement| {
                    let this = self.clone();
                    async move { this.fetch_dst_struct_statement(&src_statement).await }
                },
            )
            .await?;

        self.summary.miss_count += miss_count;
        self.summary.diff_count += diff_count;
        self.summary.extra_count += extra_count;
        if sql_count > 0 {
            self.summary.sql_count = Some(self.summary.sql_count.unwrap_or(0) + sql_count);
        }

        Ok(())
    }
}
