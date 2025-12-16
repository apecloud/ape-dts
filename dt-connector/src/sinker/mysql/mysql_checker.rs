use std::{
    cmp,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_mutex::Mutex;
use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{MySql, Pool};
use tokio::time::Instant;

use crate::{
    call_batch_fn,
    check_log::check_log::CheckSummaryLog,
    meta_fetcher::mysql::mysql_struct_fetcher::MysqlStructFetcher,
    rdb_query_builder::RdbQueryBuilder,
    rdb_router::RdbRouter,
    sinker::{
        base_checker::{
            BaseChecker, BatchCompareContext, BatchCompareRange, RecheckConfig, ReviseSqlContext,
        },
        base_sinker::BaseSinker,
    },
    Sinker,
};
use dt_common::{
    log_sql, log_summary,
    meta::{
        mysql::mysql_meta_manager::MysqlMetaManager,
        rdb_meta_manager::RdbMetaManager,
        row_data::RowData,
        struct_meta::{statement::struct_statement::StructStatement, struct_data::StructData},
    },
    monitor::monitor::Monitor,
    rdb_filter::RdbFilter,
    utils::limit_queue::LimitedQueue,
};

#[derive(Clone)]
pub struct MysqlChecker {
    pub conn_pool: Pool<MySql>,
    pub meta_manager: MysqlMetaManager,
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
impl Sinker for MysqlChecker {
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

impl MysqlChecker {
    async fn fetch_dst_struct_statement(
        &self,
        src_statement: &StructStatement,
    ) -> anyhow::Result<StructStatement> {
        let db = match src_statement {
            StructStatement::MysqlCreateDatabase(s) => s.database.name.clone(),
            StructStatement::MysqlCreateTable(s) => s.table.database_name.clone(),
            _ => return Ok(StructStatement::Unknown),
        };

        let mut struct_fetcher = MysqlStructFetcher {
            conn_pool: self.conn_pool.to_owned(),
            dbs: HashSet::from([db.clone()]),
            filter: None,
            meta_manager: self.meta_manager.clone(),
        };

        match src_statement {
            StructStatement::MysqlCreateDatabase(_) => {
                let mut statements = struct_fetcher.get_create_database_statements(&db).await?;
                if statements.is_empty() {
                    Ok(StructStatement::Unknown)
                } else {
                    Ok(StructStatement::MysqlCreateDatabase(statements.remove(0)))
                }
            }
            StructStatement::MysqlCreateTable(s) => {
                let mut statements = struct_fetcher
                    .get_create_table_statements(&s.table.database_name, &s.table.table_name)
                    .await?;
                if statements.is_empty() {
                    Ok(StructStatement::Unknown)
                } else {
                    Ok(StructStatement::MysqlCreateTable(statements.remove(0)))
                }
            }
            _ => Ok(StructStatement::Unknown),
        }
    }

    async fn serial_check(&mut self, data: Vec<RowData>) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let tb_meta = self.meta_manager.get_tb_meta_by_row_data(&data[0]).await?;
        let revise_ctx = self
            .output_revise_sql
            .then_some(ReviseSqlContext::mysql(tb_meta, self.revise_match_full_row));

        let recheck_config = RecheckConfig {
            delay_ms: self.recheck_interval_secs.saturating_mul(1000),
            times: self.recheck_attempts,
        };

        let mut miss = Vec::new();
        let mut diff = Vec::new();
        let mut rts = LimitedQueue::new(cmp::min(100, data.len()));
        for src_row_data in data.iter() {
            let query_builder = RdbQueryBuilder::new_for_mysql(tb_meta, None);
            let query_info = query_builder.get_select_query(src_row_data)?;
            let query = query_builder.create_mysql_query(&query_info)?;

            let start_time = Instant::now();
            let mut rows = query.fetch(&self.conn_pool);
            rts.push((start_time.elapsed().as_millis() as u64, 1));

            let dst_row = if let Some(row) = rows.try_next().await? {
                Some(RowData::from_mysql_row(&row, tb_meta, &None))
            } else {
                None
            };

            let (check_result, final_dst_row) = BaseChecker::check_row_with_retry(
                src_row_data,
                dst_row,
                recheck_config,
                |src_row| {
                    let pool = self.conn_pool.clone();
                    let tb_meta = tb_meta.clone();
                    let src_row = src_row.clone();
                    async move {
                        let qb = RdbQueryBuilder::new_for_mysql(&tb_meta, None);
                        let q_info = qb.get_select_query(&src_row)?;
                        let query = qb.create_mysql_query(&q_info)?;
                        let mut rows = query.fetch(&pool);
                        if let Some(row) = rows.try_next().await? {
                            let row_data = RowData::from_mysql_row(&row, &tb_meta, &None);
                            Ok(Some(row_data))
                        } else {
                            Ok(None)
                        }
                    }
                },
            )
            .await?;

            match check_result {
                crate::sinker::base_checker::CheckResult::Diff(diff_col_values) => {
                    let dst_row = final_dst_row
                        .as_ref()
                        .expect("diff result should have a dst row");

                    let revise_sql = revise_ctx
                        .as_ref()
                        .map(|ctx| ctx.build_diff_sql(src_row_data, dst_row, &diff_col_values))
                        .transpose()?
                        .flatten();

                    let diff_log = BaseChecker::build_diff_log(
                        src_row_data,
                        dst_row,
                        diff_col_values,
                        &mut self.extractor_meta_manager,
                        &self.reverse_router,
                        self.output_full_row,
                    )
                    .await?;
                    if let Some(revise_sql) = revise_sql {
                        log_sql!("{}", revise_sql);
                        self.summary.sql_count = Some(self.summary.sql_count.unwrap_or(0) + 1);
                    }
                    diff.push(diff_log);
                }
                crate::sinker::base_checker::CheckResult::Miss => {
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
                        self.summary.sql_count = Some(self.summary.sql_count.unwrap_or(0) + 1);
                    }
                    miss.push(miss_log);
                }
                crate::sinker::base_checker::CheckResult::Ok => {}
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
        let tb_meta_owned = self
            .meta_manager
            .get_tb_meta_by_row_data(&data[0])
            .await?
            .clone();
        let tb_meta = &tb_meta_owned;
        let query_builder = RdbQueryBuilder::new_for_mysql(tb_meta, None);

        // build fetch dst sql
        let query_info = query_builder.get_batch_select_query(data, start_index, batch_size)?;
        let query = query_builder.create_mysql_query(&query_info)?;

        // fetch dst
        let mut dst_row_data_map = HashMap::new();
        let start_time = Instant::now();
        let mut rts = LimitedQueue::new(1);
        let mut rows = query.fetch(&self.conn_pool);
        rts.push((start_time.elapsed().as_millis() as u64, 1));

        while let Some(row) = rows.try_next().await.unwrap() {
            let row_data = RowData::from_mysql_row(&row, tb_meta, &None);
            let hash_code = row_data.get_hash_code(&tb_meta.basic)?;
            dst_row_data_map.insert(hash_code, row_data);
        }

        let compare_range = BatchCompareRange {
            start_index,
            batch_size,
        };
        let revise_ctx = self
            .output_revise_sql
            .then(|| ReviseSqlContext::mysql(tb_meta, self.revise_match_full_row));

        let recheck_config = RecheckConfig {
            delay_ms: self.recheck_interval_secs.saturating_mul(1000),
            times: self.recheck_attempts,
        };

        let ctx = BatchCompareContext {
            dst_tb_meta: &tb_meta.basic,
            extractor_meta_manager: &mut self.extractor_meta_manager,
            reverse_router: &self.reverse_router,
            output_full_row: self.output_full_row,
            revise_ctx: revise_ctx.as_ref(),
        };

        let pool = self.conn_pool.clone();
        let fetch_tb_meta = tb_meta_owned.clone();
        let fetch_latest = move |_, src_row: &RowData| {
            let pool = pool.clone();
            let tb_meta = fetch_tb_meta.clone();
            let src_row = src_row.clone();
            async move {
                let qb = RdbQueryBuilder::new_for_mysql(&tb_meta, None);
                let q_info = qb.get_select_query(&src_row)?;
                let query = qb.create_mysql_query(&q_info)?;
                let mut rows = query.fetch(&pool);
                if let Some(row) = rows.try_next().await? {
                    let row_data = RowData::from_mysql_row(&row, &tb_meta, &None);
                    Ok(Some(row_data))
                } else {
                    Ok(None)
                }
            }
        };

        let (miss, diff, sql_count) = BaseChecker::batch_compare_row_data_items(
            data,
            dst_row_data_map,
            compare_range,
            ctx,
            recheck_config,
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

        BaseSinker::update_batch_monitor(&self.monitor, batch_size as u64, 0).await?;
        BaseSinker::update_monitor_rt(&self.monitor, &rts).await
    }

    async fn serial_check_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        let recheck_config = RecheckConfig {
            delay_ms: self.recheck_interval_secs.saturating_mul(1000),
            times: self.recheck_attempts,
        };

        let (miss_count, diff_count, extra_count, sql_count) =
            BaseChecker::check_struct_with_retry(
                data,
                recheck_config,
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
