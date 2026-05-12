use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use anyhow::bail;
use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{MySql, Pool};

use crate::{
    extractor::{
        base_extractor::{BaseExtractor, ExtractState},
        base_splitter::SnapshotChunk,
        extractor_monitor::ExtractorMonitor,
        mysql::mysql_snapshot_splitter::MySqlSnapshotSplitter,
        rdb_snapshot_extract_statement::{OrderKeyPredicateType, RdbSnapshotExtractStatement},
        resumer::recovery::Recovery,
        snapshot_dispatcher::SnapshotDispatcher,
    },
    Extractor,
};
use dt_common::utils::sql_util::MYSQL_ESCAPE;
use dt_common::{
    config::config_enums::{DbType, RdbParallelType},
    log_debug, log_info,
    meta::{
        adaptor::{mysql_col_value_convertor::MysqlColValueConvertor, sqlx_ext::SqlxMysqlExt},
        col_value::ColValue,
        dt_data::DtData,
        mysql::{
            mysql_col_type::MysqlColType, mysql_meta_manager::MysqlMetaManager,
            mysql_tb_meta::MysqlTbMeta,
        },
        order_key::OrderKey,
        position::Position,
        row_data::RowData,
    },
    quote_mysql,
    rdb_filter::RdbFilter,
    utils::serialize_util::SerializeUtil,
};

use quote_mysql as quote;

pub struct MysqlSnapshotExtractor {
    pub base_extractor: BaseExtractor,
    pub extract_state: ExtractState,
    pub conn_pool: Pool<MySql>,
    pub meta_manager: MysqlMetaManager,
    pub filter: RdbFilter,
    pub batch_size: usize,
    pub parallel_size: usize,
    pub parallel_type: RdbParallelType,
    pub sample_interval: u64,
    pub db_tbs: HashMap<String, Vec<String>>,
    pub partition_cols: HashMap<(String, String), String>,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

#[derive(Clone)]
struct MysqlSnapshotShared {
    conn_pool: Pool<MySql>,
    meta_manager: MysqlMetaManager,
    filter: RdbFilter,
    batch_size: usize,
    parallel_size: usize,
    parallel_type: RdbParallelType,
    sample_interval: u64,
    recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

struct MysqlTableWorker {
    shared: MysqlSnapshotShared,
    base_extractor: BaseExtractor,
    extract_state: ExtractState,
    db: String,
    tb: String,
    user_defined_partition_col: String,
}

struct ChunkExtractCtx {
    pub conn_pool: Pool<MySql>,
    pub tb_meta: MysqlTbMeta,
    pub partition_col: String,
    pub partition_col_type: MysqlColType,
    pub sql_le: String,
    pub sql_range: String,
    pub chunk: SnapshotChunk,
    pub ignore_cols: Option<HashSet<String>>,
    pub base_extractor: BaseExtractor,
    pub extract_state: ExtractState,
}

struct MysqlChunkDispatchState<'a> {
    base_extractor: BaseExtractor,
    table_extract_state: &'a mut ExtractState,
    splitter: MySqlSnapshotSplitter,
    pending_chunks: VecDeque<SnapshotChunk>,
    extract_cnt: u64,
}

#[derive(Debug)]
enum MysqlExtractMode {
    ScanAll,
    OrderedBatch,
    SplitChunk(MysqlSplitChunkExtractPlan),
}

#[derive(Debug)]
struct MysqlSplitChunkExtractPlan {
    splitter: MySqlSnapshotSplitter,
    initial_chunks: VecDeque<SnapshotChunk>,
    parallelism: usize,
}

#[async_trait]
impl Extractor for MysqlSnapshotExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        if self.parallel_size < 1 {
            bail!("parallel_size must be greater than 0");
        }

        let tables = self.collect_tables();
        log_info!(
            "MysqlSnapshotExtractor starts, tables: {}, parallel_type: {:?}, parallel_size: {}",
            tables.len(),
            self.parallel_type,
            self.parallel_size
        );
        let this = self.clone_for_dispatch();
        SnapshotDispatcher::dispatch_tables(
            tables,
            self.parallel_type.clone(),
            self.parallel_size,
            "mysql table worker",
            move |(db, tb)| {
                let this = this.clone_for_dispatch();
                async move {
                    let partition_col = this
                        .partition_cols
                        .get(&(db.clone(), tb.clone()))
                        .cloned()
                        .unwrap_or_default();
                    this.run_table_worker(db, tb, partition_col).await
                }
            },
        )
        .await?;

        self.base_extractor
            .wait_task_finish(&mut self.extract_state)
            .await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl MysqlSnapshotExtractor {
    fn shared(&self) -> MysqlSnapshotShared {
        MysqlSnapshotShared {
            conn_pool: self.conn_pool.clone(),
            meta_manager: self.meta_manager.clone(),
            filter: self.filter.clone(),
            batch_size: self.batch_size,
            parallel_size: self.parallel_size,
            parallel_type: self.parallel_type.clone(),
            sample_interval: self.sample_interval,
            recovery: self.recovery.clone(),
        }
    }

    fn collect_tables(&self) -> Vec<(String, String)> {
        let mut tables = Vec::new();
        for (db, tbs) in &self.db_tbs {
            for tb in tbs {
                tables.push((db.clone(), tb.clone()));
            }
        }
        tables
    }

    async fn run_table_worker(
        &self,
        db: String,
        tb: String,
        user_defined_partition_col: String,
    ) -> anyhow::Result<()> {
        let (extract_state, _guard) =
            SnapshotDispatcher::derive_table_extract_state(&self.extract_state, &db, &tb).await;

        let mut worker = MysqlTableWorker {
            shared: self.shared(),
            base_extractor: self.base_extractor.clone(),
            extract_state,
            db,
            tb,
            user_defined_partition_col,
        };
        let res = worker.extract_single_table().await;
        worker.extract_state.monitor.try_flush(true).await;
        res
    }

    fn clone_for_dispatch(&self) -> Self {
        Self {
            base_extractor: self.base_extractor.clone(),
            extract_state: SnapshotDispatcher::clone_extract_state(&self.extract_state),
            conn_pool: self.conn_pool.clone(),
            meta_manager: self.meta_manager.clone(),
            filter: self.filter.clone(),
            batch_size: self.batch_size,
            parallel_size: self.parallel_size,
            parallel_type: self.parallel_type.clone(),
            sample_interval: self.sample_interval,
            db_tbs: self.db_tbs.clone(),
            partition_cols: self.partition_cols.clone(),
            recovery: self.recovery.clone(),
        }
    }
}

impl MysqlTableWorker {
    async fn extract_single_table(&mut self) -> anyhow::Result<()> {
        log_info!(
            "MysqlSnapshotExtractor starts, schema: {}, tb: {}, batch_size: {}, parallel_size: {}",
            quote!(&self.db),
            quote!(&self.tb),
            self.shared.batch_size,
            self.shared.parallel_size
        );

        let tb_meta = self
            .shared
            .meta_manager
            .get_tb_meta(&self.db, &self.tb)
            .await?
            .to_owned();
        let extract_mode = self.prepare_extract_mode(&tb_meta).await?;
        log_debug!(
            "prepared extract mode for {}.{}: {:?}",
            quote!(&self.db),
            quote!(&self.tb),
            extract_mode
        );
        let extracted_count = self.execute_extract_mode(&tb_meta, extract_mode).await?;

        log_info!(
            "end extracting data from {}.{}, all count: {}",
            quote!(&self.db),
            quote!(&self.tb),
            extracted_count
        );

        self.base_extractor.push_snapshot_finished(
            &self.db,
            &self.tb,
            Position::RdbSnapshotFinished {
                db_type: DbType::Mysql.to_string(),
                schema: self.db.clone(),
                tb: self.tb.clone(),
            },
        )?;
        Ok(())
    }

    async fn prepare_extract_mode<'a>(
        &self,
        tb_meta: &'a MysqlTbMeta,
    ) -> anyhow::Result<MysqlExtractMode> {
        if matches!(self.shared.parallel_type, RdbParallelType::Chunk) {
            return self
                .prepare_splitter_extract_mode(tb_meta, self.shared.parallel_size)
                .await;
        }
        if self.should_use_splitter_for_table_extract(tb_meta) {
            return self.prepare_splitter_extract_mode(tb_meta, 1).await;
        }
        if tb_meta.basic.order_cols.is_empty() {
            Ok(MysqlExtractMode::ScanAll)
        } else {
            Ok(MysqlExtractMode::OrderedBatch)
        }
    }

    async fn prepare_splitter_extract_mode<'a>(
        &self,
        tb_meta: &'a MysqlTbMeta,
        parallelism: usize,
    ) -> anyhow::Result<MysqlExtractMode> {
        let mut splitter = self.build_splitter(tb_meta)?;
        let partition_col = splitter.get_partition_col();
        let resume_values = self
            .get_resume_values(tb_meta, &[partition_col], true)
            .await?;
        splitter.init(&resume_values)?;
        let initial_chunks = VecDeque::from(splitter.get_next_chunks().await?);

        if Self::is_no_split_chunks(&initial_chunks) {
            log_info!(
                "table {}.{} has no split chunk, extracting by single batch extractor",
                quote!(&self.db),
                quote!(&self.tb)
            );
            return Ok(self.fallback_extract_mode(tb_meta));
        }

        Ok(MysqlExtractMode::SplitChunk(MysqlSplitChunkExtractPlan {
            splitter,
            initial_chunks,
            parallelism,
        }))
    }

    fn fallback_extract_mode<'a>(&self, tb_meta: &'a MysqlTbMeta) -> MysqlExtractMode {
        if tb_meta.basic.order_cols.is_empty() {
            MysqlExtractMode::ScanAll
        } else {
            MysqlExtractMode::OrderedBatch
        }
    }

    async fn execute_extract_mode(
        &mut self,
        tb_meta: &MysqlTbMeta,
        extract_mode: MysqlExtractMode,
    ) -> anyhow::Result<u64> {
        match extract_mode {
            MysqlExtractMode::ScanAll => self.extract_all(tb_meta).await,
            MysqlExtractMode::OrderedBatch => self.extract_by_batch(tb_meta).await,
            MysqlExtractMode::SplitChunk(MysqlSplitChunkExtractPlan {
                splitter,
                initial_chunks,
                parallelism,
            }) => {
                self.extract_by_splitter(tb_meta, splitter, initial_chunks, parallelism)
                    .await
            }
        }
    }

    fn is_no_split_chunks(chunks: &VecDeque<SnapshotChunk>) -> bool {
        if chunks.len() != 1 {
            return false;
        }
        chunks
            .front()
            .map(|chunk| matches!(&chunk.chunk_range, (ColValue::None, ColValue::None)))
            .unwrap_or_default()
    }

    fn build_splitter(&self, tb_meta: &MysqlTbMeta) -> anyhow::Result<MySqlSnapshotSplitter> {
        let user_defined_partition_col = &self.user_defined_partition_col;
        self.validate_user_defined(tb_meta, user_defined_partition_col)?;
        Ok(MySqlSnapshotSplitter::new(
            Arc::new(tb_meta.clone()),
            self.shared.conn_pool.clone(),
            self.shared.batch_size,
            if !user_defined_partition_col.is_empty() {
                user_defined_partition_col.clone()
            } else {
                tb_meta.basic.partition_col.clone()
            },
        ))
    }

    fn should_use_splitter_for_table_extract(&self, tb_meta: &MysqlTbMeta) -> bool {
        // Splitter is required when the table has no order cols or when the user
        // explicitly forces a partition column.
        !self.user_defined_partition_col.is_empty() || tb_meta.basic.order_cols.is_empty()
    }

    async fn extract_all(&mut self, tb_meta: &MysqlTbMeta) -> anyhow::Result<u64> {
        log_info!(
            "start extracting data from {}.{} without batch",
            quote!(&self.db),
            quote!(&self.tb)
        );

        let base_count = self.extract_state.monitor.counters.pushed_record_count;
        let ignore_cols = self.shared.filter.get_ignore_cols(&self.db, &self.tb);
        let where_condition = self
            .shared
            .filter
            .get_where_condition(&self.db, &self.tb)
            .cloned()
            .unwrap_or_default();
        let sql = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.unwrap_or(&HashSet::new()))
            .with_where_condition(&where_condition)
            .build()?;

        let mut rows = sqlx::query(&sql).fetch(&self.shared.conn_pool);
        while let Some(row) = rows.try_next().await? {
            let row_data = RowData::from_mysql_row(&row, tb_meta, &ignore_cols);
            self.base_extractor
                .push_row(&mut self.extract_state, row_data, Position::None)
                .await?;
        }
        Ok(self.extract_state.monitor.counters.pushed_record_count - base_count)
    }

    async fn extract_by_batch(&mut self, tb_meta: &MysqlTbMeta) -> anyhow::Result<u64> {
        let mut resume_values = self
            .get_resume_values(tb_meta, &tb_meta.basic.order_cols, false)
            .await?;
        let mut start_from_beginning = false;
        if resume_values.is_empty() {
            resume_values = tb_meta.basic.get_default_order_col_values();
            start_from_beginning = true;
        }
        let mut extracted_count = 0u64;
        let mut start_values = resume_values;
        let ignore_cols = self.shared.filter.get_ignore_cols(&self.db, &self.tb);
        let where_condition = self
            .shared
            .filter
            .get_where_condition(&self.db, &self.tb)
            .cloned()
            .unwrap_or_default();
        let sql_from_beginning = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.unwrap_or(&HashSet::new()))
            .with_order_cols(&tb_meta.basic.order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::None)
            .with_limit(self.shared.batch_size)
            .build()?;
        let sql_from_value = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.unwrap_or(&HashSet::new()))
            .with_order_cols(&tb_meta.basic.order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::GreaterThan)
            .with_limit(self.shared.batch_size)
            .build()?;

        loop {
            let bind_values = start_values.clone();
            let query = if start_from_beginning {
                start_from_beginning = false;
                sqlx::query(&sql_from_beginning)
            } else {
                let mut query = sqlx::query(&sql_from_value);
                for order_col in &tb_meta.basic.order_cols {
                    let order_col_type = tb_meta.get_col_type(order_col)?;
                    query = query.bind_col_value(bind_values.get(order_col), order_col_type)
                }
                query
            };

            let mut rows = query.fetch(&self.shared.conn_pool);
            let mut slice_count = 0usize;

            while let Some(row) = rows.try_next().await? {
                for order_col in &tb_meta.basic.order_cols {
                    let order_col_type = tb_meta.get_col_type(order_col)?;
                    if let Some(value) = start_values.get_mut(order_col) {
                        *value =
                            MysqlColValueConvertor::from_query(&row, order_col, order_col_type)?;
                    } else {
                        bail!(
                            "{}.{} order col {} not found",
                            quote!(&self.db),
                            quote!(&self.tb),
                            quote!(order_col),
                        );
                    }
                }
                extracted_count += 1;
                slice_count += 1;
                if extracted_count % self.shared.sample_interval != 0 {
                    continue;
                }

                let row_data = RowData::from_mysql_row(&row, tb_meta, &ignore_cols);
                let position = tb_meta.basic.build_position(&DbType::Mysql, &start_values);
                self.base_extractor
                    .push_row(&mut self.extract_state, row_data, position)
                    .await?;
            }

            if slice_count < self.shared.batch_size {
                break;
            }
        }

        if tb_meta
            .basic
            .order_cols
            .iter()
            .any(|col| tb_meta.basic.is_col_nullable(col))
        {
            extracted_count += self
                .extract_nulls(tb_meta, &tb_meta.basic.order_cols)
                .await?;
        }

        Ok(extracted_count)
    }

    async fn extract_nulls(
        &mut self,
        tb_meta: &MysqlTbMeta,
        order_cols: &Vec<String>,
    ) -> anyhow::Result<u64> {
        let mut extracted_count = 0u64;
        let ignore_cols = self.shared.filter.get_ignore_cols(&self.db, &self.tb);
        let where_condition = self
            .shared
            .filter
            .get_where_condition(&self.db, &self.tb)
            .cloned()
            .unwrap_or_default();
        let sql_for_null = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.unwrap_or(&HashSet::new()))
            .with_order_cols(order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::IsNull)
            .build()?;

        let mut rows = sqlx::query(&sql_for_null).fetch(&self.shared.conn_pool);
        while let Some(row) = rows.try_next().await? {
            extracted_count += 1;
            let row_data = RowData::from_mysql_row(&row, tb_meta, &ignore_cols);
            self.base_extractor
                .push_row(&mut self.extract_state, row_data, Position::None)
                .await?;
        }
        Ok(extracted_count)
    }

    async fn extract_by_splitter(
        &mut self,
        tb_meta: &MysqlTbMeta,
        splitter: MySqlSnapshotSplitter,
        initial_chunks: VecDeque<SnapshotChunk>,
        parallelism: usize,
    ) -> anyhow::Result<u64> {
        log_info!("extracting with splitter, parallelism: {}", parallelism);
        let order_cols = vec![splitter.get_partition_col()];
        let partition_col = &order_cols[0];
        let partition_col_type = tb_meta.get_col_type(partition_col)?;

        let mut extract_cnt = 0u64;
        let monitor_handle = self.extract_state.monitor.monitor.clone();
        let task_id = self.extract_state.monitor.default_task_id.clone();
        let data_marker = self.extract_state.data_marker.clone();
        let time_filter = self.extract_state.time_filter.clone();
        let conn_pool = self.shared.conn_pool.clone();
        let base_extractor = self.base_extractor.clone();
        let ignore_cols = self
            .shared
            .filter
            .get_ignore_cols(&self.db, &self.tb)
            .cloned();
        let where_condition = self
            .shared
            .filter
            .get_where_condition(&self.db, &self.tb)
            .cloned()
            .unwrap_or_default();
        let sql_le = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.as_ref().unwrap_or(&HashSet::new()))
            .with_order_cols(&order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::LessThanOrEqual)
            .build()?;
        let sql_range = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.as_ref().unwrap_or(&HashSet::new()))
            .with_order_cols(&order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::Range)
            .build()?;

        extract_cnt += self
            .dispatch_chunk_extract(
                tb_meta.clone(),
                splitter,
                initial_chunks,
                parallelism,
                partition_col.clone(),
                partition_col_type.clone(),
                sql_le,
                sql_range,
                ignore_cols,
                conn_pool,
                base_extractor,
                monitor_handle,
                task_id,
                data_marker,
                time_filter,
            )
            .await?;

        if tb_meta.basic.is_col_nullable(partition_col) {
            extract_cnt += self.extract_nulls(tb_meta, &order_cols).await?;
        }
        Ok(extract_cnt)
    }

    #[allow(clippy::too_many_arguments)]
    async fn dispatch_chunk_extract(
        &mut self,
        tb_meta: MysqlTbMeta,
        splitter: MySqlSnapshotSplitter,
        pending_chunks: VecDeque<SnapshotChunk>,
        parallelism: usize,
        partition_col: String,
        partition_col_type: MysqlColType,
        sql_le: String,
        sql_range: String,
        ignore_cols: Option<HashSet<String>>,
        conn_pool: Pool<MySql>,
        base_extractor: BaseExtractor,
        monitor_handle: dt_common::monitor::task_monitor::TaskMonitorHandle,
        task_id: String,
        data_marker: Option<crate::data_marker::DataMarker>,
        time_filter: dt_common::time_filter::TimeFilter,
    ) -> anyhow::Result<u64> {
        let base_extractor_for_run = base_extractor.clone();
        let state = MysqlChunkDispatchState {
            base_extractor,
            table_extract_state: &mut self.extract_state,
            splitter,
            pending_chunks,
            extract_cnt: 0,
        };

        let state = SnapshotDispatcher::dispatch_work_source(
            state,
            parallelism,
            "mysql chunk worker",
            |mut state: MysqlChunkDispatchState<'_>| async move {
                if state.pending_chunks.is_empty() {
                    state
                        .pending_chunks
                        .extend(state.splitter.get_next_chunks().await?);
                }
                let work = state.pending_chunks.pop_front();
                Ok((state, work))
            },
            move |chunk| {
                let conn_pool = conn_pool.clone();
                let tb_meta = tb_meta.clone();
                let partition_col = partition_col.clone();
                let partition_col_type = partition_col_type.clone();
                let sql_le = sql_le.clone();
                let sql_range = sql_range.clone();
                let ignore_cols = ignore_cols.clone();
                let base_extractor = base_extractor_for_run.clone();
                let monitor_handle = monitor_handle.clone();
                let task_id = task_id.clone();
                let data_marker = data_marker.clone();
                let time_filter = time_filter.clone();
                async move {
                    Self::extract_chunk(ChunkExtractCtx {
                        conn_pool,
                        tb_meta,
                        partition_col,
                        partition_col_type,
                        sql_le,
                        sql_range,
                        chunk,
                        ignore_cols,
                        base_extractor,
                        extract_state: ExtractState {
                            monitor: ExtractorMonitor::new(monitor_handle, task_id).await,
                            data_marker,
                            time_filter,
                        },
                    })
                    .await
                }
            },
            move |state, result| async move {
                let MysqlChunkDispatchState {
                    base_extractor,
                    table_extract_state,
                    mut splitter,
                    pending_chunks,
                    mut extract_cnt,
                } = state;
                let (chunk_id, cnt, partition_col_value) = result;
                if let Some(position) =
                    splitter.get_next_checkpoint_position(chunk_id, partition_col_value)
                {
                    let commit = DtData::Commit { xid: String::new() };
                    base_extractor
                        .push_dt_data(table_extract_state, commit, position)
                        .await?;
                }
                extract_cnt += cnt;
                Ok(MysqlChunkDispatchState {
                    base_extractor,
                    table_extract_state,
                    splitter,
                    pending_chunks,
                    extract_cnt,
                })
            },
        )
        .await?;

        Ok(state.extract_cnt)
    }

    async fn extract_chunk(extract_ctx: ChunkExtractCtx) -> anyhow::Result<(u64, u64, ColValue)> {
        let conn_pool = extract_ctx.conn_pool;
        let tb_meta = extract_ctx.tb_meta;
        let partition_col = extract_ctx.partition_col;
        let partition_col_type = extract_ctx.partition_col_type;
        let sql_le = extract_ctx.sql_le;
        let sql_range = extract_ctx.sql_range;
        let chunk = extract_ctx.chunk;
        let ignore_cols = extract_ctx.ignore_cols;
        let base_extractor = extract_ctx.base_extractor;
        let mut extract_state = extract_ctx.extract_state;

        log_debug!(
            "extract by partition_col: {}, chunk range: {:?}",
            quote!(partition_col),
            chunk
        );
        let chunk_id = chunk.chunk_id;
        let (start_value, end_value) = chunk.chunk_range;
        let query = match (&start_value, &end_value) {
            (ColValue::None, ColValue::None) | (_, ColValue::None) => {
                bail!(
                    "chunk {} has bad chunk range from {}.{}",
                    chunk_id,
                    quote!(&tb_meta.basic.schema),
                    quote!(&tb_meta.basic.tb)
                );
            }
            (ColValue::None, _) => {
                sqlx::query(&sql_le).bind_col_value(Some(&end_value), &partition_col_type)
            }
            _ => sqlx::query(&sql_range)
                .bind_col_value(Some(&start_value), &partition_col_type)
                .bind_col_value(Some(&end_value), &partition_col_type),
        };

        let mut extracted_cnt = 0u64;
        let mut partition_col_value = ColValue::None;
        let mut rows = query.fetch(&conn_pool);
        while let Some(row) = rows.try_next().await? {
            partition_col_value =
                MysqlColValueConvertor::from_query(&row, &partition_col, &partition_col_type)?;
            let row_data = RowData::from_mysql_row(&row, &tb_meta, &ignore_cols.as_ref());
            base_extractor
                .push_row(&mut extract_state, row_data, Position::None)
                .await?;
            extracted_cnt += 1;
        }
        extract_state.monitor.try_flush(true).await;
        Ok((chunk_id, extracted_cnt, partition_col_value))
    }

    fn validate_user_defined(
        &self,
        tb_meta: &MysqlTbMeta,
        user_defined_partition_col: &String,
    ) -> anyhow::Result<()> {
        if user_defined_partition_col.is_empty() {
            return Ok(());
        }
        if tb_meta.basic.has_col(user_defined_partition_col) {
            return Ok(());
        }
        bail!(
            "user defined partition col {} not in cols of {}.{}",
            quote!(user_defined_partition_col),
            quote!(&tb_meta.basic.schema),
            quote!(&tb_meta.basic.tb),
        );
    }

    async fn get_resume_values(
        &self,
        tb_meta: &MysqlTbMeta,
        order_cols: &[String],
        check_point: bool,
    ) -> anyhow::Result<HashMap<String, ColValue>> {
        let mut resume_values: HashMap<String, ColValue> = HashMap::new();
        if let Some(handler) = &self.shared.recovery {
            if let Some(Position::RdbSnapshot {
                schema,
                tb,
                order_key: Some(order_key),
                ..
            }) = handler
                .get_snapshot_resume_position(&self.db, &self.tb, check_point)
                .await
            {
                if schema != self.db || tb != self.tb {
                    log_info!(
                        r#"{}.{} resume position db/tb not match, ignore it"#,
                        quote!(&self.db),
                        quote!(&self.tb)
                    );
                    return Ok(HashMap::new());
                }
                let order_col_values = match order_key {
                    OrderKey::Single((order_col, value)) => vec![(order_col, value)],
                    OrderKey::Composite(values) => values,
                };
                if order_col_values.len() != order_cols.len() {
                    log_info!(
                        r#"{}.{} resume values not match order cols in length"#,
                        quote!(&self.db),
                        quote!(&self.tb)
                    );
                    return Ok(HashMap::new());
                }
                for ((position_order_col, value), order_col) in
                    order_col_values.into_iter().zip(order_cols.iter())
                {
                    if position_order_col != *order_col {
                        log_info!(
                            r#"{}.{} resume position order col {} not match {}"#,
                            quote!(&self.db),
                            quote!(&self.tb),
                            position_order_col,
                            order_col
                        );
                        return Ok(HashMap::new());
                    }
                    let col_value = match value {
                        Some(v) => {
                            MysqlColValueConvertor::from_str(tb_meta.get_col_type(order_col)?, &v)?
                        }
                        None => ColValue::None,
                    };
                    resume_values.insert(position_order_col, col_value);
                }
            } else {
                log_info!(r#"`{}`.`{}` has no resume position"#, self.db, self.tb);
                return Ok(HashMap::new());
            }
        }
        log_info!(
            r#"[{}.{}] recovery from [{}]"#,
            quote!(&self.db),
            quote!(&self.tb),
            SerializeUtil::serialize_hashmap_to_json(&resume_values)?
        );
        Ok(resume_values)
    }
}
