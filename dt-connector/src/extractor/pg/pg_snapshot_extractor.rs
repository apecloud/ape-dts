use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{Pool, Postgres};

use crate::{
    extractor::{
        base_extractor::{BaseExtractor, ExtractState},
        base_splitter::SnapshotChunk,
        pg::pg_snapshot_splitter::PgSnapshotSplitter,
        rdb_snapshot_extract_statement::{OrderKeyPredicateType, RdbSnapshotExtractStatement},
        resumer::recovery::Recovery,
        snapshot_dispatcher::{SnapshotDispatcher, TableMonitorGuard},
        snapshot_types::SnapshotTableId,
    },
    Extractor,
};
use dt_common::utils::sql_util::PG_ESCAPE;
use dt_common::{
    config::config_enums::{DbType, RdbParallelType},
    log_debug, log_info,
    meta::{
        adaptor::{pg_col_value_convertor::PgColValueConvertor, sqlx_ext::SqlxPgExt},
        col_value::ColValue,
        dt_data::DtData,
        order_key::OrderKey,
        pg::{pg_col_type::PgColType, pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
        position::Position,
        row_data::RowData,
    },
    quote_pg,
    rdb_filter::RdbFilter,
    utils::serialize_util::SerializeUtil,
};

use quote_pg as quote;

pub struct PgSnapshotExtractor {
    pub base_extractor: BaseExtractor,
    pub extract_state: ExtractState,
    pub conn_pool: Pool<Postgres>,
    pub meta_manager: PgMetaManager,
    pub filter: RdbFilter,
    pub batch_size: usize,
    pub parallel_size: usize,
    pub sample_interval: u64,
    pub schema_tbs: HashMap<String, Vec<String>>,
    pub parallel_type: RdbParallelType,
    pub partition_cols: HashMap<(String, String), String>,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

#[derive(Clone)]
struct PgSnapshotShared {
    base_extractor: BaseExtractor,
    conn_pool: Pool<Postgres>,
    meta_manager: PgMetaManager,
    filter: RdbFilter,
    batch_size: usize,
    parallel_type: RdbParallelType,
    sample_interval: u64,
    recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

#[derive(Clone)]
struct PgTableCtx {
    shared: PgSnapshotShared,
    table_id: SnapshotTableId,
    user_defined_partition_col: String,
}

struct PgTableWorker {
    ctx: PgTableCtx,
    extract_state: ExtractState,
}

struct PgSnapshotDispatchState {
    shared: PgSnapshotShared,
    root_extract_state: ExtractState,
    pending_tables: VecDeque<SnapshotTableId>,
    pending_works: VecDeque<PgSnapshotWork>,
    active_tables: HashMap<SnapshotTableId, PgActiveTable>,
    partition_cols: HashMap<(String, String), String>,
}

struct PgActiveTable {
    ctx: PgTableCtx,
    extract_state: ExtractState,
    _monitor_guard: TableMonitorGuard,
    tb_meta: PgTbMeta,
    extracted_count: u64,
    mode: PgActiveTableMode,
}

enum PgActiveTableMode {
    ScanAll,
    OrderedBatch,
    SplitChunk {
        splitter: PgSnapshotSplitter,
        initial_chunks: VecDeque<SnapshotChunk>,
        queued_chunks: usize,
        running_chunks: usize,
        partition_col: String,
        partition_col_type: PgColType,
        sql_le: String,
        sql_range: String,
        ignore_cols: Option<HashSet<String>>,
    },
}

enum PgSnapshotWork {
    ScanAll {
        table_id: SnapshotTableId,
        ctx: PgTableCtx,
        extract_state: ExtractState,
        tb_meta: PgTbMeta,
    },
    OrderedBatch {
        table_id: SnapshotTableId,
        ctx: PgTableCtx,
        extract_state: ExtractState,
        tb_meta: PgTbMeta,
    },
    SplitChunk {
        table_id: SnapshotTableId,
        shared: PgSnapshotShared,
        conn_pool: Pool<Postgres>,
        tb_meta: PgTbMeta,
        partition_col: String,
        partition_col_type: PgColType,
        sql_le: String,
        sql_range: String,
        chunk: SnapshotChunk,
        ignore_cols: Option<HashSet<String>>,
        extract_state: ExtractState,
    },
    ExtractNulls {
        table_id: SnapshotTableId,
        ctx: PgTableCtx,
        extract_state: ExtractState,
        tb_meta: PgTbMeta,
        order_cols: Vec<String>,
    },
}

enum PgSnapshotWorkResult {
    TableFinished {
        table_id: SnapshotTableId,
        count: u64,
    },
    ChunkFinished {
        table_id: SnapshotTableId,
        chunk_id: u64,
        count: u64,
        partition_col_value: ColValue,
    },
    NullsFinished {
        table_id: SnapshotTableId,
        count: u64,
    },
}

#[async_trait]
impl Extractor for PgSnapshotExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        if self.parallel_size < 1 {
            bail!("parallel_size must be greater than 0");
        }

        let tables = self.collect_tables();
        log_info!(
            "PgSnapshotExtractor starts, tables: {}, parallel_type: {:?}, parallel_size: {}",
            tables.len(),
            self.parallel_type,
            self.parallel_size
        );

        let state = PgSnapshotDispatchState {
            shared: self.shared(),
            root_extract_state: SnapshotDispatcher::clone_extract_state(&self.extract_state),
            pending_tables: tables.into_iter().collect(),
            pending_works: VecDeque::new(),
            active_tables: HashMap::new(),
            partition_cols: self.partition_cols.clone(),
        };

        SnapshotDispatcher::dispatch_work_source(
            state,
            self.parallel_size,
            "pg snapshot worker",
            Self::next_work,
            Self::run_work,
            Self::on_done,
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

impl PgSnapshotExtractor {
    fn shared(&self) -> PgSnapshotShared {
        PgSnapshotShared {
            base_extractor: self.base_extractor.clone(),
            conn_pool: self.conn_pool.clone(),
            meta_manager: self.meta_manager.clone(),
            filter: self.filter.clone(),
            batch_size: self.batch_size,
            parallel_type: self.parallel_type.clone(),
            sample_interval: self.sample_interval,
            recovery: self.recovery.clone(),
        }
    }

    fn collect_tables(&self) -> Vec<SnapshotTableId> {
        let mut tables = Vec::new();
        for (schema, tbs) in &self.schema_tbs {
            for tb in tbs {
                tables.push(SnapshotTableId {
                    schema: schema.clone(),
                    tb: tb.clone(),
                });
            }
        }
        tables
    }

    async fn next_work(
        mut state: PgSnapshotDispatchState,
    ) -> anyhow::Result<(PgSnapshotDispatchState, Option<PgSnapshotWork>)> {
        if let Some(work) = state.pending_works.pop_front() {
            state.mark_work_started(&work)?;
            return Ok((state, Some(work)));
        }

        let Some(table_id) = state.pending_tables.pop_front() else {
            return Ok((state, None));
        };

        let work = state.prepare_table_work(table_id).await?;
        Ok((state, work))
    }

    async fn run_work(work: PgSnapshotWork) -> anyhow::Result<PgSnapshotWorkResult> {
        match work {
            PgSnapshotWork::ScanAll {
                table_id,
                ctx,
                extract_state,
                tb_meta,
            } => {
                let mut worker = PgTableWorker { ctx, extract_state };
                let count = worker.extract_all(&tb_meta).await?;
                worker.extract_state.monitor.try_flush(true).await;
                Ok(PgSnapshotWorkResult::TableFinished { table_id, count })
            }

            PgSnapshotWork::OrderedBatch {
                table_id,
                ctx,
                extract_state,
                tb_meta,
            } => {
                let mut worker = PgTableWorker { ctx, extract_state };
                let count = worker.extract_by_batch(&tb_meta).await?;
                worker.extract_state.monitor.try_flush(true).await;
                Ok(PgSnapshotWorkResult::TableFinished { table_id, count })
            }

            PgSnapshotWork::SplitChunk {
                table_id,
                shared,
                conn_pool,
                tb_meta,
                partition_col,
                partition_col_type,
                sql_le,
                sql_range,
                chunk,
                ignore_cols,
                extract_state,
            } => {
                let (chunk_id, count, partition_col_value) = Self::extract_chunk(
                    shared,
                    conn_pool,
                    tb_meta,
                    partition_col,
                    partition_col_type,
                    sql_le,
                    sql_range,
                    chunk,
                    ignore_cols,
                    extract_state,
                )
                .await?;
                Ok(PgSnapshotWorkResult::ChunkFinished {
                    table_id,
                    chunk_id,
                    count,
                    partition_col_value,
                })
            }

            PgSnapshotWork::ExtractNulls {
                table_id,
                ctx,
                extract_state,
                tb_meta,
                order_cols,
            } => {
                let mut worker = PgTableWorker { ctx, extract_state };
                let count =
                    PgTableWorker::extract_nulls_with(&worker.ctx, &mut worker.extract_state, &tb_meta, &order_cols)
                        .await?;
                worker.extract_state.monitor.try_flush(true).await;
                Ok(PgSnapshotWorkResult::NullsFinished { table_id, count })
            }
        }
    }

    async fn on_done(
        mut state: PgSnapshotDispatchState,
        result: PgSnapshotWorkResult,
    ) -> anyhow::Result<PgSnapshotDispatchState> {
        match result {
            PgSnapshotWorkResult::TableFinished { table_id, count } => {
                let mut active_table = state
                    .active_tables
                    .remove(&table_id)
                    .ok_or_else(|| anyhow!("missing active pg table: {}.{}", table_id.schema, table_id.tb))?;
                active_table.extracted_count += count;
                let schema = table_id.schema.clone();
                let tb = table_id.tb.clone();
                log_info!(
                    "end extracting data from {}.{}, all count: {}",
                    quote!(&table_id.schema),
                    quote!(&table_id.tb),
                    active_table.extracted_count
                );
                state.shared.base_extractor.push_snapshot_finished(
                    &schema,
                    &tb,
                    Position::RdbSnapshotFinished {
                        db_type: DbType::Pg.to_string(),
                        schema: schema.clone(),
                        tb: tb.clone(),
                    },
                )?;
            }

            PgSnapshotWorkResult::ChunkFinished {
                table_id,
                chunk_id,
                count,
                partition_col_value,
            } => {
                let mut new_works = VecDeque::new();
                let mut finish_partition_col = None;
                let should_finish;

                {
                    let active_table = state
                        .active_tables
                        .get_mut(&table_id)
                        .ok_or_else(|| anyhow!("missing active pg table: {}.{}", table_id.schema, table_id.tb))?;
                    active_table.extracted_count += count;

                    let (
                        splitter,
                        queued_chunks,
                        running_chunks,
                        partition_col,
                        partition_col_type,
                        sql_le,
                        sql_range,
                        ignore_cols,
                    ) = match &mut active_table.mode {
                        PgActiveTableMode::SplitChunk {
                            splitter,
                            queued_chunks,
                            running_chunks,
                            partition_col,
                            partition_col_type,
                            sql_le,
                            sql_range,
                            ignore_cols,
                            ..
                        } => (
                            splitter,
                            queued_chunks,
                            running_chunks,
                            partition_col,
                            partition_col_type,
                            sql_le,
                            sql_range,
                            ignore_cols,
                        ),
                        _ => bail!(
                            "chunk result returned for non-split pg table {}.{}",
                            quote!(&table_id.schema),
                            quote!(&table_id.tb)
                        ),
                    };

                    *running_chunks = running_chunks
                        .checked_sub(1)
                        .ok_or_else(|| anyhow!("pg split chunk running count underflow"))?;

                    if let Some(position) =
                        splitter.get_next_checkpoint_position(chunk_id, partition_col_value)
                    {
                        let commit = DtData::Commit { xid: String::new() };
                        state
                            .shared
                            .base_extractor
                            .push_dt_data(&mut active_table.extract_state, commit, position)
                            .await?;
                    }

                    let next_chunks = splitter.get_next_chunks().await?;
                    for chunk in next_chunks {
                        *queued_chunks += 1;
                        new_works.push_back(PgSnapshotWork::SplitChunk {
                            table_id: table_id.clone(),
                            shared: state.shared.clone(),
                            conn_pool: state.shared.conn_pool.clone(),
                            tb_meta: active_table.tb_meta.clone(),
                            partition_col: partition_col.clone(),
                            partition_col_type: partition_col_type.clone(),
                            sql_le: sql_le.clone(),
                            sql_range: sql_range.clone(),
                            chunk,
                            ignore_cols: ignore_cols.clone(),
                            extract_state: SnapshotDispatcher::clone_extract_state(
                                &active_table.extract_state,
                            ),
                        });
                    }

                    should_finish = *queued_chunks == 0 && *running_chunks == 0;
                    if should_finish {
                        finish_partition_col = Some(partition_col.clone());
                    }
                }

                state.pending_works.extend(new_works);

                if should_finish {
                    let active_table = state
                        .active_tables
                        .get(&table_id)
                        .ok_or_else(|| anyhow!("missing finished pg split table: {}.{}", table_id.schema, table_id.tb))?;
                    let partition_col = finish_partition_col.clone().unwrap();
                    if active_table.tb_meta.basic.is_col_nullable(&partition_col) {
                        state.pending_works.push_back(PgSnapshotWork::ExtractNulls {
                            table_id: table_id.clone(),
                            ctx: active_table.ctx.clone(),
                            extract_state: SnapshotDispatcher::clone_extract_state(
                                &active_table.extract_state,
                            ),
                            tb_meta: active_table.tb_meta.clone(),
                            order_cols: vec![partition_col],
                        });
                    } else {
                        let mut active_table = state.active_tables.remove(&table_id).ok_or_else(|| {
                            anyhow!("missing finished pg split table: {}.{}", table_id.schema, table_id.tb)
                        })?;
                        active_table.extract_state.monitor.try_flush(true).await;
                        let schema = table_id.schema.clone();
                        let tb = table_id.tb.clone();
                        log_info!(
                            "end extracting data from {}.{}, all count: {}",
                            quote!(&table_id.schema),
                            quote!(&table_id.tb),
                            active_table.extracted_count
                        );
                        state.shared.base_extractor.push_snapshot_finished(
                            &schema,
                            &tb,
                            Position::RdbSnapshotFinished {
                                db_type: DbType::Pg.to_string(),
                                schema: schema.clone(),
                                tb: tb.clone(),
                            },
                        )?;
                    }
                }
            }

            PgSnapshotWorkResult::NullsFinished { table_id, count } => {
                let mut active_table = state.active_tables.remove(&table_id).ok_or_else(|| {
                    anyhow!("missing pg table after null extraction: {}.{}", table_id.schema, table_id.tb)
                })?;
                active_table.extracted_count += count;
                active_table.extract_state.monitor.try_flush(true).await;
                let schema = table_id.schema.clone();
                let tb = table_id.tb.clone();
                log_info!(
                    "end extracting data from {}.{}, all count: {}",
                    quote!(&table_id.schema),
                    quote!(&table_id.tb),
                    active_table.extracted_count
                );
                state.shared.base_extractor.push_snapshot_finished(
                    &schema,
                    &tb,
                    Position::RdbSnapshotFinished {
                        db_type: DbType::Pg.to_string(),
                        schema: schema.clone(),
                        tb: tb.clone(),
                    },
                )?;
            }
        }

        Ok(state)
    }

    #[allow(clippy::too_many_arguments)]
    async fn extract_chunk(
        shared: PgSnapshotShared,
        conn_pool: Pool<Postgres>,
        tb_meta: PgTbMeta,
        partition_col: String,
        partition_col_type: PgColType,
        sql_le: String,
        sql_range: String,
        chunk: SnapshotChunk,
        ignore_cols: Option<HashSet<String>>,
        mut extract_state: ExtractState,
    ) -> anyhow::Result<(u64, u64, ColValue)> {
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
                PgColValueConvertor::from_query(&row, &partition_col, &partition_col_type)?;
            let row_data = RowData::from_pg_row(&row, &tb_meta, &ignore_cols.as_ref());
            shared
                .base_extractor
                .push_row(&mut extract_state, row_data, Position::None)
                .await?;
            extracted_cnt += 1;
        }
        extract_state.monitor.try_flush(true).await;
        Ok((chunk_id, extracted_cnt, partition_col_value))
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
}

impl PgSnapshotDispatchState {
    async fn prepare_table_work(
        &mut self,
        table_id: SnapshotTableId,
    ) -> anyhow::Result<Option<PgSnapshotWork>> {
        let user_defined_partition_col = self
            .partition_cols
            .get(&(table_id.schema.clone(), table_id.tb.clone()))
            .cloned()
            .unwrap_or_default();
        let table_ctx = PgTableCtx {
            shared: self.shared.clone(),
            table_id: table_id.clone(),
            user_defined_partition_col,
        };
        let (extract_state, monitor_guard) = SnapshotDispatcher::derive_table_extract_state(
            &self.root_extract_state,
            &table_id.schema,
            &table_id.tb,
        )
        .await;
        let tb_meta = self
            .shared
            .meta_manager
            .clone()
            .get_tb_meta(&table_id.schema, &table_id.tb)
            .await?
            .to_owned();
        let active_mode = table_ctx.prepare_active_mode(&tb_meta).await?;

        self.active_tables.insert(
            table_id.clone(),
            PgActiveTable {
                ctx: table_ctx.clone(),
                extract_state,
                _monitor_guard: monitor_guard,
                tb_meta: tb_meta.clone(),
                extracted_count: 0,
                mode: active_mode,
            },
        );

        let active_table = self
            .active_tables
            .get_mut(&table_id)
            .ok_or_else(|| anyhow!("failed to activate pg table: {}.{}", table_id.schema, table_id.tb))?;
        let task_tb_meta = active_table.tb_meta.clone();
        let work_extract_state =
            SnapshotDispatcher::clone_extract_state(&active_table.extract_state);

        let work = match &mut active_table.mode {
            PgActiveTableMode::ScanAll => Some(PgSnapshotWork::ScanAll {
                table_id: table_id.clone(),
                ctx: table_ctx,
                extract_state: work_extract_state,
                tb_meta: task_tb_meta,
            }),
            PgActiveTableMode::OrderedBatch => Some(PgSnapshotWork::OrderedBatch {
                table_id: table_id.clone(),
                ctx: table_ctx,
                extract_state: work_extract_state,
                tb_meta: task_tb_meta,
            }),
            PgActiveTableMode::SplitChunk {
                initial_chunks,
                queued_chunks,
                partition_col,
                partition_col_type,
                sql_le,
                sql_range,
                ignore_cols,
                ..
            } => {
                let initial_chunks = std::mem::take(initial_chunks);
                for chunk in initial_chunks {
                    *queued_chunks += 1;
                    self.pending_works.push_back(PgSnapshotWork::SplitChunk {
                        table_id: table_id.clone(),
                        shared: self.shared.clone(),
                        conn_pool: self.shared.conn_pool.clone(),
                        tb_meta: task_tb_meta.clone(),
                        partition_col: partition_col.clone(),
                        partition_col_type: partition_col_type.clone(),
                        sql_le: sql_le.clone(),
                        sql_range: sql_range.clone(),
                        chunk,
                        ignore_cols: ignore_cols.clone(),
                        extract_state: SnapshotDispatcher::clone_extract_state(
                            &work_extract_state,
                        ),
                    });
                }
                let next_work = self.pending_works.pop_front();
                if let Some(ref work) = next_work {
                    self.mark_work_started(work)?;
                }
                next_work
            }
        };

        Ok(work)
    }

    fn mark_work_started(&mut self, work: &PgSnapshotWork) -> anyhow::Result<()> {
        let PgSnapshotWork::SplitChunk { table_id, .. } = work else {
            return Ok(());
        };
        let active_table = self
            .active_tables
            .get_mut(table_id)
            .ok_or_else(|| anyhow!("missing active pg table: {}.{}", table_id.schema, table_id.tb))?;
        let (queued_chunks, running_chunks) = match &mut active_table.mode {
            PgActiveTableMode::SplitChunk {
                queued_chunks,
                running_chunks,
                ..
            } => (queued_chunks, running_chunks),
            _ => {
                bail!(
                    "split chunk work scheduled for non-split pg table {}.{}",
                    quote!(&table_id.schema),
                    quote!(&table_id.tb)
                )
            }
        };
        *queued_chunks = queued_chunks
            .checked_sub(1)
            .ok_or_else(|| anyhow!("pg split chunk queued count underflow"))?;
        *running_chunks += 1;
        Ok(())
    }
}

impl PgTableCtx {
    async fn prepare_active_mode(&self, tb_meta: &PgTbMeta) -> anyhow::Result<PgActiveTableMode> {
        if matches!(self.shared.parallel_type, RdbParallelType::Chunk) {
            return self.prepare_splitter_active_mode(tb_meta).await;
        }
        if self.should_use_splitter_for_table_extract(tb_meta) {
            return self.prepare_splitter_active_mode(tb_meta).await;
        }
        if tb_meta.basic.order_cols.is_empty() {
            Ok(PgActiveTableMode::ScanAll)
        } else {
            Ok(PgActiveTableMode::OrderedBatch)
        }
    }

    async fn prepare_splitter_active_mode(
        &self,
        tb_meta: &PgTbMeta,
    ) -> anyhow::Result<PgActiveTableMode> {
        let mut splitter = self.build_splitter(tb_meta)?;
        let partition_col = splitter.get_partition_col();
        let resume_values = self
            .get_resume_values(tb_meta, &[partition_col.clone()], true)
            .await?;
        splitter.init(&resume_values)?;
        let initial_chunks = VecDeque::from(splitter.get_next_chunks().await?);

        if PgSnapshotExtractor::is_no_split_chunks(&initial_chunks) {
            log_info!(
                "table {}.{} has no split chunk, extracting by single batch extractor",
                quote!(&self.table_id.schema),
                quote!(&self.table_id.tb)
            );
            return Ok(self.fallback_active_mode(tb_meta));
        }

        let order_cols = vec![partition_col.clone()];
        let partition_col_type = tb_meta.get_col_type(&partition_col)?.clone();
        let ignore_cols = self
            .shared
            .filter
            .get_ignore_cols(&self.table_id.schema, &self.table_id.tb)
            .cloned();
        let where_condition = self
            .shared
            .filter
            .get_where_condition(&self.table_id.schema, &self.table_id.tb)
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

        Ok(PgActiveTableMode::SplitChunk {
            splitter,
            initial_chunks,
            queued_chunks: 0,
            running_chunks: 0,
            partition_col,
            partition_col_type,
            sql_le,
            sql_range,
            ignore_cols,
        })
    }

    fn fallback_active_mode(&self, tb_meta: &PgTbMeta) -> PgActiveTableMode {
        if tb_meta.basic.order_cols.is_empty() {
            PgActiveTableMode::ScanAll
        } else {
            PgActiveTableMode::OrderedBatch
        }
    }

    fn build_splitter(&self, tb_meta: &PgTbMeta) -> anyhow::Result<PgSnapshotSplitter> {
        let user_defined_partition_col = &self.user_defined_partition_col;
        self.validate_user_defined(tb_meta, user_defined_partition_col)?;
        Ok(PgSnapshotSplitter::new(
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

    fn should_use_splitter_for_table_extract(&self, tb_meta: &PgTbMeta) -> bool {
        !self.user_defined_partition_col.is_empty() || tb_meta.basic.order_cols.is_empty()
    }

    fn validate_user_defined(
        &self,
        tb_meta: &PgTbMeta,
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
        tb_meta: &PgTbMeta,
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
                .get_snapshot_resume_position(&self.table_id.schema, &self.table_id.tb, check_point)
                .await
            {
                if schema != self.table_id.schema || tb != self.table_id.tb {
                    log_info!(
                        r#"{}.{} resume position schema/tb not match, ignore it"#,
                        quote!(&self.table_id.schema),
                        quote!(&self.table_id.tb)
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
                        quote!(&self.table_id.schema),
                        quote!(&self.table_id.tb)
                    );
                    return Ok(HashMap::new());
                }
                let mut meta_manager = self.shared.meta_manager.clone();
                for ((position_order_col, value), order_col) in
                    order_col_values.into_iter().zip(order_cols.iter())
                {
                    if position_order_col != *order_col {
                        log_info!(
                            r#"{}.{} resume position order col {} not match {}"#,
                            quote!(&self.table_id.schema),
                            quote!(&self.table_id.tb),
                            position_order_col,
                            order_col
                        );
                        return Ok(HashMap::new());
                    }
                    let col_value = match value {
                        Some(v) => {
                            PgColValueConvertor::from_str(
                                tb_meta.get_col_type(order_col)?,
                                &v,
                                &mut meta_manager,
                            )?
                        }
                        None => ColValue::None,
                    };
                    resume_values.insert(position_order_col, col_value);
                }
            } else {
                log_info!(
                    r#"{}.{} has no resume position"#,
                    quote!(&self.table_id.schema),
                    quote!(&self.table_id.tb)
                );
                return Ok(HashMap::new());
            }
        }
        log_info!(
            r#"[{}.{}] recovery from [{}]"#,
            quote!(&self.table_id.schema),
            quote!(&self.table_id.tb),
            SerializeUtil::serialize_hashmap_to_json(&resume_values)?
        );
        Ok(resume_values)
    }
}

impl PgTableWorker {
    async fn extract_all(&mut self, tb_meta: &PgTbMeta) -> anyhow::Result<u64> {
        log_info!(
            "start extracting data from {}.{} without batch",
            quote!(&self.ctx.table_id.schema),
            quote!(&self.ctx.table_id.tb)
        );

        let base_count = self.extract_state.monitor.counters.pushed_record_count;
        let ignore_cols = self
            .ctx
            .shared
            .filter
            .get_ignore_cols(&self.ctx.table_id.schema, &self.ctx.table_id.tb);
        let where_condition = self
            .ctx
            .shared
            .filter
            .get_where_condition(&self.ctx.table_id.schema, &self.ctx.table_id.tb)
            .cloned()
            .unwrap_or_default();
        let sql = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.unwrap_or(&HashSet::new()))
            .with_where_condition(&where_condition)
            .build()?;

        let mut rows = sqlx::query(&sql).fetch(&self.ctx.shared.conn_pool);
        while let Some(row) = rows.try_next().await? {
            let row_data = RowData::from_pg_row(&row, tb_meta, &ignore_cols);
            self.ctx
                .shared
                .base_extractor
                .push_row(&mut self.extract_state, row_data, Position::None)
                .await?;
        }
        Ok(self.extract_state.monitor.counters.pushed_record_count - base_count)
    }

    async fn extract_by_batch(&mut self, tb_meta: &PgTbMeta) -> anyhow::Result<u64> {
        let mut resume_values = self
            .ctx
            .get_resume_values(tb_meta, &tb_meta.basic.order_cols, false)
            .await?;
        let mut start_from_beginning = false;
        if resume_values.is_empty() {
            resume_values = tb_meta.basic.get_default_order_col_values();
            start_from_beginning = true;
        }
        let mut extracted_count = 0u64;
        let mut start_values = resume_values;
        let ignore_cols = self
            .ctx
            .shared
            .filter
            .get_ignore_cols(&self.ctx.table_id.schema, &self.ctx.table_id.tb);
        let where_condition = self
            .ctx
            .shared
            .filter
            .get_where_condition(&self.ctx.table_id.schema, &self.ctx.table_id.tb)
            .cloned()
            .unwrap_or_default();
        let sql_from_beginning = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.unwrap_or(&HashSet::new()))
            .with_order_cols(&tb_meta.basic.order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::None)
            .with_limit(self.ctx.shared.batch_size)
            .build()?;
        let sql_from_value = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.unwrap_or(&HashSet::new()))
            .with_order_cols(&tb_meta.basic.order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::GreaterThan)
            .with_limit(self.ctx.shared.batch_size)
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

            let mut rows = query.fetch(&self.ctx.shared.conn_pool);
            let mut slice_count = 0usize;

            while let Some(row) = rows.try_next().await? {
                for order_col in &tb_meta.basic.order_cols {
                    let order_col_type = tb_meta.get_col_type(order_col)?;
                    if let Some(value) = start_values.get_mut(order_col) {
                        *value = PgColValueConvertor::from_query(&row, order_col, order_col_type)?;
                    } else {
                        bail!(
                            "{}.{} order col {} not found",
                            quote!(&self.ctx.table_id.schema),
                            quote!(&self.ctx.table_id.tb),
                            quote!(order_col),
                        );
                    }
                }
                extracted_count += 1;
                slice_count += 1;
                if extracted_count % self.ctx.shared.sample_interval != 0 {
                    continue;
                }

                let row_data = RowData::from_pg_row(&row, tb_meta, &ignore_cols);
                let position = tb_meta.basic.build_position(&DbType::Pg, &start_values);
                self.ctx
                    .shared
                    .base_extractor
                    .push_row(&mut self.extract_state, row_data, position)
                    .await?;
            }

            if slice_count < self.ctx.shared.batch_size {
                break;
            }
        }

        if tb_meta
            .basic
            .order_cols
            .iter()
            .any(|col| tb_meta.basic.is_col_nullable(col))
        {
            extracted_count +=
                Self::extract_nulls_with(&self.ctx, &mut self.extract_state, tb_meta, &tb_meta.basic.order_cols)
                    .await?;
        }

        Ok(extracted_count)
    }

    async fn extract_nulls_with(
        ctx: &PgTableCtx,
        extract_state: &mut ExtractState,
        tb_meta: &PgTbMeta,
        order_cols: &Vec<String>,
    ) -> anyhow::Result<u64> {
        let mut extracted_count = 0u64;
        let ignore_cols = ctx
            .shared
            .filter
            .get_ignore_cols(&ctx.table_id.schema, &ctx.table_id.tb);
        let where_condition = ctx
            .shared
            .filter
            .get_where_condition(&ctx.table_id.schema, &ctx.table_id.tb)
            .cloned()
            .unwrap_or_default();
        let sql_for_null = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.unwrap_or(&HashSet::new()))
            .with_order_cols(order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::IsNull)
            .build()?;

        let mut rows = sqlx::query(&sql_for_null).fetch(&ctx.shared.conn_pool);
        while let Some(row) = rows.try_next().await? {
            extracted_count += 1;
            let row_data = RowData::from_pg_row(&row, tb_meta, &ignore_cols);
            ctx.shared
                .base_extractor
                .push_row(extract_state, row_data, Position::None)
                .await?;
        }
        Ok(extracted_count)
    }
}
