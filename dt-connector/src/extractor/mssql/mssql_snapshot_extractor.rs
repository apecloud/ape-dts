use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use dt_common::{
    config::config_enums::{DbType, RdbParallelType},
    error::{DtError, DtErrorContextExt, ErrorObject, Stage},
    log_debug, log_info,
    meta::{
        adaptor::mssql_col_value_convertor::MssqlColValueConvertor,
        col_value::ColValue,
        dt_data::DtData,
        mssql::{
            mssql_col_type::MssqlColType, mssql_connection_pool::MssqlConnectionPool,
            mssql_meta_manager::MssqlMetaManager, mssql_tb_meta::MssqlTbMeta,
        },
        order_key::OrderKey,
        position::Position,
        row_data::RowData,
    },
    rdb_filter::RdbFilter,
    utils::{serialize_util::SerializeUtil, sql_util::SqlUtil},
};
use futures::TryStreamExt;
use tiberius::Query;

use crate::{
    extractor::{
        base_extractor::{BaseExtractor, ExtractState},
        base_splitter::SnapshotChunk,
        mssql::mssql_snapshot_splitter::MssqlSnapshotSplitter,
        rdb_snapshot_extract_statement::{OrderKeyPredicateType, RdbSnapshotExtractStatement},
        resumer::recovery::Recovery,
        snapshot_chunk_id_generator::SnapshotChunkIdGenerator,
        snapshot_dispatcher::{SnapshotDispatcher, TableMonitorGuard},
        snapshot_types::SnapshotTableId,
    },
    Extractor,
};

pub struct MssqlSnapshotExtractor {
    pub shared: MssqlSnapshotShared,
    pub extract_state: ExtractState,
    pub parallel_size: usize,
    pub schema_tbs: HashMap<String, Vec<String>>,
}

#[derive(Clone)]
pub struct MssqlSnapshotShared {
    pub base_extractor: BaseExtractor,
    pub connection_pool: MssqlConnectionPool,
    pub meta_manager: MssqlMetaManager,
    pub filter: Arc<RdbFilter>,
    pub partition_cols: Arc<HashMap<(String, String), String>>,
    pub batch_size: usize,
    pub parallel_type: RdbParallelType,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

enum MssqlSnapshotWork {
    Table {
        table_id: SnapshotTableId,
        ctx: MssqlTableCtx,
        extract_state: ExtractState,
        tb_meta: Box<MssqlTbMeta>,
    },
    Chunk {
        table_id: SnapshotTableId,
        shared: MssqlSnapshotShared,
        tb_meta: Box<MssqlTbMeta>,
        partition_col: String,
        partition_col_type: MssqlColType,
        sql_le: String,
        sql_range: String,
        chunk: Box<SnapshotChunk>,
        extract_state: ExtractState,
    },
    NullChunk {
        table_id: SnapshotTableId,
        ctx: MssqlTableCtx,
        extract_state: ExtractState,
        tb_meta: Box<MssqlTbMeta>,
        order_cols: Vec<String>,
    },
}

enum MssqlSnapshotWorkResult {
    Table {
        table_id: SnapshotTableId,
        count: u64,
    },
    Chunk {
        table_id: SnapshotTableId,
        chunk_id: u64,
        count: u64,
        partition_col_value: ColValue,
    },
    NullChunk {
        table_id: SnapshotTableId,
        count: u64,
    },
}

struct MssqlSnapshotDispatchState {
    shared: MssqlSnapshotShared,
    root_extract_state: ExtractState,
    pending_tables: VecDeque<SnapshotTableId>,
    pending_works: VecDeque<MssqlSnapshotWork>,
    active_tables: HashMap<SnapshotTableId, MssqlActiveTable>,
}

struct MssqlActiveTable {
    ctx: MssqlTableCtx,
    extract_state: ExtractState,
    _monitor_guard: TableMonitorGuard,
    tb_meta: MssqlTbMeta,
    extracted_count: u64,
    mode: MssqlActiveTableMode,
}

enum MssqlActiveTableMode {
    Table,
    Chunk {
        splitter: MssqlSnapshotSplitter,
        initial_chunks: VecDeque<SnapshotChunk>,
        queued_chunks: usize,
        running_chunks: usize,
        partition_col: String,
        partition_col_type: Box<MssqlColType>,
        sql_le: String,
        sql_range: String,
    },
}

#[derive(Clone)]
struct MssqlTableCtx {
    shared: MssqlSnapshotShared,
    table_id: SnapshotTableId,
    user_defined_partition_col: String,
}

#[async_trait]
impl Extractor for MssqlSnapshotExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        if self.parallel_size < 1 {
            bail!(
                DtError::InvalidConfig("parallel_size must be greater than 0".to_string())
                    .stage(Stage::Bootstrap)
            );
        }
        if self.shared.batch_size < 1 {
            bail!(
                DtError::InvalidConfig("batch_size must be greater than 0".to_string())
                    .stage(Stage::Bootstrap)
            );
        }

        let tables = self.collect_tables();
        log_info!(
            "MssqlSnapshotExtractor starts, tables: {}, parallel_type: {:?}, parallel_size: {}",
            tables.len(),
            self.shared.parallel_type,
            self.parallel_size
        );

        let state = MssqlSnapshotDispatchState {
            shared: self.shared.clone(),
            root_extract_state: SnapshotDispatcher::fork_extract_state(&self.extract_state),
            pending_tables: tables.into_iter().collect(),
            pending_works: VecDeque::new(),
            active_tables: HashMap::new(),
        };

        SnapshotDispatcher::dispatch_work_source(
            state,
            self.parallel_size,
            "mssql snapshot worker",
            Self::next_work,
            Self::run_work,
            Self::on_done,
        )
        .await?;

        self.shared
            .base_extractor
            .wait_task_finish(&mut self.extract_state)
            .await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl MssqlSnapshotExtractor {
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
        mut state: MssqlSnapshotDispatchState,
    ) -> anyhow::Result<(MssqlSnapshotDispatchState, Option<MssqlSnapshotWork>)> {
        if let Some(work) = state.take_next_pending_work()? {
            return Ok((state, Some(work)));
        }
        let Some(table_id) = state.pending_tables.pop_front() else {
            return Ok((state, None));
        };
        let work = state.prepare_table_work(table_id).await?;
        Ok((state, work))
    }

    async fn run_work(work: MssqlSnapshotWork) -> anyhow::Result<MssqlSnapshotWorkResult> {
        match work {
            MssqlSnapshotWork::Table {
                table_id,
                ctx,
                mut extract_state,
                tb_meta,
            } => {
                let count = ctx
                    .extract_table(&mut extract_state, tb_meta.as_ref())
                    .await?;
                extract_state.monitor.try_flush(true).await;
                Ok(MssqlSnapshotWorkResult::Table { table_id, count })
            }
            MssqlSnapshotWork::Chunk {
                table_id,
                shared,
                tb_meta,
                partition_col,
                partition_col_type,
                sql_le,
                sql_range,
                chunk,
                extract_state,
            } => {
                let (chunk_id, count, partition_col_value) = Self::extract_chunk(
                    shared,
                    *tb_meta,
                    partition_col,
                    partition_col_type,
                    sql_le,
                    sql_range,
                    *chunk,
                    extract_state,
                )
                .await?;
                Ok(MssqlSnapshotWorkResult::Chunk {
                    table_id,
                    chunk_id,
                    count,
                    partition_col_value,
                })
            }
            MssqlSnapshotWork::NullChunk {
                table_id,
                ctx,
                mut extract_state,
                tb_meta,
                order_cols,
            } => {
                let count = ctx
                    .extract_nulls(&mut extract_state, tb_meta.as_ref(), &order_cols)
                    .await?;
                extract_state.monitor.try_flush(true).await;
                Ok(MssqlSnapshotWorkResult::NullChunk { table_id, count })
            }
        }
    }

    async fn on_done(
        mut state: MssqlSnapshotDispatchState,
        result: MssqlSnapshotWorkResult,
    ) -> anyhow::Result<MssqlSnapshotDispatchState> {
        match result {
            MssqlSnapshotWorkResult::Table { table_id, count } => {
                state.finish_table(&table_id, count, false).await?;
            }
            MssqlSnapshotWorkResult::Chunk {
                table_id,
                chunk_id,
                count,
                partition_col_value,
            } => {
                let mut new_works = VecDeque::new();
                let mut finish_partition_col = None;
                let should_finish;
                {
                    let active_table = state.active_tables.get_mut(&table_id).ok_or_else(|| {
                        anyhow!(
                            "missing active MSSQL table: {}.{}",
                            table_id.schema,
                            table_id.tb
                        )
                    })?;
                    active_table.extracted_count += count;
                    let (
                        splitter,
                        queued_chunks,
                        running_chunks,
                        partition_col,
                        partition_col_type,
                        sql_le,
                        sql_range,
                    ) = match &mut active_table.mode {
                        MssqlActiveTableMode::Chunk {
                            splitter,
                            queued_chunks,
                            running_chunks,
                            partition_col,
                            partition_col_type,
                            sql_le,
                            sql_range,
                            ..
                        } => (
                            splitter,
                            queued_chunks,
                            running_chunks,
                            partition_col,
                            partition_col_type,
                            sql_le,
                            sql_range,
                        ),
                        _ => bail!(
                            "chunk result returned for non-split MSSQL table {}.{}",
                            Self::quote(&table_id.schema),
                            Self::quote(&table_id.tb)
                        ),
                    };
                    *running_chunks = running_chunks.checked_sub(1).ok_or_else(|| {
                        DtError::InvariantViolated(
                            "MSSQL split chunk running count underflow".to_string(),
                        )
                    })?;
                    if let Some(position) =
                        splitter.get_next_checkpoint_position(chunk_id, partition_col_value)
                    {
                        state
                            .shared
                            .base_extractor
                            .push_dt_data(
                                &mut active_table.extract_state,
                                DtData::Commit { xid: String::new() },
                                position,
                            )
                            .await?;
                    }
                    for chunk in splitter.get_next_chunks().await? {
                        *queued_chunks += 1;
                        new_works.push_back(MssqlSnapshotWork::Chunk {
                            table_id: table_id.clone(),
                            shared: state.shared.clone(),
                            tb_meta: Box::new(active_table.tb_meta.clone()),
                            partition_col: partition_col.clone(),
                            partition_col_type: **partition_col_type,
                            sql_le: sql_le.clone(),
                            sql_range: sql_range.clone(),
                            chunk: Box::new(chunk),
                            extract_state: SnapshotDispatcher::fork_extract_state(
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
                    let active_table = state.active_tables.get(&table_id).ok_or_else(|| {
                        anyhow!(
                            "missing finished MSSQL split table: {}.{}",
                            table_id.schema,
                            table_id.tb
                        )
                    })?;
                    let partition_col = finish_partition_col.ok_or_else(|| {
                        DtError::InvariantViolated(
                            "finished MSSQL split is missing its partition column".to_string(),
                        )
                    })?;
                    if active_table.tb_meta.basic.is_col_nullable(&partition_col) {
                        state.pending_works.push_back(MssqlSnapshotWork::NullChunk {
                            table_id: table_id.clone(),
                            ctx: active_table.ctx.clone(),
                            extract_state: SnapshotDispatcher::fork_extract_state(
                                &active_table.extract_state,
                            ),
                            tb_meta: Box::new(active_table.tb_meta.clone()),
                            order_cols: vec![partition_col],
                        });
                    } else {
                        state.finish_table(&table_id, 0, true).await?;
                    }
                }
            }
            MssqlSnapshotWorkResult::NullChunk { table_id, count } => {
                state.finish_table(&table_id, count, true).await?;
            }
        }
        Ok(state)
    }

    #[allow(clippy::too_many_arguments)]
    async fn extract_chunk(
        shared: MssqlSnapshotShared,
        tb_meta: MssqlTbMeta,
        partition_col: String,
        partition_col_type: MssqlColType,
        sql_le: String,
        sql_range: String,
        chunk: SnapshotChunk,
        mut extract_state: ExtractState,
    ) -> anyhow::Result<(u64, u64, ColValue)> {
        log_debug!(
            "extract by partition_col: {}, chunk range: {:?}",
            Self::quote(&partition_col),
            chunk
        );
        let chunk_id = chunk.chunk_id;
        let (start_value, end_value) = chunk.chunk_range;
        let mut query = match (&start_value, &end_value) {
            (ColValue::None, ColValue::None) | (_, ColValue::None) => bail!(
                "chunk {} has bad range for {}.{}",
                chunk_id,
                Self::quote(&tb_meta.basic.schema),
                Self::quote(&tb_meta.basic.tb)
            ),
            (ColValue::None, _) => Query::new(sql_le),
            _ => Query::new(sql_range),
        };
        match (&start_value, &end_value) {
            (ColValue::None, end) => {
                MssqlColValueConvertor::bind(&mut query, end, &partition_col_type)?;
            }
            (start, end) => {
                MssqlColValueConvertor::bind(&mut query, start, &partition_col_type)?;
                MssqlColValueConvertor::bind(&mut query, end, &partition_col_type)?;
            }
        }
        let ignore_cols = shared
            .filter
            .get_ignore_cols(&tb_meta.basic.schema, &tb_meta.basic.tb)
            .cloned();
        let mut connection = shared.connection_pool.get().await?;
        let mut rows = query
            .query(connection.client_mut())
            .await?
            .into_row_stream();
        let mut extracted_count = 0u64;
        let mut partition_col_value = ColValue::None;
        while let Some(row) = rows.try_next().await? {
            extracted_count += 1;
            partition_col_value =
                MssqlColValueConvertor::from_query(&row, &partition_col, &partition_col_type)?;
            let row_data =
                RowData::from_mssql_row(&row, &tb_meta, &ignore_cols.as_ref(), Some(chunk_id))?;
            shared
                .base_extractor
                .push_row(&mut extract_state, row_data, Position::None)
                .await?;
        }
        extract_state.monitor.try_flush(true).await;
        Ok((chunk_id, extracted_count, partition_col_value))
    }

    fn is_no_split_chunks(chunks: &VecDeque<SnapshotChunk>) -> bool {
        if chunks.is_empty() {
            return true;
        }
        chunks.len() == 1
            && chunks
                .front()
                .is_some_and(|chunk| matches!(&chunk.chunk_range, (ColValue::None, ColValue::None)))
    }

    fn quote(identifier: &str) -> String {
        SqlUtil::escape_by_db_type(identifier, &DbType::Mssql)
    }
}

impl MssqlSnapshotDispatchState {
    async fn finish_table(
        &mut self,
        table_id: &SnapshotTableId,
        count: u64,
        flush_monitor: bool,
    ) -> anyhow::Result<()> {
        let mut active_table = self.active_tables.remove(table_id).ok_or_else(|| {
            anyhow!(
                "missing active MSSQL table when finishing: {}.{}",
                table_id.schema,
                table_id.tb
            )
        })?;
        active_table.extracted_count += count;
        if flush_monitor {
            active_table.extract_state.monitor.try_flush(true).await;
        }
        log_info!(
            "end extracting data from {}.{}, all count: {}",
            MssqlSnapshotExtractor::quote(&table_id.schema),
            MssqlSnapshotExtractor::quote(&table_id.tb),
            active_table.extracted_count
        );
        self.shared
            .base_extractor
            .push_snapshot_finished(
                &mut active_table.extract_state,
                Position::RdbSnapshotFinished {
                    db_type: DbType::Mssql.to_string(),
                    schema: table_id.schema.clone(),
                    tb: table_id.tb.clone(),
                },
            )
            .await
    }

    async fn prepare_table_work(
        &mut self,
        table_id: SnapshotTableId,
    ) -> anyhow::Result<Option<MssqlSnapshotWork>> {
        let user_defined_partition_col = self
            .shared
            .partition_cols
            .get(&(table_id.schema.clone(), table_id.tb.clone()))
            .cloned()
            .unwrap_or_default();
        let tb_meta = self
            .shared
            .meta_manager
            .get_tb_meta(&table_id.schema, &table_id.tb)
            .await?
            .clone();
        let table_ctx = MssqlTableCtx {
            shared: self.shared.clone(),
            table_id: table_id.clone(),
            user_defined_partition_col,
        };
        table_ctx.validate_order_cols(&tb_meta)?;
        let (extract_state, monitor_guard) = SnapshotDispatcher::fork_table_extract_state(
            &self.root_extract_state,
            &table_id.schema,
            &table_id.tb,
        )
        .await;
        let active_mode = table_ctx.prepare_active_mode(&tb_meta).await?;
        self.active_tables.insert(
            table_id.clone(),
            MssqlActiveTable {
                ctx: table_ctx.clone(),
                extract_state,
                _monitor_guard: monitor_guard,
                tb_meta: tb_meta.clone(),
                extracted_count: 0,
                mode: active_mode,
            },
        );
        let active_table = self.active_tables.get_mut(&table_id).ok_or_else(|| {
            anyhow!(
                "failed to activate MSSQL table: {}.{}",
                table_id.schema,
                table_id.tb
            )
        })?;
        let task_tb_meta = active_table.tb_meta.clone();
        let work_extract_state =
            SnapshotDispatcher::fork_extract_state(&active_table.extract_state);
        let work = match &mut active_table.mode {
            MssqlActiveTableMode::Table => Some(MssqlSnapshotWork::Table {
                table_id: table_id.clone(),
                ctx: table_ctx,
                extract_state: work_extract_state,
                tb_meta: Box::new(task_tb_meta),
            }),
            MssqlActiveTableMode::Chunk {
                initial_chunks,
                queued_chunks,
                partition_col,
                partition_col_type,
                sql_le,
                sql_range,
                ..
            } => {
                for chunk in std::mem::take(initial_chunks) {
                    *queued_chunks += 1;
                    self.pending_works.push_back(MssqlSnapshotWork::Chunk {
                        table_id: table_id.clone(),
                        shared: self.shared.clone(),
                        tb_meta: Box::new(task_tb_meta.clone()),
                        partition_col: partition_col.clone(),
                        partition_col_type: **partition_col_type,
                        sql_le: sql_le.clone(),
                        sql_range: sql_range.clone(),
                        chunk: Box::new(chunk),
                        extract_state: SnapshotDispatcher::fork_extract_state(&work_extract_state),
                    });
                }
                self.take_next_pending_work()?
            }
        };
        Ok(work)
    }

    fn take_next_pending_work(&mut self) -> anyhow::Result<Option<MssqlSnapshotWork>> {
        let mut index = None;
        for (candidate, work) in self.pending_works.iter().enumerate() {
            if self.can_start_work(work)? {
                index = Some(candidate);
                break;
            }
        }
        let Some(index) = index else {
            return Ok(None);
        };
        let work = self.pending_works.remove(index).ok_or_else(|| {
            DtError::InvariantViolated("pending MSSQL snapshot work is missing".to_string())
        })?;
        self.mark_work_started(&work)?;
        Ok(Some(work))
    }

    fn can_start_work(&self, work: &MssqlSnapshotWork) -> anyhow::Result<bool> {
        if !matches!(self.shared.parallel_type, RdbParallelType::Table) {
            return Ok(true);
        }
        let MssqlSnapshotWork::Chunk { table_id, .. } = work else {
            return Ok(true);
        };
        let active_table = self.active_tables.get(table_id).ok_or_else(|| {
            anyhow!(
                "missing active MSSQL table: {}.{}",
                table_id.schema,
                table_id.tb
            )
        })?;
        let MssqlActiveTableMode::Chunk { running_chunks, .. } = &active_table.mode else {
            bail!(
                "split chunk work scheduled for non-split MSSQL table {}.{}",
                MssqlSnapshotExtractor::quote(&table_id.schema),
                MssqlSnapshotExtractor::quote(&table_id.tb)
            );
        };
        Ok(*running_chunks == 0)
    }

    fn mark_work_started(&mut self, work: &MssqlSnapshotWork) -> anyhow::Result<()> {
        let MssqlSnapshotWork::Chunk { table_id, .. } = work else {
            return Ok(());
        };
        let active_table = self.active_tables.get_mut(table_id).ok_or_else(|| {
            anyhow!(
                "missing active MSSQL table: {}.{}",
                table_id.schema,
                table_id.tb
            )
        })?;
        let MssqlActiveTableMode::Chunk {
            queued_chunks,
            running_chunks,
            ..
        } = &mut active_table.mode
        else {
            bail!(
                "split chunk work scheduled for non-split MSSQL table {}.{}",
                MssqlSnapshotExtractor::quote(&table_id.schema),
                MssqlSnapshotExtractor::quote(&table_id.tb)
            );
        };
        *queued_chunks = queued_chunks.checked_sub(1).ok_or_else(|| {
            DtError::InvariantViolated("MSSQL split chunk queued count underflow".to_string())
        })?;
        *running_chunks += 1;
        Ok(())
    }
}

impl MssqlTableCtx {
    fn uses_splitter(&self, tb_meta: &MssqlTbMeta) -> bool {
        matches!(self.shared.parallel_type, RdbParallelType::Chunk)
            || !self.user_defined_partition_col.is_empty()
            || tb_meta.basic.order_cols.is_empty()
    }

    fn partition_col(&self, tb_meta: &MssqlTbMeta) -> String {
        if self.user_defined_partition_col.is_empty() {
            tb_meta.basic.partition_col.clone()
        } else {
            self.user_defined_partition_col.clone()
        }
    }

    fn validate_order_cols(&self, tb_meta: &MssqlTbMeta) -> anyhow::Result<()> {
        let order_cols = if self.uses_splitter(tb_meta) {
            vec![self.partition_col(tb_meta)]
        } else {
            tb_meta.basic.order_cols.clone()
        };
        for col in order_cols {
            if !tb_meta.basic.has_col(&col) {
                bail!(
                    "user defined partition col {} not in cols of {}.{}",
                    MssqlSnapshotExtractor::quote(&col),
                    MssqlSnapshotExtractor::quote(&tb_meta.basic.schema),
                    MssqlSnapshotExtractor::quote(&tb_meta.basic.tb),
                );
            }
            let kind = if tb_meta.computed_cols.contains(&col) {
                Some("computed")
            } else if tb_meta
                .generated_always_type_map
                .get(&col)
                .is_some_and(|generated_always_type| *generated_always_type != 0)
            {
                Some("generated always")
            } else if tb_meta.rowversion_cols.contains(&col) {
                Some("rowversion/timestamp")
            } else {
                None
            };
            if let Some(kind) = kind {
                bail!(
                    DtError::DatabaseUnsupportedTableStructure(
                        DbType::Mssql,
                        format!(
                            "order column {}.{}.{} is {kind} and cannot be migrated as an order column",
                            tb_meta.basic.schema, tb_meta.basic.tb, col
                        ),
                    )
                    .message("An MSSQL snapshot order column is generated by SQL Server")
                    .hint(
                        "Choose a writable source column as the partition column, or change the table key used for snapshot ordering.",
                    )
                    .stage(Stage::Extractor)
                    .object(ErrorObject {
                        schema: Some(tb_meta.basic.schema.clone()),
                        table: Some(tb_meta.basic.tb.clone()),
                        column: Some(col),
                        ..Default::default()
                    })
                );
            }
        }
        Ok(())
    }

    async fn prepare_active_mode(
        &self,
        tb_meta: &MssqlTbMeta,
    ) -> anyhow::Result<MssqlActiveTableMode> {
        if self.uses_splitter(tb_meta) {
            return self.prepare_splitter_active_mode(tb_meta).await;
        }
        Ok(MssqlActiveTableMode::Table)
    }

    async fn prepare_splitter_active_mode(
        &self,
        tb_meta: &MssqlTbMeta,
    ) -> anyhow::Result<MssqlActiveTableMode> {
        let mut splitter = self.build_splitter(tb_meta)?;
        let partition_col = splitter.get_partition_col();
        let resume_values = self
            .get_resume_values(tb_meta, &[partition_col.clone()], true)
            .await?;
        splitter.init(&resume_values)?;
        let initial_chunks = VecDeque::from(splitter.get_next_chunks().await?);
        if MssqlSnapshotExtractor::is_no_split_chunks(&initial_chunks) {
            log_info!(
                "table {}.{} has no split chunk, extracting by single batch extractor",
                MssqlSnapshotExtractor::quote(&self.table_id.schema),
                MssqlSnapshotExtractor::quote(&self.table_id.tb)
            );
            return Ok(MssqlActiveTableMode::Table);
        }
        let order_cols = vec![partition_col.clone()];
        let partition_col_type = *tb_meta.get_col_type(&partition_col)?;
        let ignore_cols = self
            .shared
            .filter
            .get_ignore_cols(&self.table_id.schema, &self.table_id.tb)
            .cloned()
            .unwrap_or_default();
        let mut select_ignore_cols = ignore_cols;
        select_ignore_cols.remove(&partition_col);
        let where_condition = self
            .shared
            .filter
            .get_where_condition(&self.table_id.schema, &self.table_id.tb)
            .cloned()
            .unwrap_or_default();
        let sql_le = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(&select_ignore_cols)
            .with_order_cols(&order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::LessThanOrEqual)
            .build()?;
        let sql_range = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(&select_ignore_cols)
            .with_order_cols(&order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::Range)
            .build()?;
        Ok(MssqlActiveTableMode::Chunk {
            splitter,
            initial_chunks,
            queued_chunks: 0,
            running_chunks: 0,
            partition_col,
            partition_col_type: Box::new(partition_col_type),
            sql_le,
            sql_range,
        })
    }

    fn build_splitter(&self, tb_meta: &MssqlTbMeta) -> anyhow::Result<MssqlSnapshotSplitter> {
        let partition_col = self.partition_col(tb_meta);
        Ok(MssqlSnapshotSplitter::new(
            Arc::new(tb_meta.clone()),
            self.shared.connection_pool.clone(),
            self.shared.batch_size,
            partition_col,
        ))
    }

    async fn extract_table(
        &self,
        extract_state: &mut ExtractState,
        tb_meta: &MssqlTbMeta,
    ) -> anyhow::Result<u64> {
        if tb_meta.basic.order_cols.is_empty() {
            self.extract_all(extract_state, tb_meta).await
        } else {
            self.extract_by_batch(extract_state, tb_meta).await
        }
    }

    async fn extract_nulls(
        &self,
        extract_state: &mut ExtractState,
        tb_meta: &MssqlTbMeta,
        order_cols: &[String],
    ) -> anyhow::Result<u64> {
        let ignore_cols = self
            .shared
            .filter
            .get_ignore_cols(&self.table_id.schema, &self.table_id.tb);
        let where_condition = self
            .shared
            .filter
            .get_where_condition(&self.table_id.schema, &self.table_id.tb)
            .cloned()
            .unwrap_or_default();
        let empty_ignore_cols = HashSet::new();
        let order_cols = order_cols.to_vec();
        let sql = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.unwrap_or(&empty_ignore_cols))
            .with_order_cols(&order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::IsNull)
            .build()?;
        let mut connection = self.shared.connection_pool.get().await?;
        let mut rows = connection
            .client_mut()
            .query(&sql, &[])
            .await?
            .into_row_stream();
        let mut count = 0u64;
        let mut chunk_ids = SnapshotChunkIdGenerator::new(self.shared.batch_size);
        while let Some(row) = rows.try_next().await? {
            count += 1;
            let row_data = RowData::from_mssql_row(
                &row,
                tb_meta,
                &ignore_cols,
                Some(chunk_ids.next_row_chunk_id()),
            )?;
            self.shared
                .base_extractor
                .push_row(extract_state, row_data, Position::None)
                .await?;
        }
        Ok(count)
    }

    async fn get_resume_values(
        &self,
        tb_meta: &MssqlTbMeta,
        order_cols: &[String],
        checkpoint: bool,
    ) -> anyhow::Result<HashMap<String, ColValue>> {
        let mut resume_values = HashMap::new();
        if let Some(recovery) = &self.shared.recovery {
            let Some(Position::RdbSnapshot {
                schema,
                tb,
                order_key: Some(order_key),
                ..
            }) = recovery
                .get_snapshot_resume_position(&self.table_id.schema, &self.table_id.tb, checkpoint)
                .await
            else {
                log_info!(
                    "{}.{} has no resume position",
                    MssqlSnapshotExtractor::quote(&self.table_id.schema),
                    MssqlSnapshotExtractor::quote(&self.table_id.tb)
                );
                return Ok(resume_values);
            };
            if schema != self.table_id.schema || tb != self.table_id.tb {
                log_info!(
                    "{}.{} resume position schema/table does not match, ignoring it",
                    MssqlSnapshotExtractor::quote(&self.table_id.schema),
                    MssqlSnapshotExtractor::quote(&self.table_id.tb)
                );
                return Ok(resume_values);
            }
            let position_values = match order_key {
                OrderKey::Single(value) => vec![value],
                OrderKey::Composite(values) => values,
            };
            if position_values.len() != order_cols.len() {
                log_info!(
                    "{}.{} resume values do not match order columns in length",
                    MssqlSnapshotExtractor::quote(&self.table_id.schema),
                    MssqlSnapshotExtractor::quote(&self.table_id.tb)
                );
                return Ok(resume_values);
            }
            for ((position_col, value), order_col) in
                position_values.into_iter().zip(order_cols.iter())
            {
                if position_col != *order_col {
                    log_info!(
                        "{}.{} resume column {} does not match {}",
                        MssqlSnapshotExtractor::quote(&self.table_id.schema),
                        MssqlSnapshotExtractor::quote(&self.table_id.tb),
                        position_col,
                        order_col
                    );
                    return Ok(HashMap::new());
                }
                let value = match value {
                    Some(value) => {
                        MssqlColValueConvertor::from_str(tb_meta.get_col_type(order_col)?, &value)?
                    }
                    None => ColValue::None,
                };
                resume_values.insert(position_col, value);
            }
        }
        log_info!(
            "[{}.{}] recovery from [{}]",
            MssqlSnapshotExtractor::quote(&self.table_id.schema),
            MssqlSnapshotExtractor::quote(&self.table_id.tb),
            SerializeUtil::serialize_hashmap_to_json(&resume_values)?
        );
        Ok(resume_values)
    }

    async fn extract_all(
        &self,
        extract_state: &mut ExtractState,
        tb_meta: &MssqlTbMeta,
    ) -> anyhow::Result<u64> {
        log_info!(
            "start extracting data from {}.{} without batching",
            MssqlSnapshotExtractor::quote(&self.table_id.schema),
            MssqlSnapshotExtractor::quote(&self.table_id.tb)
        );
        let ignore_cols = self
            .shared
            .filter
            .get_ignore_cols(&self.table_id.schema, &self.table_id.tb);
        let where_condition = self
            .shared
            .filter
            .get_where_condition(&self.table_id.schema, &self.table_id.tb)
            .cloned()
            .unwrap_or_default();
        let empty_ignore_cols = HashSet::new();
        let sql = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.unwrap_or(&empty_ignore_cols))
            .with_where_condition(&where_condition)
            .build()?;
        let mut connection = self.shared.connection_pool.get().await?;
        let mut rows = connection
            .client_mut()
            .query(&sql, &[])
            .await?
            .into_row_stream();
        let mut count = 0u64;
        let mut chunk_ids = SnapshotChunkIdGenerator::new(self.shared.batch_size);
        while let Some(row) = rows.try_next().await? {
            count += 1;
            let row_data = RowData::from_mssql_row(
                &row,
                tb_meta,
                &ignore_cols,
                Some(chunk_ids.next_row_chunk_id()),
            )?;
            self.shared
                .base_extractor
                .push_row(extract_state, row_data, Position::None)
                .await?;
        }
        Ok(count)
    }

    async fn extract_by_batch(
        &self,
        extract_state: &mut ExtractState,
        tb_meta: &MssqlTbMeta,
    ) -> anyhow::Result<u64> {
        let mut start_values = self
            .get_resume_values(tb_meta, &tb_meta.basic.order_cols, false)
            .await?;
        let mut start_from_beginning = start_values.is_empty();
        if start_from_beginning {
            start_values = tb_meta.basic.get_default_order_col_values();
        }
        let ignore_cols = self
            .shared
            .filter
            .get_ignore_cols(&self.table_id.schema, &self.table_id.tb);
        let where_condition = self
            .shared
            .filter
            .get_where_condition(&self.table_id.schema, &self.table_id.tb)
            .cloned()
            .unwrap_or_default();
        let mut select_ignore_cols = ignore_cols.cloned().unwrap_or_default();
        for order_col in &tb_meta.basic.order_cols {
            select_ignore_cols.remove(order_col);
        }
        let sql_from_beginning = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(&select_ignore_cols)
            .with_order_cols(&tb_meta.basic.order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::None)
            .with_limit(self.shared.batch_size)
            .build()?;
        let sql_from_value = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(&select_ignore_cols)
            .with_order_cols(&tb_meta.basic.order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::GreaterThan)
            .with_limit(self.shared.batch_size)
            .build()?;
        let mut extracted_count = 0u64;
        let mut chunk_ids = SnapshotChunkIdGenerator::new(self.shared.batch_size);

        loop {
            let bind_values = start_values.clone();
            let query = if start_from_beginning {
                start_from_beginning = false;
                Query::new(sql_from_beginning.clone())
            } else {
                let mut query = Query::new(sql_from_value.clone());
                for order_col in &tb_meta.basic.order_cols {
                    let value = bind_values.get(order_col).ok_or_else(|| {
                        anyhow!(
                            "{}.{} order column {} has no resume value",
                            MssqlSnapshotExtractor::quote(&self.table_id.schema),
                            MssqlSnapshotExtractor::quote(&self.table_id.tb),
                            MssqlSnapshotExtractor::quote(order_col)
                        )
                    })?;
                    MssqlColValueConvertor::bind(
                        &mut query,
                        value,
                        tb_meta.get_col_type(order_col)?,
                    )?;
                }
                query
            };

            let mut connection = self.shared.connection_pool.get().await?;
            let mut rows = query
                .query(connection.client_mut())
                .await?
                .into_row_stream();
            let mut slice_count = 0usize;
            while let Some(row) = rows.try_next().await? {
                for order_col in &tb_meta.basic.order_cols {
                    let value = MssqlColValueConvertor::from_query(
                        &row,
                        order_col,
                        tb_meta.get_col_type(order_col)?,
                    )?;
                    start_values.insert(order_col.clone(), value);
                }
                extracted_count += 1;
                slice_count += 1;
                let row_data = RowData::from_mssql_row(
                    &row,
                    tb_meta,
                    &ignore_cols,
                    Some(chunk_ids.next_row_chunk_id()),
                )?;
                let position = tb_meta.basic.build_position(&DbType::Mssql, &start_values);
                self.shared
                    .base_extractor
                    .push_row(extract_state, row_data, position)
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
                .extract_nulls(extract_state, tb_meta, &tb_meta.basic.order_cols)
                .await?;
        }
        Ok(extracted_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(start: ColValue, end: ColValue) -> SnapshotChunk {
        SnapshotChunk {
            chunk_id: 1,
            chunk_range: (start, end),
        }
    }

    #[test]
    fn no_split_chunk_sentinel_is_distinct_from_real_chunks() {
        assert!(MssqlSnapshotExtractor::is_no_split_chunks(&VecDeque::new()));
        assert!(MssqlSnapshotExtractor::is_no_split_chunks(&VecDeque::from(
            [chunk(ColValue::None, ColValue::None)]
        )));
        assert!(!MssqlSnapshotExtractor::is_no_split_chunks(
            &VecDeque::from([chunk(ColValue::None, ColValue::Long(10))])
        ));
        assert!(!MssqlSnapshotExtractor::is_no_split_chunks(
            &VecDeque::from([
                chunk(ColValue::None, ColValue::Long(10)),
                chunk(ColValue::Long(10), ColValue::Long(20)),
            ])
        ));
    }
}
