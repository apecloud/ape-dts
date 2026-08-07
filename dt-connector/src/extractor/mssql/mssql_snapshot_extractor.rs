use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use anyhow::bail;
use async_trait::async_trait;
use dt_common::{
    config::config_enums::RdbParallelType,
    error::{DtError, DtErrorContextExt, Stage},
    log_info,
    meta::{
        col_value::ColValue,
        mssql::{
            mssql_col_type::MssqlColType, mssql_connection_pool::MssqlConnectionPool,
            mssql_meta_manager::MssqlMetaManager, mssql_tb_meta::MssqlTbMeta,
        },
    },
    rdb_filter::RdbFilter,
};

use crate::{
    extractor::{
        base_extractor::{BaseExtractor, ExtractState},
        base_splitter::SnapshotChunk,
        mssql::mssql_snapshot_splitter::MssqlSnapshotSplitter,
        resumer::recovery::Recovery,
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
        _state: MssqlSnapshotDispatchState,
    ) -> anyhow::Result<(MssqlSnapshotDispatchState, Option<MssqlSnapshotWork>)> {
        todo!("mssql snapshot dispatcher next-work selection is not implemented")
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
        _state: MssqlSnapshotDispatchState,
        _result: MssqlSnapshotWorkResult,
    ) -> anyhow::Result<MssqlSnapshotDispatchState> {
        todo!("mssql snapshot work completion handling is not implemented")
    }

    #[allow(clippy::too_many_arguments)]
    async fn extract_chunk(
        _shared: MssqlSnapshotShared,
        _tb_meta: MssqlTbMeta,
        _partition_col: String,
        _partition_col_type: MssqlColType,
        _sql_le: String,
        _sql_range: String,
        _chunk: SnapshotChunk,
        _extract_state: ExtractState,
    ) -> anyhow::Result<(u64, u64, ColValue)> {
        todo!("mssql snapshot chunk extraction is not implemented")
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

impl MssqlTableCtx {
    async fn prepare_active_mode(
        &self,
        _tb_meta: &MssqlTbMeta,
    ) -> anyhow::Result<MssqlActiveTableMode> {
        todo!("mssql snapshot active table mode selection is not implemented")
    }

    async fn prepare_splitter_active_mode(
        &self,
        _tb_meta: &MssqlTbMeta,
    ) -> anyhow::Result<MssqlActiveTableMode> {
        todo!("mssql snapshot splitter active mode preparation is not implemented")
    }

    fn build_splitter(&self, _tb_meta: &MssqlTbMeta) -> anyhow::Result<MssqlSnapshotSplitter> {
        todo!("mssql snapshot splitter construction is not implemented")
    }

    async fn extract_table(
        &self,
        _extract_state: &mut ExtractState,
        _tb_meta: &MssqlTbMeta,
    ) -> anyhow::Result<u64> {
        todo!("mssql snapshot table extraction is not implemented")
    }

    async fn extract_nulls(
        &self,
        _extract_state: &mut ExtractState,
        _tb_meta: &MssqlTbMeta,
        _order_cols: &[String],
    ) -> anyhow::Result<u64> {
        todo!("mssql snapshot NULL chunk extraction is not implemented")
    }
}
