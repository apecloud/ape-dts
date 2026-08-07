use std::{collections::HashMap, sync::Arc};

use dt_common::{
    config::config_enums::DbType,
    meta::{
        col_value::ColValue,
        mssql::{mssql_connection_pool::MssqlConnectionPool, mssql_tb_meta::MssqlTbMeta},
        position::Position,
        rdb_tb_meta::RdbTbMeta,
    },
};

use crate::extractor::base_splitter::{BaseSplitter, ChunkRange, EvenSplitOutcome, SnapshotChunk};

pub struct MssqlSnapshotSplitter {
    basic: BaseSplitter,
    snapshot_range: Option<ChunkRange>,
    mssql_tb_meta: Arc<MssqlTbMeta>,
    connection_pool: MssqlConnectionPool,
    batch_size: u64,
    estimated_row_count: u64,
    partition_col: String,
    current_col_value: Option<ColValue>,
}

impl MssqlSnapshotSplitter {
    pub fn new(
        mssql_tb_meta: Arc<MssqlTbMeta>,
        connection_pool: MssqlConnectionPool,
        batch_size: usize,
        partition_col: String,
    ) -> Self {
        Self {
            basic: BaseSplitter::new(),
            snapshot_range: None,
            mssql_tb_meta,
            connection_pool,
            batch_size: batch_size as u64,
            estimated_row_count: 0,
            partition_col,
            current_col_value: None,
        }
    }

    pub fn init(&mut self, resume_values: &HashMap<String, ColValue>) -> anyhow::Result<()> {
        self.current_col_value = resume_values.get(&self.partition_col).cloned();
        Ok(())
    }

    pub async fn get_next_chunks(&mut self) -> anyhow::Result<Vec<SnapshotChunk>> {
        todo!("mssql snapshot chunk generation is not implemented")
    }

    pub fn get_next_checkpoint_position(
        &mut self,
        chunk_id: u64,
        partition_col_value: ColValue,
    ) -> Option<Position> {
        self.basic.get_next_checkpoint_position(
            chunk_id,
            partition_col_value,
            &DbType::Mssql,
            &self.partition_col,
            &self.mssql_tb_meta.basic,
        )
    }

    pub fn get_partition_col(&self) -> String {
        self.partition_col.clone()
    }

    async fn estimate_row_count(&mut self, _tb_meta: &RdbTbMeta) -> anyhow::Result<u64> {
        todo!("mssql snapshot row count estimation is not implemented")
    }

    async fn get_partition_col_range(
        &mut self,
        _tb_meta: &MssqlTbMeta,
    ) -> anyhow::Result<ChunkRange> {
        todo!("mssql snapshot partition range query is not implemented")
    }

    async fn get_evenly_sized_chunks(
        &mut self,
        _tb_meta: &MssqlTbMeta,
    ) -> anyhow::Result<EvenSplitOutcome> {
        todo!("mssql evenly sized snapshot chunk generation is not implemented")
    }

    async fn get_next_unevenly_sized_chunk(
        &mut self,
        _tb_meta: &MssqlTbMeta,
    ) -> anyhow::Result<Option<SnapshotChunk>> {
        todo!("mssql uneven snapshot chunk generation is not implemented")
    }
}
