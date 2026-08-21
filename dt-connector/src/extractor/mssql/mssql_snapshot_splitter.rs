use std::{collections::HashMap, sync::Arc};

use anyhow::{bail, Context};
use dt_common::{
    config::config_enums::DbType,
    log_debug, log_info,
    meta::{
        adaptor::mssql_col_value_convertor::MssqlColValueConvertor,
        col_value::ColValue,
        mssql::{mssql_connection_pool::MssqlConnectionPool, mssql_tb_meta::MssqlTbMeta},
        position::Position,
        rdb_tb_meta::RdbTbMeta,
    },
    utils::sql_util::SqlUtil,
};
use tiberius::Query;

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
        if self.batch_size == 0 {
            bail!("MSSQL snapshot batch_size must be greater than 0");
        }
        if self.basic.has_no_next_chunks() {
            return Ok(Vec::new());
        }
        let tb_meta = Arc::clone(&self.mssql_tb_meta);
        let partition_col_type = tb_meta.get_col_type(&self.partition_col)?;
        if !partition_col_type.can_be_splitted() {
            log_info!(
                "table {}.{} partition col: {}, type: {:?}, can not be split",
                Self::quote(&tb_meta.basic.schema),
                Self::quote(&tb_meta.basic.tb),
                Self::quote(&self.partition_col),
                partition_col_type,
            );
            self.basic.mark_no_next_chunks();
            return Ok(vec![self
                .basic
                .gen_next_chunk((ColValue::None, ColValue::None))]);
        }
        if self.estimated_row_count == 0 {
            self.estimated_row_count = self.estimate_row_count(&tb_meta.basic).await?;
        }
        if self.estimated_row_count <= self.batch_size {
            log_debug!(
                "table {}.{} row count {} is too small, no need to split",
                Self::quote(&tb_meta.basic.schema),
                Self::quote(&tb_meta.basic.tb),
                self.estimated_row_count
            );
            self.basic.mark_no_next_chunks();
            return Ok(vec![self
                .basic
                .gen_next_chunk((ColValue::None, ColValue::None))]);
        }
        if !self.basic.has_no_even_chunks() && partition_col_type.is_integer() {
            match self.get_evenly_sized_chunks(&tb_meta).await? {
                EvenSplitOutcome::Chunks(chunks) => return Ok(chunks),
                EvenSplitOutcome::NoSplit => {
                    return Ok(vec![self
                        .basic
                        .gen_next_chunk((ColValue::None, ColValue::None))]);
                }
                EvenSplitOutcome::UseUnevenSplit => {}
            }
        }
        if let Some(chunk) = self.get_next_unevenly_sized_chunk(&tb_meta).await? {
            return Ok(vec![chunk]);
        }
        Ok(Vec::new())
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

    async fn estimate_row_count(&mut self, tb_meta: &RdbTbMeta) -> anyhow::Result<u64> {
        let catalog = Self::catalog_prefix(&tb_meta.db);
        let mut query = Query::new(format!(
            "SELECT COALESCE(SUM(CONVERT(bigint, p.rows)), CONVERT(bigint, 0)) AS row_count \
             FROM {catalog}sys.tables AS t \
             JOIN {catalog}sys.schemas AS s ON s.schema_id = t.schema_id \
             JOIN {catalog}sys.partitions AS p ON p.object_id = t.object_id \
             WHERE p.index_id IN (0, 1) AND s.name = @P1 AND t.name = @P2"
        ));
        query.bind(tb_meta.schema.as_str());
        query.bind(tb_meta.tb.as_str());
        let mut connection = self.connection_pool.get().await?;
        let row = query
            .query(connection.client_mut())
            .await?
            .into_row()
            .await?
            .context("MSSQL snapshot row count query returned no row")?;
        let row_count = MssqlColValueConvertor::from_query_required_i64(&row, "row_count")?;
        Ok(row_count.max(0) as u64)
    }

    async fn get_partition_col_range(
        &mut self,
        tb_meta: &MssqlTbMeta,
    ) -> anyhow::Result<ChunkRange> {
        let partition_col = Self::quote(&self.partition_col);
        let partition_col_type = tb_meta.get_col_type(&self.partition_col)?;
        let sql = format!(
            "SELECT MIN({partition_col}) AS min_value, MAX({partition_col}) AS max_value \
             FROM {}",
            Self::table_name(&tb_meta.basic)
        );
        let mut connection = self.connection_pool.get().await?;
        let row = connection
            .client_mut()
            .query(&sql, &[])
            .await?
            .into_row()
            .await?
            .context("MSSQL snapshot partition range query returned no row")?;
        let min_value = MssqlColValueConvertor::from_query(&row, "min_value", partition_col_type)
            .with_context(|| {
            format!(
                "schema: {}, table: {}, column: {}, failed to get minimum value",
                tb_meta.basic.schema, tb_meta.basic.tb, self.partition_col
            )
        })?;
        let max_value = MssqlColValueConvertor::from_query(&row, "max_value", partition_col_type)
            .with_context(|| {
            format!(
                "schema: {}, table: {}, column: {}, failed to get maximum value",
                tb_meta.basic.schema, tb_meta.basic.tb, self.partition_col
            )
        })?;
        Ok((min_value, max_value))
    }

    async fn get_evenly_sized_chunks(
        &mut self,
        tb_meta: &MssqlTbMeta,
    ) -> anyhow::Result<EvenSplitOutcome> {
        if self.basic.has_no_even_chunks() || self.basic.has_no_next_chunks() {
            return Ok(EvenSplitOutcome::Chunks(Vec::new()));
        }
        self.basic.mark_no_even_chunks();
        let (min_value, max_value) = if let Some(range) = &self.snapshot_range {
            range.clone()
        } else {
            let range = self.get_partition_col_range(tb_meta).await?;
            if range.0.is_same_value(&range.1) {
                log_info!(
                    "table {}.{} has no usable split range: min={}, max={}",
                    Self::quote(&tb_meta.basic.schema),
                    Self::quote(&tb_meta.basic.tb),
                    range.0,
                    range.1,
                );
                return Ok(EvenSplitOutcome::NoSplit);
            }
            self.snapshot_range = Some(range.clone());
            range
        };
        self.basic.gen_next_evenly_sized_chunks(
            (min_value, max_value),
            self.batch_size,
            self.estimated_row_count,
            &self.current_col_value,
        )
    }

    async fn get_next_unevenly_sized_chunk(
        &mut self,
        tb_meta: &MssqlTbMeta,
    ) -> anyhow::Result<Option<SnapshotChunk>> {
        let partition_col = &self.partition_col;
        let partition_col_type = tb_meta.get_col_type(partition_col)?;
        let quoted_partition_col = Self::quote(partition_col);
        let mut predicates = Vec::new();
        if tb_meta.basic.is_col_nullable(partition_col) {
            predicates.push(format!("{quoted_partition_col} IS NOT NULL"));
        }
        if self.current_col_value.is_some() {
            predicates.push(format!("{quoted_partition_col} > @P1"));
        }
        let where_clause = if predicates.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", predicates.join(" AND "))
        };
        let sql = format!(
            "SELECT MAX({quoted_partition_col}) AS max_value FROM (\
                 SELECT TOP ({}) {quoted_partition_col} FROM {} {} \
                 ORDER BY {quoted_partition_col} ASC\
             ) AS snapshot_chunk",
            self.batch_size,
            Self::table_name(&tb_meta.basic),
            where_clause,
        );
        let mut query = Query::new(sql);
        match &self.current_col_value {
            Some(ColValue::None) => {
                self.basic.mark_no_next_chunks();
                return Ok(None);
            }
            Some(current_value) => {
                MssqlColValueConvertor::bind(&mut query, current_value, partition_col_type)?;
            }
            None => {}
        }
        let mut connection = self.connection_pool.get().await?;
        let row = query
            .query(connection.client_mut())
            .await?
            .into_row()
            .await?
            .context("MSSQL next snapshot chunk query returned no row")?;
        let next_chunk_end_value =
            MssqlColValueConvertor::from_query(&row, "max_value", partition_col_type)?;
        log_debug!("next MSSQL chunk end value: {:?}", next_chunk_end_value);
        if matches!(next_chunk_end_value, ColValue::None) {
            self.basic.mark_no_next_chunks();
            return Ok(None);
        }
        let chunk_range = match &self.current_col_value {
            Some(current_value) => (current_value.clone(), next_chunk_end_value.clone()),
            None => (ColValue::None, next_chunk_end_value.clone()),
        };
        self.current_col_value = Some(next_chunk_end_value);
        Ok(Some(self.basic.gen_next_chunk(chunk_range)))
    }

    fn quote(identifier: &str) -> String {
        SqlUtil::escape_by_db_type(identifier, &DbType::Mssql)
    }

    fn catalog_prefix(db: &str) -> String {
        if db.is_empty() {
            String::new()
        } else {
            format!("{}.", Self::quote(db))
        }
    }

    fn table_name(tb_meta: &RdbTbMeta) -> String {
        SqlUtil::render_rdb_table(&DbType::Mssql, &tb_meta.db, &tb_meta.schema, &tb_meta.tb)
    }
}
