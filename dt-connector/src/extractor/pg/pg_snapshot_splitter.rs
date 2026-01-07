use std::vec;
use std::{cmp, collections::HashMap};

use anyhow::Context;
use dt_common::meta::pg::pg_col_type::PgColType;
use dt_common::meta::position::Position;
use dt_common::meta::{
    adaptor::{pg_col_value_convertor::PgColValueConvertor, sqlx_ext::SqlxPgExt},
    col_value::ColValue,
    pg::pg_tb_meta::PgTbMeta,
    rdb_tb_meta::RdbTbMeta,
};
use dt_common::utils::sql_util::*;
use dt_common::{config::config_enums::DbType, quote_pg};
use dt_common::{log_debug, log_info};
use futures::TryStreamExt;
use sqlx::{Pool, Postgres, Row};

use crate::extractor::splitter;
use crate::extractor::splitter::Error::*;

use quote_pg as quote;

const DISTRIBUTION_FACTOR_LOWER: f64 = 0.05;
const DISTRIBUTION_FACTOR_UPPER: f64 = 1000.0;
const NO_NEXT_CHUNKS: u8 = 0b01;
const NO_EVEN_CHUNKS: u8 = 0b10;

pub type ChunkRange = (ColValue, ColValue);

#[derive(Debug, Clone)]
pub struct SnapshotChunk {
    pub chunk_id: u64,
    pub chunk_range: ChunkRange,
}
pub struct PgSnapshotSplitter<'a> {
    snapshot_range: Option<ChunkRange>,
    pg_tb_meta: &'a PgTbMeta,
    split_state: u8,
    conn_pool: Pool<Postgres>,
    batch_size: u64,
    estimated_row_count: u64,
    partition_col: String,
    current_col_value: Option<ColValue>,
    chunk_id_generator: u64,
    checkpoint_id: u64,
    checkpoint_map: HashMap<u64, ColValue>,
}

impl PgSnapshotSplitter<'_> {
    pub fn new(
        pg_tb_meta: &PgTbMeta,
        conn_pool: Pool<Postgres>,
        batch_size: usize,
        partition_col: String,
    ) -> PgSnapshotSplitter<'_> {
        // todo: support user-defined split column
        PgSnapshotSplitter {
            snapshot_range: None,
            pg_tb_meta,
            split_state: 0,
            conn_pool,
            batch_size: batch_size as u64,
            estimated_row_count: 0,
            partition_col,
            current_col_value: None,
            chunk_id_generator: 0,
            checkpoint_id: 0,
            checkpoint_map: HashMap::new(),
        }
    }

    pub fn init(&mut self, resume_values: &HashMap<String, ColValue>) -> anyhow::Result<()> {
        self.current_col_value = if !resume_values.is_empty() {
            resume_values.get(&self.partition_col).cloned()
        } else {
            None
        };
        Ok(())
    }

    pub async fn get_next_chunks(&mut self) -> anyhow::Result<Vec<SnapshotChunk>> {
        // only support single-column splitting.
        if self.has(NO_NEXT_CHUNKS) {
            return Ok(Vec::new());
        }
        let pg_tb_meta = self.pg_tb_meta;
        let partition_col = &self.partition_col;
        let partition_col_type = pg_tb_meta.get_col_type(partition_col)?;
        if !partition_col_type.can_be_splitted() {
            log_info!(
                "table {}.{} partition col: {}, type: {:?}, can not be splitted",
                quote!(pg_tb_meta.basic.schema),
                quote!(pg_tb_meta.basic.tb),
                quote!(partition_col),
                partition_col_type,
            );
            self.toggle(NO_NEXT_CHUNKS);
            // represents no split
            return Ok(vec![SnapshotChunk {
                chunk_id: 0,
                chunk_range: (ColValue::None, ColValue::None),
            }]);
        }
        if self.estimated_row_count == 0 {
            self.estimated_row_count = self.estimate_row_count(&pg_tb_meta.basic).await?;
        }
        if self.estimated_row_count <= self.batch_size {
            log_info!(
                "table {}.{} row count {} is too small, no need to split",
                pg_tb_meta.basic.schema,
                pg_tb_meta.basic.tb,
                self.estimated_row_count
            );
            self.toggle(NO_NEXT_CHUNKS);
            return Ok(vec![SnapshotChunk {
                chunk_id: self.get_next_chunk_id(),
                chunk_range: (ColValue::None, ColValue::None),
            }]);
        }
        if !self.has(NO_EVEN_CHUNKS) && partition_col_type.is_integer() {
            let chunks = self.get_evenly_sized_chunks(pg_tb_meta).await;
            if let Err(e) = chunks {
                match e.downcast_ref::<splitter::Error>() {
                    Some(BadSplitColumnError { .. }) => {
                        return Ok(vec![SnapshotChunk {
                            chunk_id: self.get_next_chunk_id(),
                            chunk_range: (ColValue::None, ColValue::None),
                        }]);
                    }
                    Some(OutOfDistributionFactorRangeError { .. }) => {
                        // fallback to get_next_unevenly_sized_chunk
                    }
                    _ => return Err(e),
                }
            } else {
                return chunks;
            }
        }
        if let Some(chunk) = self.get_next_unevenly_sized_chunk(pg_tb_meta).await? {
            return Ok(vec![chunk]);
        }
        Ok(Vec::new())
    }

    pub fn get_next_checkpoint_position(
        &mut self,
        chunk_id: u64,
        partition_col_value: ColValue,
    ) -> Option<Position> {
        let partition_col = &self.partition_col;
        let mut position = if chunk_id == self.checkpoint_id + 1 {
            self.checkpoint_id = chunk_id;
            self.pg_tb_meta.basic.build_position_for_partition(
                &DbType::Pg,
                partition_col,
                &partition_col_value,
            )
        } else {
            self.checkpoint_map
                .insert(chunk_id, partition_col_value.clone());
            return None;
        };
        while let Some(partition_col_values) = self.checkpoint_map.remove(&(self.checkpoint_id + 1))
        {
            self.checkpoint_id += 1;
            position = self.pg_tb_meta.basic.build_position_for_partition(
                &DbType::Pg,
                partition_col,
                &partition_col_values,
            );
        }
        Some(position)
    }

    async fn estimate_row_count(&mut self, tb_meta: &RdbTbMeta) -> anyhow::Result<u64> {
        let sql = format!(
            "SELECT
    c.reltuples::bigint AS row_count
FROM
    pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE
    c.relkind = 'r'
    AND n.nspname = '{}'
    AND c.relname = '{}'",
            tb_meta.schema, tb_meta.tb,
        );
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        if let Some(row) = rows.try_next().await? {
            let row_count: i64 = row.try_get(0)?;
            return Ok(if row_count < 0 { 0 } else { row_count as u64 });
        }
        Ok(0)
    }

    async fn get_partition_col_range(&mut self, tb_meta: &PgTbMeta) -> anyhow::Result<ChunkRange> {
        let partition_col = &self.partition_col;
        let sql = format!(
            "SELECT
    MIN({}) AS min_value, MAX({}) AS max_value
FROM
    {}.{}",
            quote!(partition_col),
            quote!(partition_col),
            quote!(tb_meta.basic.schema),
            quote!(tb_meta.basic.tb)
        );
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        if let Some(row) = rows.try_next().await? {
            let min_value = PgColValueConvertor::from_query(
                &row,
                "min_value",
                tb_meta.get_col_type(partition_col)?,
            )
            .with_context(|| {
                format!(
                    "schema: {}, tb: {}, col: {}, fails to get min value",
                    tb_meta.basic.schema, tb_meta.basic.tb, partition_col,
                )
            })?;
            let max_value = PgColValueConvertor::from_query(
                &row,
                "max_value",
                tb_meta.get_col_type(partition_col)?,
            )
            .with_context(|| {
                format!(
                    "schema: {}, tb: {}, col: {}, fails to get max value",
                    tb_meta.basic.schema, tb_meta.basic.tb, partition_col,
                )
            })?;
            return Ok((min_value, max_value));
        }
        Ok((ColValue::None, ColValue::None))
    }

    async fn get_evenly_sized_chunks(
        &mut self,
        tb_meta: &PgTbMeta,
    ) -> anyhow::Result<Vec<SnapshotChunk>> {
        if self.has(NO_EVEN_CHUNKS) | self.has(NO_NEXT_CHUNKS) {
            return Ok(Vec::new());
        }
        self.toggle(NO_EVEN_CHUNKS);
        let (min_value, max_value) = if let Some(range) = &self.snapshot_range {
            (range.0.clone(), range.1.clone())
        } else {
            let range = self.get_partition_col_range(tb_meta).await?;
            if range.0.is_same_value(&range.1) {
                let err = BadSplitColumnError(range.0.to_string(), range.1.to_string());
                log_info!(
                    "splitting {}.{} gets: {:?}",
                    quote!(tb_meta.basic.schema),
                    quote!(tb_meta.basic.tb),
                    err.to_string()
                );
                return Err(err.into());
            }
            self.snapshot_range = Some(range.clone());
            range
        };
        let (min_value_i128, max_value_i128) = (
            min_value.convert_into_integer_128()?,
            max_value.convert_into_integer_128()?,
        );
        let distribution_factor: f64 =
            (max_value_i128 - min_value_i128 + 1) as f64 / (self.estimated_row_count as f64);
        if distribution_factor < DISTRIBUTION_FACTOR_LOWER
            || distribution_factor > DISTRIBUTION_FACTOR_UPPER
        {
            let err = OutOfDistributionFactorRangeError(
                distribution_factor,
                DISTRIBUTION_FACTOR_LOWER,
                DISTRIBUTION_FACTOR_UPPER,
            );
            log_info!(
                "splitting {}.{} gets: {:?}",
                quote!(tb_meta.basic.schema),
                quote!(tb_meta.basic.tb),
                err.to_string()
            );
            return Err(err.into());
        }
        let step_size = cmp::max(
            (distribution_factor * self.batch_size as f64) as i128,
            1i128,
        );
        let mut chunks = Vec::new();
        let (mut cur_value, mut cur_value_i128) = match &self.current_col_value {
            Some(ColValue::None) => {
                // unexpected or all data have been extracted.
                self.toggle(NO_NEXT_CHUNKS);
                return Ok(chunks);
            }
            Some(current_col_value) => {
                // from resume value
                let cur_value = current_col_value.clone();
                let cur_value_i128 = current_col_value.convert_into_integer_128()?;
                (cur_value, cur_value_i128)
            }
            None => {
                // from beginning
                // chunk range represents left-closed and right-open interval like [v1, v2).
                // cornor case for the first interval.
                let cur_value_i128 = min_value_i128 + step_size;
                let cur_value = if cur_value_i128 >= max_value_i128 {
                    max_value.clone()
                } else {
                    min_value.add_integer_128(step_size)?
                };
                chunks.push(SnapshotChunk {
                    chunk_id: self.get_next_chunk_id(),
                    chunk_range: (ColValue::None, cur_value.clone()),
                });
                (cur_value, cur_value_i128)
            }
        };
        while cur_value_i128 < max_value_i128 {
            let t_i128 = cur_value_i128 + step_size;
            let t_value = if t_i128 >= max_value_i128 {
                max_value.clone()
            } else {
                cur_value.add_integer_128(step_size)?
            };
            chunks.push(SnapshotChunk {
                chunk_id: self.get_next_chunk_id(),
                chunk_range: (cur_value, t_value.clone()),
            });
            cur_value_i128 = t_i128;
            cur_value = t_value;
        }
        self.toggle(NO_NEXT_CHUNKS);
        Ok(chunks)
    }

    async fn get_next_unevenly_sized_chunk(
        &mut self,
        tb_meta: &PgTbMeta,
    ) -> anyhow::Result<Option<SnapshotChunk>> {
        let partition_col = &self.partition_col;
        let partition_col_type = tb_meta.get_col_type(partition_col)?;
        let mut where_clause = String::new();
        if self.current_col_value.is_some() {
            where_clause = format!(
                "WHERE {} > $1::{}",
                quote!(tb_meta.basic.partition_col),
                partition_col_type.alias
            );
        }
        let extract_type = PgColValueConvertor::get_extract_type(partition_col_type);
        let get_next_chunk_end_sql = format!(
            "SELECT MAX({})::{} AS max_value FROM (
SELECT {} FROM {}.{} {} ORDER BY {} ASC LIMIT {}) AS T",
            quote!(partition_col),
            extract_type,
            quote!(partition_col),
            quote!(tb_meta.basic.schema),
            quote!(tb_meta.basic.tb),
            where_clause,
            quote!(partition_col),
            self.batch_size,
        );
        let query = match &self.current_col_value {
            Some(ColValue::None) => {
                self.toggle(NO_NEXT_CHUNKS);
                return Ok(None);
            }
            Some(current_col_value) => sqlx::query(&get_next_chunk_end_sql)
                .bind_col_value(Some(current_col_value), partition_col_type),
            None => sqlx::query(&get_next_chunk_end_sql),
        };
        let row = query.fetch_one(&self.conn_pool).await.with_context(|| {
            format!(
                "schema: {}, tb: {}, fails to get next chunk end value",
                tb_meta.basic.schema, tb_meta.basic.tb
            )
        })?;
        let next_chunk_end_value =
            PgColValueConvertor::from_query(&row, "max_value", partition_col_type)?;
        if let ColValue::None = next_chunk_end_value {
            self.toggle(NO_NEXT_CHUNKS);
            return Ok(None);
        }
        let chunk_range = if let Some(current_value) = &self.current_col_value {
            (current_value.clone(), next_chunk_end_value.clone())
        } else {
            (ColValue::None, next_chunk_end_value.clone())
        };
        self.current_col_value = Some(next_chunk_end_value);
        Ok(Some(SnapshotChunk {
            chunk_id: self.get_next_chunk_id(),
            chunk_range,
        }))
    }

    #[inline(always)]
    pub fn get_partition_col(&self) -> String {
        self.partition_col.clone()
    }

    #[inline(always)]
    fn toggle(&mut self, mask: u8) {
        self.split_state ^= mask;
    }

    #[inline(always)]
    fn has(&self, mask: u8) -> bool {
        (self.split_state & mask) == mask
    }

    #[inline(always)]
    fn get_next_chunk_id(&mut self) -> u64 {
        self.chunk_id_generator += 1;
        self.chunk_id_generator
    }
}
