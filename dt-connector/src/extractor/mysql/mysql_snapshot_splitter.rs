use std::vec;
use std::{cmp, collections::HashMap};

use anyhow::Context;
use dt_common::log_debug;
use dt_common::meta::position::Position;
use dt_common::meta::{
    adaptor::{mysql_col_value_convertor::MysqlColValueConvertor, sqlx_ext::SqlxMysqlExt},
    col_value::ColValue,
    mysql::mysql_tb_meta::MysqlTbMeta,
    rdb_tb_meta::RdbTbMeta,
};
use dt_common::utils::sql_util::*;
use dt_common::{config::config_enums::DbType, quote_mysql};
use futures::TryStreamExt;
use sqlx::{MySql, Pool, Row};

use crate::extractor::splitter::Error::*;
use crate::extractor::{mysql, splitter};

const DISTRIBUTION_FACTOR_LOWER: f64 = 0.05;
const DISTRIBUTION_FACTOR_UPPER: f64 = 1000.0;

pub type ChunkRange = (ColValue, ColValue);

#[derive(Debug, Clone)]
pub struct SnapshotChunk {
    pub chunk_id: u64,
    pub chunk_range: ChunkRange,
}
pub struct MySqlSnapshotSplitter<'a> {
    snapshot_range: Option<ChunkRange>,
    mysql_tb_meta: &'a MysqlTbMeta,
    has_next_chunks: bool,
    conn_pool: Pool<MySql>,
    batch_size: u64,
    estimated_row_count: u64,
    partition_col: String,
    current_col_value: Option<ColValue>,
    chunk_id_generator: u64,
    checkpoint_id: u64,
    checkpoint_map: HashMap<u64, ColValue>,
}

impl MySqlSnapshotSplitter<'_> {
    pub fn new(
        mysql_tb_meta: &MysqlTbMeta,
        conn_pool: Pool<MySql>,
        batch_size: usize,
    ) -> MySqlSnapshotSplitter<'_> {
        // todo: support user-defined split column
        MySqlSnapshotSplitter {
            snapshot_range: None,
            mysql_tb_meta,
            has_next_chunks: true,
            conn_pool,
            batch_size: batch_size as u64,
            estimated_row_count: 0,
            partition_col: mysql_tb_meta.basic.partition_col.clone(),
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

    pub async fn can_be_splitted(&mut self) -> anyhow::Result<bool> {
        // only support single-column splitting.
        if self.partition_col.is_empty() {
            return Ok(false);
        }
        if self.estimated_row_count == 0 {
            self.estimated_row_count = self.estimate_row_count(&self.mysql_tb_meta.basic).await?;
        }
        if self.estimated_row_count <= self.batch_size {
            return Ok(false);
        }
        if !self.mysql_tb_meta.basic.order_cols.is_empty() {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn get_next_chunks(&mut self) -> anyhow::Result<Vec<SnapshotChunk>> {
        // only support single-column splitting.
        if !self.has_next_chunks {
            return Ok(Vec::new());
        }
        let mysql_tb_meta = self.mysql_tb_meta;
        if self.estimated_row_count == 0 {
            self.estimated_row_count = self.estimate_row_count(&mysql_tb_meta.basic).await?;
        }
        if self.estimated_row_count <= self.batch_size {
            log_debug!(
                "table {}.{} row count {} is too small, no need to split",
                mysql_tb_meta.basic.schema,
                mysql_tb_meta.basic.tb,
                self.estimated_row_count
            );
            self.has_next_chunks = false;
            // represent no split
            return Ok(vec![SnapshotChunk {
                chunk_id: 0,
                chunk_range: (ColValue::None, ColValue::None),
            }]);
        }
        let partition_col = &self.partition_col;
        let partition_col_type = mysql_tb_meta.get_col_type(partition_col)?;
        match mysql_tb_meta.basic.order_cols.len() {
            0 => {
                // no primary or unique key
                if let Some(chunk) = self.get_next_unevenly_sized_chunk(mysql_tb_meta).await? {
                    return Ok(vec![chunk]);
                } else {
                    return Ok(Vec::new());
                }
            }
            _ => {
                // single or composite primary or unique key
                if partition_col_type.is_integer() {
                    let chunks = self.get_evenly_sized_chunks(mysql_tb_meta).await;
                    if let Err(e) = chunks {
                        match e.downcast_ref::<splitter::Error>() {
                            Some(OutOfDistributionFactorRangeError { .. })
                            | Some(BadSplitColumnError { .. }) => {
                                return Ok(vec![SnapshotChunk {
                                    chunk_id: 0,
                                    chunk_range: (ColValue::None, ColValue::None),
                                }]);
                            }
                            _ => return Err(e),
                        }
                    } else {
                        return chunks;
                    }
                } else if let Some(chunk) =
                    self.get_next_unevenly_sized_chunk(mysql_tb_meta).await?
                {
                    return Ok(vec![chunk]);
                }
            }
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
            self.mysql_tb_meta.basic.build_position_for_partition(
                &DbType::Mysql,
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
            position = self.mysql_tb_meta.basic.build_position_for_partition(
                &DbType::Mysql,
                partition_col,
                &partition_col_values,
            );
        }
        Some(position)
    }

    async fn estimate_row_count(&mut self, tb_meta: &RdbTbMeta) -> anyhow::Result<u64> {
        let sql = format!(
            "select
    TABLE_ROWS
FROM
    information_schema.TABLES
WHERE
    table_type = 'BASE TABLE'
    AND table_schema = '{}'
    AND table_name = '{}'
LIMIT 1",
            tb_meta.schema, tb_meta.tb
        );
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        if let Some(row) = rows.try_next().await? {
            let row_count: i64 = row.try_get(0)?;
            return Ok(if row_count < 0 { 0 } else { row_count as u64 });
        }
        Ok(0)
    }

    async fn get_partition_col_range(
        &mut self,
        tb_meta: &MysqlTbMeta,
    ) -> anyhow::Result<ChunkRange> {
        let partition_col = &self.partition_col;
        let sql = format!(
            "select
    MIN({}) AS min_value, MAX({}) AS max_value
FROM
    `{}`.`{}`",
            partition_col, partition_col, tb_meta.basic.schema, tb_meta.basic.tb
        );
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        if let Some(row) = rows.try_next().await? {
            let min_value = MysqlColValueConvertor::from_query(
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
            let max_value = MysqlColValueConvertor::from_query(
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
        tb_meta: &MysqlTbMeta,
    ) -> anyhow::Result<Vec<SnapshotChunk>> {
        let (min_value, max_value) = if let Some(range) = &self.snapshot_range {
            (range.0.clone(), range.1.clone())
        } else {
            let range = self.get_partition_col_range(tb_meta).await?;
            if range.0.is_same_value(&range.1) {
                return Err(BadSplitColumnError(range.0.to_string(), range.1.to_string()).into());
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
            return Err(OutOfDistributionFactorRangeError(
                distribution_factor,
                DISTRIBUTION_FACTOR_LOWER,
                DISTRIBUTION_FACTOR_UPPER,
            )
            .into());
        }
        let step_size = cmp::max(
            (distribution_factor * self.batch_size as f64) as i128,
            1i128,
        );
        let mut chunks = Vec::new();
        let mut cur_value_i128 = min_value_i128;
        let mut cur_value = min_value;
        while cur_value_i128 < max_value_i128 {
            let t_i128 = cur_value_i128 + step_size;
            let mut t_value = cur_value.clone();
            if t_i128 >= max_value_i128 {
                t_value = max_value.clone();
            } else {
                t_value = t_value.add_integer_128(step_size)?;
            }
            self.chunk_id_generator += 1;
            chunks.push(SnapshotChunk {
                chunk_id: self.chunk_id_generator,
                chunk_range: (cur_value, t_value.clone()),
            });
            cur_value_i128 = t_i128;
            cur_value = t_value;
        }
        self.has_next_chunks = false;
        Ok(chunks)
    }

    async fn get_next_unevenly_sized_chunk(
        &mut self,
        tb_meta: &MysqlTbMeta,
    ) -> anyhow::Result<Option<SnapshotChunk>> {
        let mut where_clause = String::new();
        if self.current_col_value.is_some() {
            where_clause = format!("{} > ?", quote_mysql!(tb_meta.basic.partition_col));
        }
        let partition_col = &self.partition_col;
        let get_next_chunk_end_sql = format!(
            "SELECT MAX({}) AS max_value FROM (
SELECT {} FROM {}.{} WHERE {} ORDER BY {} ASC LIMIT {}) AS T",
            quote_mysql!(partition_col),
            quote_mysql!(partition_col),
            quote_mysql!(tb_meta.basic.schema),
            quote_mysql!(tb_meta.basic.tb),
            where_clause,
            quote_mysql!(partition_col),
            self.batch_size,
        );
        let partition_col_type = tb_meta.get_col_type(partition_col)?;
        let query = if self.current_col_value.is_some() {
            sqlx::query(&get_next_chunk_end_sql)
                .bind_col_value(self.current_col_value.as_ref(), partition_col_type)
        } else {
            sqlx::query(&get_next_chunk_end_sql)
        };
        let row = query.fetch_one(&self.conn_pool).await.with_context(|| {
            format!(
                "schema: {}, tb: {}, fails to get next chunk end value",
                tb_meta.basic.schema, tb_meta.basic.tb
            )
        })?;
        let next_chunk_end_value =
            MysqlColValueConvertor::from_query(&row, "max_value", partition_col_type)?;
        if let ColValue::None = next_chunk_end_value {
            self.has_next_chunks = false;
            return Ok(None);
        }
        let chunk_range = if let Some(current_value) = &self.current_col_value {
            (current_value.clone(), next_chunk_end_value.clone())
        } else {
            (ColValue::None, next_chunk_end_value.clone())
        };
        self.current_col_value = Some(next_chunk_end_value);
        self.chunk_id_generator += 1;
        Ok(Some(SnapshotChunk {
            chunk_id: self.chunk_id_generator,
            chunk_range,
        }))
    }
}
