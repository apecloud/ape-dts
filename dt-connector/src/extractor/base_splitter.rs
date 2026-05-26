use std::cmp;
use std::collections::HashMap;

use dt_common::config::config_enums::DbType;
use dt_common::meta::position::Position;
use dt_common::meta::rdb_tb_meta::RdbTbMeta;
use dt_common::{log_info, meta::col_value::ColValue};
use thiserror::Error;

use crate::extractor::snapshot_chunk_id_generator::SnapshotChunkIdGenerator;

#[inline(always)]
fn non_empty(v: ColValue) -> Option<ColValue> {
    if matches!(v, ColValue::None) {
        None
    } else {
        Some(v)
    }
}

const DISTRIBUTION_FACTOR_LOWER: f64 = 0.05;
const DISTRIBUTION_FACTOR_UPPER: f64 = 1000.0;
const NO_NEXT_CHUNKS: u8 = 0b01;
const NO_EVEN_CHUNKS: u8 = 0b10;

#[derive(Error, Debug)]
pub enum Error {
    #[error("bad split column, min value:{0}, max value:{1}")]
    BadSplitColumnError(String, String),
    #[error("{0} out of distribution factor range [{1},{2}]")]
    OutOfDistributionFactorRangeError(f64, f64, f64),
}

pub type ChunkRange = (ColValue, ColValue);

#[derive(Debug, Clone)]
pub struct SnapshotChunk {
    pub chunk_id: u64,
    pub chunk_range: ChunkRange,
}

#[derive(Debug)]
pub struct BaseSplitter {
    chunk_id_generator: SnapshotChunkIdGenerator,
    split_state: u8,
    checkpoint_id_generator: SnapshotChunkIdGenerator,
    checkpoint_map: HashMap<u64, ColValue>,
}

impl Default for BaseSplitter {
    fn default() -> Self {
        // Both generators must start in lock-step: a chunk produced by
        // `chunk_id_generator` is matched against `checkpoint_id_generator`
        // by id.
        let id_generator = SnapshotChunkIdGenerator::default();
        Self {
            chunk_id_generator: id_generator.clone(),
            split_state: 0,
            checkpoint_id_generator: id_generator,
            checkpoint_map: HashMap::new(),
        }
    }
}

impl BaseSplitter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn gen_next_evenly_sized_chunks(
        &mut self,
        range: ChunkRange,
        batch_size: u64,
        row_cnt: u64,
        resume_value: &Option<ColValue>,
    ) -> anyhow::Result<Vec<SnapshotChunk>> {
        let (min_value, max_value) = range;
        let (min_value_i128, max_value_i128) = (
            min_value.convert_into_integer_128()?,
            max_value.convert_into_integer_128()?,
        );
        let distribution_factor: f64 =
            (max_value_i128 - min_value_i128 + 1) as f64 / (row_cnt as f64);
        if distribution_factor < DISTRIBUTION_FACTOR_LOWER
            || distribution_factor > DISTRIBUTION_FACTOR_UPPER
        {
            let err = Error::OutOfDistributionFactorRangeError(
                distribution_factor,
                DISTRIBUTION_FACTOR_LOWER,
                DISTRIBUTION_FACTOR_UPPER,
            );
            log_info!("{}", err.to_string());
            return Err(err.into());
        }
        let step_size = cmp::max((distribution_factor * batch_size as f64) as i128, 1i128);
        let mut chunks = Vec::new();
        let (mut cur_value, mut cur_value_i128) = match resume_value {
            Some(ColValue::None) => {
                // unexpected or all data have been extracted.
                self.mark_no_next_chunks();
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
                // chunk range represents left-open and right-closed interval like (v1, v2].
                // cornor case for the first interval.
                let cur_value_i128 = min_value_i128 + step_size;
                let cur_value = if cur_value_i128 >= max_value_i128 {
                    max_value.clone()
                } else {
                    min_value.add_integer_128(step_size)?
                };
                chunks.push(self.gen_next_chunk((ColValue::None, cur_value.clone())));
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
            chunks.push(self.gen_next_chunk((cur_value, t_value.clone())));
            cur_value_i128 = t_i128;
            cur_value = t_value;
        }
        self.mark_no_next_chunks();
        Ok(chunks)
    }

    /// Returns the position of the highest contiguously-completed chunk.
    /// Returns None when the chunk arrived out of order (buffered until the
    /// gap fills) or when the entire contiguous run carried no rows — empty
    /// chunks still advance the id sequence but never emit a position, so
    /// the previously persisted checkpoint is preserved.
    pub fn get_next_checkpoint_position(
        &mut self,
        chunk_id: u64,
        partition_col_value: ColValue,
        db_type: &DbType,
        partition_col: &str,
        tb_meta: &RdbTbMeta,
    ) -> Option<Position> {
        if chunk_id != self.checkpoint_id_generator.peek_next_chunk_id() {
            self.checkpoint_map.insert(chunk_id, partition_col_value);
            return None;
        }
        self.checkpoint_id_generator.next_chunk_id();
        let mut latest_value = non_empty(partition_col_value);
        while let Some(buffered) = self
            .checkpoint_map
            .remove(&self.checkpoint_id_generator.peek_next_chunk_id())
        {
            self.checkpoint_id_generator.next_chunk_id();
            if let Some(v) = non_empty(buffered) {
                latest_value = Some(v);
            }
        }
        latest_value.map(|v| Self::build_position(db_type, partition_col, tb_meta, &v))
    }

    fn build_position(
        db_type: &DbType,
        partition_col: &str,
        tb_meta: &RdbTbMeta,
        partition_col_value: &ColValue,
    ) -> Position {
        tb_meta.build_position_for_single_col(db_type, partition_col, partition_col_value, true)
    }

    #[inline(always)]
    pub fn set_state(&mut self, mask: u8) {
        self.split_state |= mask;
    }

    #[inline(always)]
    pub fn has_state(&self, mask: u8) -> bool {
        (self.split_state & mask) == mask
    }

    #[inline(always)]
    pub fn has_no_next_chunks(&self) -> bool {
        self.has_state(NO_NEXT_CHUNKS)
    }

    #[inline(always)]
    pub fn has_no_even_chunks(&self) -> bool {
        self.has_state(NO_EVEN_CHUNKS)
    }

    #[inline(always)]
    pub fn mark_no_next_chunks(&mut self) {
        self.set_state(NO_NEXT_CHUNKS);
    }

    #[inline(always)]
    pub fn mark_no_even_chunks(&mut self) {
        self.set_state(NO_EVEN_CHUNKS);
    }

    #[inline(always)]
    fn gen_next_chunk_id(&mut self) -> u64 {
        self.chunk_id_generator.next_chunk_id()
    }

    #[inline(always)]
    pub fn gen_next_chunk(&mut self, range: (ColValue, ColValue)) -> SnapshotChunk {
        SnapshotChunk {
            chunk_id: self.gen_next_chunk_id(),
            chunk_range: range,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dt_common::meta::order_key::OrderKey;

    fn make_tb_meta() -> RdbTbMeta {
        RdbTbMeta {
            schema: "s".to_string(),
            tb: "t".to_string(),
            cols: vec!["id".to_string()],
            partition_col: "id".to_string(),
            ..Default::default()
        }
    }

    fn checkpoint(
        s: &mut BaseSplitter,
        tb_meta: &RdbTbMeta,
        chunk_id: u64,
        v: i32,
    ) -> Option<Position> {
        s.get_next_checkpoint_position(chunk_id, ColValue::Long(v), &DbType::Mysql, "id", tb_meta)
    }

    fn extract_value(position: &Position) -> Option<String> {
        match position {
            Position::RdbSnapshot {
                order_key: Some(OrderKey::Single((_, value))),
                ..
            } => value.clone(),
            _ => None,
        }
    }

    #[test]
    fn checkpoint_in_order_advances_each_chunk() {
        let mut s = BaseSplitter::new();
        let tb_meta = make_tb_meta();
        let p1 = checkpoint(&mut s, &tb_meta, 1, 10).unwrap();
        let p2 = checkpoint(&mut s, &tb_meta, 2, 20).unwrap();
        let p3 = checkpoint(&mut s, &tb_meta, 3, 30).unwrap();
        assert_eq!(extract_value(&p1).as_deref(), Some("10"));
        assert_eq!(extract_value(&p2).as_deref(), Some("20"));
        assert_eq!(extract_value(&p3).as_deref(), Some("30"));
    }

    #[test]
    fn checkpoint_buffers_out_of_order_chunks() {
        let mut s = BaseSplitter::new();
        let tb_meta = make_tb_meta();
        // chunk 2 arrives before chunk 1 — must be buffered.
        assert!(checkpoint(&mut s, &tb_meta, 2, 20).is_none());
        // chunk 1 fills the gap and the buffered 2 is collapsed in.
        let p = checkpoint(&mut s, &tb_meta, 1, 10).unwrap();
        // Position reflects the highest contiguously-completed chunk (2).
        assert_eq!(extract_value(&p).as_deref(), Some("20"));
    }

    #[test]
    fn checkpoint_collapses_long_buffered_run() {
        let mut s = BaseSplitter::new();
        let tb_meta = make_tb_meta();
        assert!(checkpoint(&mut s, &tb_meta, 3, 30).is_none());
        assert!(checkpoint(&mut s, &tb_meta, 2, 20).is_none());
        assert!(checkpoint(&mut s, &tb_meta, 5, 50).is_none());
        // chunk 1 arrives — 1, 2, 3 collapse, but 5 is still gapped behind 4.
        let p = checkpoint(&mut s, &tb_meta, 1, 10).unwrap();
        assert_eq!(extract_value(&p).as_deref(), Some("30"));
        // chunk 4 fills the next gap; 5 collapses in.
        let p = checkpoint(&mut s, &tb_meta, 4, 40).unwrap();
        assert_eq!(extract_value(&p).as_deref(), Some("50"));
    }

    #[test]
    fn checkpoint_returns_none_position_when_partition_col_unknown() {
        // partition_col not present in cols → build_position_for_single_col
        // returns Position::None for is_partition=true.
        let tb_meta = RdbTbMeta {
            schema: "s".to_string(),
            tb: "t".to_string(),
            cols: vec!["other".to_string()],
            partition_col: "id".to_string(),
            ..Default::default()
        };
        let mut s = BaseSplitter::new();
        let p = s
            .get_next_checkpoint_position(1, ColValue::Long(10), &DbType::Pg, "id", &tb_meta)
            .unwrap();
        assert!(matches!(p, Position::None));
    }

    #[test]
    fn chunk_and_checkpoint_generators_start_aligned() {
        // Invariant: the first chunk emitted by gen_next_chunk has chunk_id = 1,
        // and the checkpoint side expects to see exactly that same id first.
        // If this ever fails, the two generators have drifted in initial state
        // and ordered checkpointing will silently buffer chunk 1 forever.
        let mut s = BaseSplitter::new();
        let chunk = s.gen_next_chunk((ColValue::None, ColValue::None));
        assert_eq!(
            chunk.chunk_id,
            s.checkpoint_id_generator.peek_next_chunk_id()
        );
    }

    /// Empty chunks (ColValue::None) advance the id sequence but must not
    /// emit a position — committing one would clobber the previously saved
    /// non-empty checkpoint and cause re-extraction on resume.
    #[test]
    fn checkpoint_empty_chunk_does_not_emit_position() {
        let mut s = BaseSplitter::new();
        let tb_meta = make_tb_meta();
        let p1 = checkpoint(&mut s, &tb_meta, 1, 10).unwrap();
        assert_eq!(extract_value(&p1).as_deref(), Some("10"));
        let p2 = s.get_next_checkpoint_position(2, ColValue::None, &DbType::Mysql, "id", &tb_meta);
        assert!(p2.is_none());
        // Next non-empty chunk resumes normally with its own value.
        let p3 = checkpoint(&mut s, &tb_meta, 3, 30).unwrap();
        assert_eq!(extract_value(&p3).as_deref(), Some("30"));
    }

    /// Buffered empty chunks must not surface as the emitted position when
    /// they collapse together with non-empty ones — the latest non-empty
    /// value in the contiguous run wins.
    #[test]
    fn checkpoint_collapse_skips_empty_buffered_chunks() {
        let mut s = BaseSplitter::new();
        let tb_meta = make_tb_meta();
        // Out-of-order: 3 (real), 2 (empty) buffered before 1 arrives.
        assert!(checkpoint(&mut s, &tb_meta, 3, 30).is_none());
        assert!(s
            .get_next_checkpoint_position(2, ColValue::None, &DbType::Mysql, "id", &tb_meta)
            .is_none());
        let p = checkpoint(&mut s, &tb_meta, 1, 10).unwrap();
        // 1, 2(empty), 3 collapse — final value carried is 30, not None.
        assert_eq!(extract_value(&p).as_deref(), Some("30"));
    }
}
