use std::cmp;

use dt_common::{log_info, meta::col_value::ColValue};
use thiserror::Error;

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

#[derive(Default)]
pub struct BaseSplitter {
    chunk_id_generator: u64,
    split_state: u8,
}

impl BaseSplitter {
    pub fn new() -> Self {
        BaseSplitter::default()
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
        self.chunk_id_generator += 1;
        self.chunk_id_generator
    }

    #[inline(always)]
    pub fn gen_next_chunk(&mut self, range: (ColValue, ColValue)) -> SnapshotChunk {
        SnapshotChunk {
            chunk_id: self.gen_next_chunk_id(),
            chunk_range: range,
        }
    }
}
