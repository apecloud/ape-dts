use strum::{Display, EnumString, IntoStaticStr};

use super::config_enums::ParallelType;

#[derive(Clone)]
pub struct ParallelizerConfig {
    pub parallel_type: ParallelType,
    pub parallel_size: usize,
    pub chunk_partitioner_rebalance: ChunkPartitionerRebalanceConfig,
}

#[derive(Clone, Debug)]
pub struct ChunkPartitionerRebalanceConfig {
    /// Rebalance behavior for snapshot DML partitions.
    pub strategy: ChunkPartitionerRebalanceStrategy,
    /// Cost metric used to order partitions, detect skew, and choose split points.
    pub cost: ChunkPartitionerRebalanceCost,
    /// Optional hard cap multiplier: max partitions = effective sinkers * this value.
    /// usize::MAX means the partitioner derives the cap from the current batch rows.
    pub max_partitions_per_sinker: usize,
    /// Minimum rows kept in each split partition; defaults to sinker.batch_size at load time.
    pub min_partition_rows: usize,
    /// Adaptive split threshold: split when largest partition cost is greater than
    /// average cost per sinker times this ratio.
    pub split_skew_ratio: f64,
}

impl Default for ChunkPartitionerRebalanceConfig {
    fn default() -> Self {
        Self {
            strategy: ChunkPartitionerRebalanceStrategy::Adaptive,
            cost: ChunkPartitionerRebalanceCost::Rows,
            // The partitioner derives the effective cap from the current batch size.
            max_partitions_per_sinker: 2,
            min_partition_rows: 200,
            split_skew_ratio: 1.0,
        }
    }
}

#[derive(Clone, Debug, Display, EnumString, IntoStaticStr, PartialEq, Eq)]
pub enum ChunkPartitionerRebalanceStrategy {
    /// Keep logical chunk order after grouping; no sorting or splitting.
    #[strum(serialize = "none")]
    None,
    /// Sort logical chunks by configured cost, largest first; no splitting.
    #[strum(serialize = "chunk_largest_first")]
    ChunkLargestFirst,
    /// Split large insert-only chunks whenever safe, up to the partition cap.
    #[strum(serialize = "split_large_insert")]
    SplitLargeInsert,
    /// Sort by cost and split only when there are too few or clearly skewed partitions.
    #[strum(serialize = "adaptive")]
    Adaptive,
}

#[derive(Clone, Debug, Display, EnumString, IntoStaticStr, PartialEq, Eq)]
pub enum ChunkPartitionerRebalanceCost {
    /// Use estimated row bytes as the primary cost, with row count as tie-breaker.
    #[strum(serialize = "bytes")]
    Bytes,
    /// Use row count as the cost metric.
    #[strum(serialize = "rows")]
    Rows,
}
