use std::collections::HashMap;

use dt_common::{
    config::parallelizer_config::{
        ChunkPartitionerRebalanceConfig, ChunkPartitionerRebalanceCost,
        ChunkPartitionerRebalanceStrategy,
    },
    meta::row_data::RowData,
};

pub(crate) struct StringKeyRowRebalancePartitioner;

#[derive(Clone, Copy, Debug)]
struct PartitionCost {
    bytes: u64,
    rows: usize,
}

struct Partition {
    rows: Vec<RowData>,
    cost: PartitionCost,
}

impl Partition {
    fn new(rows: Vec<RowData>, cost_type: &ChunkPartitionerRebalanceCost) -> Self {
        let cost = PartitionCost::from_rows(&rows, cost_type);
        Self { rows, cost }
    }

    fn can_split(&self, min_partition_rows: usize) -> bool {
        self.cost.rows >= min_partition_rows.saturating_mul(2)
    }

    fn cost_key(&self, cost: &ChunkPartitionerRebalanceCost) -> (u64, u64) {
        (self.cost.primary(cost), self.cost.secondary(cost))
    }

    fn safe_primary_cost(&self, cost: &ChunkPartitionerRebalanceCost) -> u64 {
        self.cost.primary(cost).max(self.cost.rows as u64)
    }

    fn split(
        &mut self,
        cost: &ChunkPartitionerRebalanceCost,
        min_partition_rows: usize,
    ) -> Option<Partition> {
        let (split_at, left_bytes) = self.split_at(cost);
        if split_at < min_partition_rows || self.cost.rows - split_at < min_partition_rows {
            return None;
        }
        Some(self.split_off(split_at, left_bytes, cost))
    }

    fn split_at(&self, cost: &ChunkPartitionerRebalanceCost) -> (usize, Option<u64>) {
        match cost {
            ChunkPartitionerRebalanceCost::Bytes => self.split_at_by_bytes(),
            ChunkPartitionerRebalanceCost::Rows => ((self.cost.rows + 1) / 2, None),
        }
    }

    fn split_at_by_bytes(&self) -> (usize, Option<u64>) {
        if self.cost.bytes == 0 {
            return ((self.cost.rows + 1) / 2, Some(0));
        }

        let target_bytes = self.cost.bytes / 2;
        let mut best_split_at = 1;
        let mut best_left_bytes = 0;
        let mut current_bytes = 0;
        let mut reached_target = false;
        for (index, row) in self.rows.iter().take(self.cost.rows - 1).enumerate() {
            let previous_bytes = current_bytes;
            current_bytes += row.get_data_size();
            let split_at = index + 1;

            if current_bytes >= target_bytes {
                reached_target = true;
                let previous_diff = target_bytes.abs_diff(previous_bytes);
                let current_diff = target_bytes.abs_diff(current_bytes);
                if split_at > 1 && previous_diff <= current_diff {
                    best_split_at = split_at - 1;
                    best_left_bytes = previous_bytes;
                } else {
                    best_split_at = split_at;
                    best_left_bytes = current_bytes;
                }
                break;
            }
        }
        if !reached_target {
            best_split_at = self.cost.rows - 1;
            best_left_bytes = current_bytes;
        }
        (best_split_at, Some(best_left_bytes))
    }

    fn split_off(
        &mut self,
        split_at: usize,
        left_bytes: Option<u64>,
        cost: &ChunkPartitionerRebalanceCost,
    ) -> Partition {
        let original_cost = self.cost;
        let tail_rows = self.rows.split_off(split_at);
        let tail_row_count = tail_rows.len();

        match cost {
            ChunkPartitionerRebalanceCost::Bytes => {
                let left_bytes = left_bytes.unwrap_or(0);
                let tail_bytes = original_cost.bytes.saturating_sub(left_bytes);
                self.cost = PartitionCost {
                    bytes: left_bytes,
                    rows: split_at,
                };
                Partition {
                    rows: tail_rows,
                    cost: PartitionCost {
                        bytes: tail_bytes,
                        rows: tail_row_count,
                    },
                }
            }
            ChunkPartitionerRebalanceCost::Rows => {
                self.cost = PartitionCost {
                    bytes: 0,
                    rows: split_at,
                };
                Partition {
                    rows: tail_rows,
                    cost: PartitionCost {
                        bytes: 0,
                        rows: tail_row_count,
                    },
                }
            }
        }
    }

    fn into_rows(self) -> Vec<RowData> {
        self.rows
    }
}

impl PartitionCost {
    fn from_rows(rows: &[RowData], cost: &ChunkPartitionerRebalanceCost) -> Self {
        let bytes = match cost {
            ChunkPartitionerRebalanceCost::Bytes => rows.iter().map(RowData::get_data_size).sum(),
            ChunkPartitionerRebalanceCost::Rows => 0,
        };

        Self {
            bytes,
            rows: rows.len(),
        }
    }

    fn primary(&self, cost: &ChunkPartitionerRebalanceCost) -> u64 {
        match cost {
            ChunkPartitionerRebalanceCost::Bytes => self.bytes,
            ChunkPartitionerRebalanceCost::Rows => self.rows as u64,
        }
    }

    fn secondary(&self, cost: &ChunkPartitionerRebalanceCost) -> u64 {
        match cost {
            ChunkPartitionerRebalanceCost::Bytes => self.rows as u64,
            ChunkPartitionerRebalanceCost::Rows => self.rows as u64,
        }
    }
}

impl StringKeyRowRebalancePartitioner {
    pub(crate) fn partition_dml(
        data: Vec<RowData>,
        target_partitions: usize,
        config: &ChunkPartitionerRebalanceConfig,
    ) -> anyhow::Result<Vec<Vec<RowData>>> {
        if target_partitions <= 1 {
            return Ok(vec![data]);
        }

        let mut group_indexes: HashMap<String, usize> = HashMap::new();
        let mut sub_data: Vec<Vec<RowData>> = Vec::new();
        for row_data in data {
            let sch_tb_chunk = format!("{}.{}.{}", row_data.schema, row_data.tb, row_data.chunk_id);
            if let Some(index) = group_indexes.get(&sch_tb_chunk) {
                sub_data[*index].push(row_data);
            } else {
                group_indexes.insert(sch_tb_chunk, sub_data.len());
                sub_data.push(vec![row_data]);
            }
        }

        Ok(Self::rebalance_partitions(
            sub_data,
            target_partitions,
            config,
        ))
    }

    fn rebalance_partitions(
        sub_data: Vec<Vec<RowData>>,
        target_partitions: usize,
        config: &ChunkPartitionerRebalanceConfig,
    ) -> Vec<Vec<RowData>> {
        let partitions: Vec<Partition> = sub_data
            .into_iter()
            .map(|rows| Partition::new(rows, &config.cost))
            .collect();
        match config.strategy {
            ChunkPartitionerRebalanceStrategy::None => {
                partitions.into_iter().map(Partition::into_rows).collect()
            }
            ChunkPartitionerRebalanceStrategy::ChunkLargestFirst => {
                Self::sort_by_largest_first(partitions, config)
            }
            ChunkPartitionerRebalanceStrategy::SplitLargeInsert => Self::sort_by_largest_first(
                Self::split_large_insert_partitions(partitions, target_partitions, config, true),
                config,
            ),
            ChunkPartitionerRebalanceStrategy::Adaptive => Self::sort_by_largest_first(
                Self::split_large_insert_partitions(partitions, target_partitions, config, false),
                config,
            ),
        }
    }

    fn sort_by_largest_first(
        mut partitions: Vec<Partition>,
        config: &ChunkPartitionerRebalanceConfig,
    ) -> Vec<Vec<RowData>> {
        partitions.sort_by(|left, right| {
            right
                .cost_key(&config.cost)
                .cmp(&left.cost_key(&config.cost))
        });
        partitions.into_iter().map(Partition::into_rows).collect()
    }

    fn split_large_insert_partitions(
        mut partitions: Vec<Partition>,
        target_partitions: usize,
        config: &ChunkPartitionerRebalanceConfig,
        force_split: bool,
    ) -> Vec<Partition> {
        let max_partitions =
            Self::max_partitions(&partitions, target_partitions, config).max(target_partitions);
        let total_cost = Self::total_safe_primary_cost(&partitions, config);
        while partitions.len() < max_partitions {
            let Some(index) = partitions
                .iter()
                .enumerate()
                .filter(|(_, partition)| partition.can_split(config.min_partition_rows))
                .max_by_key(|(_, partition)| partition.cost_key(&config.cost))
                .map(|(index, _)| index)
            else {
                break;
            };

            if !force_split
                && partitions.len() >= target_partitions
                && !Self::is_partition_skewed(
                    &partitions[index],
                    total_cost,
                    target_partitions,
                    config,
                )
            {
                break;
            }

            let Some(tail) = partitions[index].split(&config.cost, config.min_partition_rows)
            else {
                break;
            };
            partitions.push(tail);
        }

        partitions
    }

    fn max_partitions(
        partitions: &[Partition],
        target_partitions: usize,
        config: &ChunkPartitionerRebalanceConfig,
    ) -> usize {
        let total_rows: usize = partitions.iter().map(|partition| partition.cost.rows).sum();
        let max_by_rows = (total_rows / config.min_partition_rows.max(1)).max(1);
        let max_by_config = target_partitions.saturating_mul(config.max_partitions_per_sinker);
        max_by_rows.min(max_by_config)
    }

    fn total_safe_primary_cost(
        partitions: &[Partition],
        config: &ChunkPartitionerRebalanceConfig,
    ) -> u64 {
        partitions
            .iter()
            .map(|partition| partition.safe_primary_cost(&config.cost))
            .sum()
    }

    fn is_partition_skewed(
        largest: &Partition,
        total_cost: u64,
        target_partitions: usize,
        config: &ChunkPartitionerRebalanceConfig,
    ) -> bool {
        let avg_cost_per_sinker =
            (total_cost / target_partitions.max(1) as u64).max(config.min_partition_rows as u64);
        let largest_cost = largest.safe_primary_cost(&config.cost);
        (largest_cost as f64) > (avg_cost_per_sinker as f64 * config.split_skew_ratio)
    }
}
