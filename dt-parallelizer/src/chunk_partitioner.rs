use std::collections::HashMap;

use dt_common::{
    config::parallelizer_config::{
        ChunkPartitionerRebalanceConfig, ChunkPartitionerRebalanceCost,
        ChunkPartitionerRebalanceStrategy,
    },
    meta::{
        dt_data::{DtData, DtItem},
        row_data::RowData,
        row_type::RowType,
    },
};

pub struct ChunkPartitioner {}

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
    fn new(rows: Vec<RowData>) -> Self {
        let cost = PartitionCost::from_rows(&rows);
        Self { rows, cost }
    }

    #[allow(dead_code)]
    fn is_insert_only(&self) -> bool {
        self.rows.iter().all(|row| row.row_type == RowType::Insert)
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

        let mut best_split_at = 1;
        let mut best_left_bytes = 0;
        let mut best_diff = self.cost.bytes;
        let mut current_bytes = 0;
        for (index, row) in self.rows.iter().enumerate() {
            current_bytes += row.get_data_size();
            let split_at = index + 1;
            if split_at >= self.cost.rows {
                break;
            }

            let left_bytes = current_bytes;
            let right_bytes = self.cost.bytes - current_bytes;
            // Pick the boundary closest to half the configured byte cost.
            let diff = left_bytes.abs_diff(right_bytes);
            if diff < best_diff {
                best_diff = diff;
                best_split_at = split_at;
                best_left_bytes = left_bytes;
            }
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
    fn from_rows(rows: &[RowData]) -> Self {
        Self {
            bytes: rows.iter().map(RowData::get_data_size).sum(),
            rows: rows.len(),
        }
    }

    fn primary(&self, cost: &ChunkPartitionerRebalanceCost) -> u64 {
        // User-selected primary metric controls sort order, skew detection, and split point.
        match cost {
            ChunkPartitionerRebalanceCost::Bytes => self.bytes,
            ChunkPartitionerRebalanceCost::Rows => self.rows as u64,
        }
    }

    fn secondary(&self, cost: &ChunkPartitionerRebalanceCost) -> u64 {
        match cost {
            // Only use row count as the secondary metric for now
            ChunkPartitionerRebalanceCost::Bytes => self.rows as u64,
            ChunkPartitionerRebalanceCost::Rows => self.rows as u64,
        }
    }
}

impl ChunkPartitioner {
    pub fn partition_dml(
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
            // Keep each logical snapshot chunk together before any strategy-specific rebalance.
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
        let partitions: Vec<Partition> = sub_data.into_iter().map(Partition::new).collect();
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
        // Only snapshot inserts are safe to split; mixed DML keeps logical chunks intact.
        // For now, chunk partitioner rebalance only happens for snapshot parallelizer, so all rows are inserts.
        // if !partitions.iter().all(Partition::is_insert_only) {
        //     return partitions;
        // }

        let max_partitions =
            Self::max_partitions(&partitions, target_partitions, config).max(target_partitions);
        while partitions.len() < max_partitions {
            // Always split the currently largest eligible partition; sinkers consume the
            // resulting queue dynamically, so static round-robin assignment is unnecessary.
            let Some(index) = partitions
                .iter()
                .enumerate()
                // Both sides of the split must still satisfy min_partition_rows.
                .filter(|(_, partition)| partition.can_split(config.min_partition_rows))
                .max_by_key(|(_, partition)| partition.cost_key(&config.cost))
                .map(|(index, _)| index)
            else {
                break;
            };

            // adaptive stops once concurrency is filled and the largest partition is no longer
            // skewed. split_large_insert passes force_split=true and keeps splitting up to cap.
            if !force_split
                && partitions.len() >= target_partitions
                && !Self::is_partition_skewed(&partitions, index, target_partitions, config)
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
        // Derive the effective cap from the current drain batch to avoid over-splitting.
        let max_by_rows = (total_rows / config.min_partition_rows.max(1)).max(1);
        let max_by_config = target_partitions.saturating_mul(config.max_partitions_per_sinker);
        max_by_rows.min(max_by_config)
    }

    fn is_partition_skewed(
        partitions: &[Partition],
        largest_index: usize,
        target_partitions: usize,
        config: &ChunkPartitionerRebalanceConfig,
    ) -> bool {
        // Use the configured cost metric, but never let zero-byte rows make cost disappear.
        let total_cost: u64 = partitions
            .iter()
            .map(|partition| partition.safe_primary_cost(&config.cost))
            .sum();
        // Compare the largest partition with ideal per-sinker work, not average partition size.
        let avg_cost_per_sinker =
            (total_cost / target_partitions.max(1) as u64).max(config.min_partition_rows as u64);
        let largest_cost = partitions[largest_index].safe_primary_cost(&config.cost);

        // Example: ratio=2.0 means "split if the largest partition is > 2x ideal work".
        (largest_cost as f64) > (avg_cost_per_sinker as f64 * config.split_skew_ratio)
    }

    pub fn partition_raw(data: Vec<DtItem>) -> anyhow::Result<Vec<Vec<DtItem>>> {
        let mut sub_data_map: HashMap<String, Vec<DtItem>> = HashMap::new();
        let defualt_key = "default".to_string();
        for item in data {
            if let DtData::Dml { row_data } = &item.dt_data {
                let sch_tb_chunk =
                    format!("{}.{}.{}", row_data.schema, row_data.tb, row_data.chunk_id);
                if let Some(sub_data) = sub_data_map.get_mut(&sch_tb_chunk) {
                    sub_data.push(item);
                } else {
                    sub_data_map.insert(sch_tb_chunk, vec![item]);
                }
            } else if let Some(sub_data) = sub_data_map.get_mut(&defualt_key) {
                sub_data.push(item);
            } else {
                sub_data_map.insert(defualt_key.clone(), vec![item]);
            }
        }

        Ok(sub_data_map.into_values().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dt_common::config::parallelizer_config::ChunkPartitionerRebalanceStrategy;

    fn config(strategy: ChunkPartitionerRebalanceStrategy) -> ChunkPartitionerRebalanceConfig {
        ChunkPartitionerRebalanceConfig {
            strategy,
            cost: ChunkPartitionerRebalanceCost::Bytes,
            max_partitions_per_sinker: 4,
            min_partition_rows: 1,
            split_skew_ratio: 2.0,
        }
    }

    fn row(chunk_id: u64, row_type: RowType) -> RowData {
        sized_row("schema", "tb", chunk_id, row_type, 1)
    }

    fn sized_row(
        schema: &str,
        tb: &str,
        chunk_id: u64,
        row_type: RowType,
        data_size: usize,
    ) -> RowData {
        let mut row = RowData::new(
            schema.to_string(),
            tb.to_string(),
            chunk_id,
            row_type,
            None,
            None,
        );
        row.data_size = data_size;
        row
    }

    fn chunk_ids(partitions: &[Vec<RowData>]) -> Vec<Vec<u64>> {
        partitions
            .iter()
            .map(|partition| partition.iter().map(|row| row.chunk_id).collect())
            .collect()
    }

    #[test]
    fn partition_dml_none_keeps_stable_chunk_order_without_split_or_sort() {
        let data = vec![
            row(2, RowType::Insert),
            row(1, RowType::Insert),
            row(2, RowType::Insert),
            row(3, RowType::Insert),
            row(1, RowType::Insert),
        ];

        let partitions = ChunkPartitioner::partition_dml(
            data,
            4,
            &config(ChunkPartitionerRebalanceStrategy::None),
        )
        .unwrap();

        assert_eq!(
            chunk_ids(&partitions),
            vec![vec![2, 2], vec![1, 1], vec![3]]
        );
    }

    #[test]
    fn partition_dml_largest_first_sorts_by_bytes_then_rows() {
        let data = vec![
            sized_row("schema", "tb", 1, RowType::Insert, 10),
            sized_row("schema", "tb", 1, RowType::Insert, 10),
            sized_row("schema", "tb", 2, RowType::Insert, 100),
            sized_row("schema", "tb", 3, RowType::Insert, 10),
            sized_row("schema", "tb", 3, RowType::Insert, 10),
            sized_row("schema", "tb", 3, RowType::Insert, 10),
        ];

        let partitions = ChunkPartitioner::partition_dml(
            data,
            2,
            &config(ChunkPartitionerRebalanceStrategy::ChunkLargestFirst),
        )
        .unwrap();

        assert_eq!(
            chunk_ids(&partitions),
            vec![vec![2], vec![3, 3, 3], vec![1, 1]]
        );
    }

    #[test]
    fn partition_dml_adaptive_splits_large_insert_group_when_too_few_partitions() {
        let data = vec![
            row(1, RowType::Insert),
            row(1, RowType::Insert),
            row(1, RowType::Insert),
            row(1, RowType::Insert),
        ];

        let partitions = ChunkPartitioner::partition_dml(
            data,
            4,
            &config(ChunkPartitionerRebalanceStrategy::Adaptive),
        )
        .unwrap();

        assert_eq!(partitions.len(), 4);
        assert!(partitions.iter().all(|partition| partition.len() == 1));
    }

    #[test]
    fn partition_dml_adaptive_splits_large_insert_group_into_contiguous_segments() {
        let data = (0..8)
            .map(|index| sized_row("schema", "tb", 1, RowType::Insert, index + 1))
            .collect();

        let partitions = ChunkPartitioner::partition_dml(
            data,
            2,
            &config(ChunkPartitionerRebalanceStrategy::Adaptive),
        )
        .unwrap();

        assert!(partitions.len() > 1);
        assert_eq!(partitions.iter().map(Vec::len).sum::<usize>(), 8);
        assert!(partitions
            .iter()
            .all(|partition| partition.iter().all(|row| row.chunk_id == 1)));
    }

    #[test]
    fn partition_dml_adaptive_does_not_split_mixed_row_types() {
        let data = vec![
            row(1, RowType::Insert),
            row(1, RowType::Update),
            row(1, RowType::Insert),
            row(1, RowType::Insert),
        ];

        let partitions = ChunkPartitioner::partition_dml(
            data,
            4,
            &config(ChunkPartitionerRebalanceStrategy::Adaptive),
        )
        .unwrap();

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].len(), 4);
    }

    #[test]
    fn partition_dml_does_not_create_empty_partitions_when_target_exceeds_rows() {
        let data = vec![row(1, RowType::Insert), row(1, RowType::Insert)];

        let partitions = ChunkPartitioner::partition_dml(
            data,
            8,
            &config(ChunkPartitionerRebalanceStrategy::SplitLargeInsert),
        )
        .unwrap();

        assert_eq!(partitions.len(), 2);
        assert!(partitions.iter().all(|partition| partition.len() == 1));
    }

    #[test]
    fn partition_dml_groups_by_schema_table_and_chunk_id() {
        let data = vec![
            sized_row("schema_1", "tb", 1, RowType::Insert, 1),
            sized_row("schema_2", "tb", 1, RowType::Insert, 1),
            sized_row("schema_1", "tb", 1, RowType::Insert, 1),
            sized_row("schema_1", "tb_2", 1, RowType::Insert, 1),
        ];

        let partitions = ChunkPartitioner::partition_dml(
            data,
            4,
            &config(ChunkPartitionerRebalanceStrategy::ChunkLargestFirst),
        )
        .unwrap();

        let lengths: Vec<usize> = partitions.iter().map(Vec::len).collect();
        assert_eq!(lengths, vec![2, 1, 1]);
    }

    #[test]
    fn target_partitions_one_returns_single_partition() {
        let data = vec![
            row(1, RowType::Insert),
            row(2, RowType::Insert),
            row(1, RowType::Insert),
        ];

        let partitions = ChunkPartitioner::partition_dml(
            data,
            1,
            &config(ChunkPartitionerRebalanceStrategy::Adaptive),
        )
        .unwrap();

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].len(), 3);
    }

    #[test]
    fn partition_raw_keeps_existing_grouping_behavior() {
        use dt_common::meta::dt_data::{DtData, DtItem};
        use dt_common::meta::position::Position;

        let data = vec![
            DtItem {
                dt_data: DtData::Dml {
                    row_data: row(1, RowType::Insert),
                },
                position: Position::None,
                data_origin_node: String::new(),
            },
            DtItem {
                dt_data: DtData::Dml {
                    row_data: row(1, RowType::Insert),
                },
                position: Position::None,
                data_origin_node: String::new(),
            },
        ];

        let partitions = ChunkPartitioner::partition_raw(data).unwrap();

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].len(), 2);
    }

    #[test]
    fn partition_dml_adaptive_sorts_partitions_by_size_after_split() {
        let data = vec![
            row(1, RowType::Insert),
            row(1, RowType::Insert),
            row(1, RowType::Insert),
            row(1, RowType::Insert),
            row(1, RowType::Insert),
        ];

        let partitions = ChunkPartitioner::partition_dml(
            data,
            3,
            &config(ChunkPartitionerRebalanceStrategy::Adaptive),
        )
        .unwrap();

        let lengths: Vec<usize> = partitions.iter().map(Vec::len).collect();
        assert_eq!(lengths, vec![2, 2, 1]);
    }

    #[test]
    fn partition_dml_respects_min_partition_rows() {
        let data = vec![
            row(1, RowType::Insert),
            row(1, RowType::Insert),
            row(1, RowType::Insert),
        ];
        let mut config = config(ChunkPartitionerRebalanceStrategy::SplitLargeInsert);
        config.min_partition_rows = 2;

        let partitions = ChunkPartitioner::partition_dml(data, 4, &config).unwrap();

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].len(), 3);
    }

    #[test]
    fn partition_dml_splits_by_cost() {
        let data = vec![
            sized_row("schema", "tb", 1, RowType::Insert, 1),
            sized_row("schema", "tb", 1, RowType::Insert, 1),
            sized_row("schema", "tb", 1, RowType::Insert, 100),
            sized_row("schema", "tb", 1, RowType::Insert, 1),
        ];
        let mut config = config(ChunkPartitionerRebalanceStrategy::SplitLargeInsert);
        config.max_partitions_per_sinker = 1;

        let partitions = ChunkPartitioner::partition_dml(data, 2, &config).unwrap();

        let bytes: Vec<u64> = partitions
            .iter()
            .map(|partition| partition.iter().map(RowData::get_data_size).sum())
            .collect();
        assert_eq!(bytes, vec![101, 2]);
    }

    #[test]
    fn partition_dml_can_use_rows_as_cost() {
        let data = vec![
            sized_row("schema", "tb", 1, RowType::Insert, 1000),
            sized_row("schema", "tb", 2, RowType::Insert, 1),
            sized_row("schema", "tb", 2, RowType::Insert, 1),
        ];
        let mut config = config(ChunkPartitionerRebalanceStrategy::ChunkLargestFirst);
        config.cost = ChunkPartitionerRebalanceCost::Rows;

        let partitions = ChunkPartitioner::partition_dml(data, 2, &config).unwrap();

        assert_eq!(chunk_ids(&partitions), vec![vec![2, 2], vec![1]]);
    }

    #[test]
    fn partition_dml_rows_cost_does_not_tie_break_with_bytes() {
        let data = vec![
            sized_row("schema", "tb", 1, RowType::Insert, 1),
            sized_row("schema", "tb", 2, RowType::Insert, 1000),
        ];
        let mut config = config(ChunkPartitionerRebalanceStrategy::ChunkLargestFirst);
        config.cost = ChunkPartitionerRebalanceCost::Rows;

        let partitions = ChunkPartitioner::partition_dml(data, 2, &config).unwrap();

        assert_eq!(chunk_ids(&partitions), vec![vec![1], vec![2]]);
    }

    #[test]
    fn partition_dml_adaptive_splits_skewed_group_after_target_is_reached() {
        let data = vec![
            row(1, RowType::Insert),
            row(1, RowType::Insert),
            row(1, RowType::Insert),
            row(1, RowType::Insert),
            row(1, RowType::Insert),
            row(2, RowType::Insert),
        ];

        let mut config = config(ChunkPartitionerRebalanceStrategy::Adaptive);
        config.split_skew_ratio = 1.5;

        let partitions = ChunkPartitioner::partition_dml(data, 2, &config).unwrap();

        let lengths: Vec<usize> = partitions.iter().map(Vec::len).collect();
        assert_eq!(lengths, vec![3, 2, 1]);
    }
}
