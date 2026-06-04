use std::collections::HashMap;

use dt_common::{
    config::parallelizer_config::{
        ChunkPartitionerRebalanceConfig, ChunkPartitionerRebalanceCost,
        ChunkPartitionerRebalanceStrategy,
    },
    meta::{
        dt_data::{DtData, DtItem},
        row_data::RowData,
    },
};

pub struct ChunkPartitioner {}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ChunkKey<'a> {
    pub schema: &'a str,
    pub tb: &'a str,
    pub chunk_id: u64,
}

struct GroupPlan {
    // Row indexes are stored in logical chunk order. Rows for the same chunk may be non-contiguous
    // in the original data Vec, so a prefix over the original Vec cannot answer group-local splits.
    row_indexes: Vec<usize>,
    // Only Bytes cost needs prefix bytes. Rows cost keeps this empty to avoid per-row u64 storage.
    prefix_bytes: Option<Vec<u64>>,
}

struct MergedGroupPlan {
    last_chunk_id: u64,
    group_indexes: Vec<usize>,
    prefix_rows: Vec<usize>,
    rows: usize,
}

#[derive(Clone, Copy, Debug)]
struct PartitionPlan {
    group_index: usize,
    // start/end are indexes into GroupPlan.row_indexes, not original RowData indexes.
    // RowData is moved only once in materialize_partitions after rebalance is finalized.
    start: usize,
    end: usize,
    bytes: u64,
}

#[derive(Clone, Copy, Debug)]
struct MergedPartitionPlan {
    merged_group_index: usize,
    start: usize,
    end: usize,
}

impl GroupPlan {
    fn new(use_bytes: bool) -> Self {
        Self {
            row_indexes: Vec::new(),
            prefix_bytes: use_bytes.then_some(vec![0]),
        }
    }

    fn push(&mut self, row_index: usize, bytes: u64) {
        self.row_indexes.push(row_index);
        if let Some(prefix_bytes) = &mut self.prefix_bytes {
            let next_bytes = prefix_bytes.last().copied().unwrap_or(0) + bytes;
            prefix_bytes.push(next_bytes);
        }
    }

    fn rows(&self) -> usize {
        self.row_indexes.len()
    }

    fn bytes(&self, start: usize, end: usize) -> u64 {
        self.prefix_bytes
            .as_ref()
            .map(|prefix_bytes| prefix_bytes[end].saturating_sub(prefix_bytes[start]))
            .unwrap_or(0)
    }
}

impl MergedGroupPlan {
    fn new(group_index: usize, key: ChunkKey<'_>, group_rows: usize) -> Self {
        Self {
            last_chunk_id: key.chunk_id,
            group_indexes: vec![group_index],
            prefix_rows: vec![0, group_rows],
            rows: group_rows,
        }
    }

    fn can_append(&self, last_key: ChunkKey<'_>, key: ChunkKey<'_>) -> bool {
        last_key.schema == key.schema && last_key.tb == key.tb && self.last_chunk_id < key.chunk_id
    }

    fn append(&mut self, group_index: usize, key: ChunkKey<'_>, group_rows: usize) {
        self.last_chunk_id = key.chunk_id;
        self.group_indexes.push(group_index);
        self.rows += group_rows;
        self.prefix_rows.push(self.rows);
    }
}

impl PartitionPlan {
    fn new(group_index: usize, group: &GroupPlan, cost: &ChunkPartitionerRebalanceCost) -> Self {
        let rows = group.rows();
        let bytes = match cost {
            ChunkPartitionerRebalanceCost::Bytes => group.bytes(0, rows),
            ChunkPartitionerRebalanceCost::Rows => 0,
        };
        Self {
            group_index,
            start: 0,
            end: rows,
            bytes,
        }
    }

    fn rows(&self) -> usize {
        self.end - self.start
    }

    fn can_split(&self, min_partition_rows: usize) -> bool {
        self.rows() >= min_partition_rows.saturating_mul(2)
    }

    fn cost_key(&self, cost: &ChunkPartitionerRebalanceCost) -> (u64, u64) {
        match cost {
            ChunkPartitionerRebalanceCost::Bytes => (self.bytes, self.rows() as u64),
            ChunkPartitionerRebalanceCost::Rows => (self.rows() as u64, self.rows() as u64),
        }
    }

    fn safe_primary_cost(&self, cost: &ChunkPartitionerRebalanceCost) -> u64 {
        match cost {
            ChunkPartitionerRebalanceCost::Bytes => self.bytes.max(self.rows() as u64),
            ChunkPartitionerRebalanceCost::Rows => self.rows() as u64,
        }
    }

    fn split(
        &mut self,
        groups: &[GroupPlan],
        cost: &ChunkPartitionerRebalanceCost,
        min_partition_rows: usize,
    ) -> Option<PartitionPlan> {
        let original_split_at = self.split_at(groups, cost);
        // Align the left side to full sinker batches when min_partition_rows is used as
        // the sinker batch size. The right side is still checked against the same minimum.
        let split_at = self.align_split_at(original_split_at, min_partition_rows);
        let left_rows = split_at - self.start;
        let right_rows = self.end - split_at;
        if left_rows < min_partition_rows || right_rows < min_partition_rows {
            return None;
        }
        let left_bytes = match cost {
            ChunkPartitionerRebalanceCost::Bytes => {
                Some(groups[self.group_index].bytes(self.start, split_at))
            }
            ChunkPartitionerRebalanceCost::Rows => None,
        };
        Some(self.split_off(split_at, left_bytes, cost))
    }

    fn split_at(&self, groups: &[GroupPlan], cost: &ChunkPartitionerRebalanceCost) -> usize {
        match cost {
            ChunkPartitionerRebalanceCost::Bytes => self.split_at_by_bytes(groups),
            ChunkPartitionerRebalanceCost::Rows => self.start + (self.rows() + 1) / 2,
        }
    }

    fn align_split_at(&self, split_at: usize, min_partition_rows: usize) -> usize {
        if min_partition_rows <= 1 {
            return split_at;
        }

        let min_left_rows = min_partition_rows;
        let max_left_rows = self.rows().saturating_sub(min_partition_rows);
        if min_left_rows > max_left_rows {
            return split_at;
        }

        let left_rows = split_at - self.start;
        let lower = left_rows / min_partition_rows * min_partition_rows;
        let upper = lower.saturating_add(min_partition_rows);
        match (
            (min_left_rows..=max_left_rows).contains(&lower),
            (min_left_rows..=max_left_rows).contains(&upper),
        ) {
            (true, true) => {
                if left_rows - lower <= upper - left_rows {
                    self.start + lower
                } else {
                    self.start + upper
                }
            }
            (true, false) => self.start + lower,
            (false, true) => self.start + upper,
            (false, false) => self.start + left_rows.clamp(min_left_rows, max_left_rows),
        }
    }

    fn split_at_by_bytes(&self, groups: &[GroupPlan]) -> usize {
        if self.bytes == 0 {
            return self.start + (self.rows() + 1) / 2;
        }

        let group = &groups[self.group_index];
        let prefix_bytes = group.prefix_bytes.as_ref().unwrap();
        let start_bytes = prefix_bytes[self.start];
        let target_bytes = start_bytes + self.bytes / 2;
        let search_start = self.start + 1;
        let search_end = self.end;
        let search_range = &prefix_bytes[search_start..search_end];
        let current_split_at =
            search_start + search_range.partition_point(|bytes| *bytes < target_bytes);
        let current_split_at = current_split_at.min(self.end - 1);
        let previous_split_at = if current_split_at > self.start + 1 {
            current_split_at - 1
        } else {
            current_split_at
        };

        let previous_left_bytes = group.bytes(self.start, previous_split_at);
        let current_left_bytes = group.bytes(self.start, current_split_at);
        let target_left_bytes = target_bytes - start_bytes;
        if previous_split_at != current_split_at
            && target_left_bytes.abs_diff(previous_left_bytes)
                <= target_left_bytes.abs_diff(current_left_bytes)
        {
            previous_split_at
        } else {
            current_split_at
        }
    }

    fn split_off(
        &mut self,
        split_at: usize,
        left_bytes: Option<u64>,
        cost: &ChunkPartitionerRebalanceCost,
    ) -> PartitionPlan {
        let original_bytes = self.bytes;
        let original_end = self.end;

        match cost {
            ChunkPartitionerRebalanceCost::Bytes => {
                let left_bytes = left_bytes.unwrap_or(0);
                let tail_bytes = original_bytes.saturating_sub(left_bytes);
                self.end = split_at;
                self.bytes = left_bytes;
                PartitionPlan {
                    group_index: self.group_index,
                    start: split_at,
                    end: original_end,
                    bytes: tail_bytes,
                }
            }

            ChunkPartitionerRebalanceCost::Rows => {
                self.end = split_at;
                self.bytes = 0;
                PartitionPlan {
                    group_index: self.group_index,
                    start: split_at,
                    end: original_end,
                    bytes: 0,
                }
            }
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

        let index_capacity = target_partitions.max(1);
        let mut group_indexes: HashMap<ChunkKey<'_>, usize> =
            HashMap::with_capacity(index_capacity);
        let mut groups: Vec<GroupPlan> = Vec::with_capacity(index_capacity);
        let mut group_keys: Vec<ChunkKey<'_>> = Vec::with_capacity(index_capacity);
        let use_bytes = matches!(config.cost, ChunkPartitionerRebalanceCost::Bytes)
            && !matches!(
                config.strategy,
                ChunkPartitionerRebalanceStrategy::MinRows
                    | ChunkPartitionerRebalanceStrategy::GroupEven
            );
        // Snapshot rows are usually chunk-contiguous. Reuse the previous group index to avoid
        // hashing schema/table for every row in a long run from the same logical chunk.
        let mut last_group: Option<(ChunkKey<'_>, usize)> = None;

        for (row_index, row_data) in data.iter().enumerate() {
            // Keep each logical snapshot chunk together before any strategy-specific rebalance.
            let key = ChunkKey {
                schema: row_data.schema.as_str(),
                tb: row_data.tb.as_str(),
                chunk_id: row_data.chunk_id,
            };

            let index = match last_group {
                Some((last_key, last_index)) if last_key == key => last_index,
                _ => {
                    let index = match group_indexes.get(&key).copied() {
                        Some(index) => index,
                        None => {
                            let index = groups.len();
                            group_indexes.insert(key, index);
                            groups.push(GroupPlan::new(use_bytes));
                            group_keys.push(key);
                            index
                        }
                    };
                    last_group = Some((key, index));
                    index
                }
            };

            let bytes = if use_bytes {
                row_data.get_data_size()
            } else {
                0
            };
            groups[index].push(row_index, bytes);
        }
        // group_indexes holds borrowed keys into data. Drop it before moving RowData out of data.
        drop(group_indexes);

        if matches!(
            config.strategy,
            ChunkPartitionerRebalanceStrategy::MinRows
                | ChunkPartitionerRebalanceStrategy::GroupEven
        ) {
            let merged_groups = Self::merge_contiguous_groups(&groups, &group_keys);
            let partitions = match config.strategy {
                ChunkPartitionerRebalanceStrategy::MinRows => {
                    Self::partition_merged_by_min_rows(&merged_groups, config.min_partition_rows)
                }
                _ => Self::partition_merged_group_even(
                    &merged_groups,
                    target_partitions,
                    config.min_partition_rows,
                ),
            };
            drop(group_keys);
            return Ok(Self::materialize_merged_partitions(
                data,
                &groups,
                &merged_groups,
                partitions,
            ));
        }

        let partitions = groups
            .iter()
            .enumerate()
            .map(|(group_index, group)| PartitionPlan::new(group_index, group, &config.cost))
            .collect();
        drop(group_keys);

        let partitions =
            Self::rebalance_partition_items(partitions, &groups, target_partitions, config);
        Ok(Self::materialize_partitions(data, &groups, partitions))
    }

    fn rebalance_partition_items(
        partitions: Vec<PartitionPlan>,
        groups: &[GroupPlan],
        target_partitions: usize,
        config: &ChunkPartitionerRebalanceConfig,
    ) -> Vec<PartitionPlan> {
        match config.strategy {
            ChunkPartitionerRebalanceStrategy::None => partitions,
            ChunkPartitionerRebalanceStrategy::ChunkLargestFirst => {
                Self::sort_by_largest_first(partitions, config)
            }
            ChunkPartitionerRebalanceStrategy::SplitLargeInsert => Self::sort_by_largest_first(
                Self::split_large_insert_partitions(
                    partitions,
                    groups,
                    target_partitions,
                    config,
                    true,
                ),
                config,
            ),
            ChunkPartitionerRebalanceStrategy::Adaptive => Self::sort_by_largest_first(
                Self::split_large_insert_partitions(
                    partitions,
                    groups,
                    target_partitions,
                    config,
                    false,
                ),
                config,
            ),
            ChunkPartitionerRebalanceStrategy::MinRows
            | ChunkPartitionerRebalanceStrategy::GroupEven => partitions,
        }
    }

    fn merge_contiguous_groups(
        groups: &[GroupPlan],
        group_keys: &[ChunkKey<'_>],
    ) -> Vec<MergedGroupPlan> {
        let mut merged_groups: Vec<MergedGroupPlan> = Vec::new();
        let mut last_merged_key: Option<ChunkKey<'_>> = None;
        let mut sorted_group_indexes: Vec<usize> = (0..group_keys.len()).collect();
        sorted_group_indexes.sort_by(|left, right| {
            let left_key = group_keys[*left];
            let right_key = group_keys[*right];
            (left_key.schema, left_key.tb, left_key.chunk_id).cmp(&(
                right_key.schema,
                right_key.tb,
                right_key.chunk_id,
            ))
        });

        for group_index in sorted_group_indexes {
            let key = group_keys[group_index];
            let group_rows = groups[group_index].rows();
            if let Some(last) = merged_groups.last_mut() {
                if last_merged_key.is_some_and(|last_key| last.can_append(last_key, key)) {
                    last.append(group_index, key, group_rows);
                    last_merged_key = Some(key);
                    continue;
                }
            }
            merged_groups.push(MergedGroupPlan::new(group_index, key, group_rows));
            last_merged_key = Some(key);
        }
        merged_groups
    }

    fn partition_merged_by_min_rows(
        merged_groups: &[MergedGroupPlan],
        min_partition_rows: usize,
    ) -> Vec<MergedPartitionPlan> {
        let target_rows = min_partition_rows.max(1);
        let mut partitions = Vec::new();
        for (merged_group_index, merged_group) in merged_groups.iter().enumerate() {
            let mut start = 0;
            while start < merged_group.rows {
                let end = start.saturating_add(target_rows).min(merged_group.rows);
                partitions.push(MergedPartitionPlan {
                    merged_group_index,
                    start,
                    end,
                });
                start = end;
            }
        }
        partitions
    }

    fn partition_merged_group_even(
        merged_groups: &[MergedGroupPlan],
        target_partitions: usize,
        min_partition_rows: usize,
    ) -> Vec<MergedPartitionPlan> {
        let mut partitions = Vec::new();
        let target_partitions = target_partitions.max(1);
        for (merged_group_index, merged_group) in merged_groups.iter().enumerate() {
            let mut remaining_rows = merged_group.rows;
            let mut remaining_parts = target_partitions.min(merged_group.rows).max(1);
            let mut start = 0;

            while remaining_parts > 0 {
                let len = Self::aligned_partition_len(
                    remaining_rows,
                    remaining_parts,
                    min_partition_rows,
                );
                let end = start + len;
                partitions.push(MergedPartitionPlan {
                    merged_group_index,
                    start,
                    end,
                });
                start = end;
                remaining_rows -= len;
                remaining_parts -= 1;
            }
        }
        partitions
    }

    fn aligned_partition_len(
        remaining_rows: usize,
        remaining_parts: usize,
        min_partition_rows: usize,
    ) -> usize {
        if remaining_parts <= 1 {
            return remaining_rows;
        }

        let ideal = remaining_rows.div_ceil(remaining_parts);
        if min_partition_rows <= 1 {
            return ideal;
        }

        let min_len = 1;
        let max_len = remaining_rows - (remaining_parts - 1);
        let lower = ideal / min_partition_rows * min_partition_rows;
        let upper = lower.saturating_add(min_partition_rows);
        match (
            (min_len..=max_len).contains(&lower),
            (min_len..=max_len).contains(&upper),
        ) {
            (true, true) => {
                if ideal - lower <= upper - ideal {
                    lower
                } else {
                    upper
                }
            }
            (true, false) => lower,
            (false, true) => upper,
            (false, false) => ideal.clamp(min_len, max_len),
        }
    }

    fn sort_by_largest_first(
        mut partitions: Vec<PartitionPlan>,
        config: &ChunkPartitionerRebalanceConfig,
    ) -> Vec<PartitionPlan> {
        partitions.sort_by(|left, right| {
            right
                .cost_key(&config.cost)
                .cmp(&left.cost_key(&config.cost))
        });
        partitions
    }

    fn split_large_insert_partitions(
        mut partitions: Vec<PartitionPlan>,
        groups: &[GroupPlan],
        target_partitions: usize,
        config: &ChunkPartitionerRebalanceConfig,
        force_split: bool,
    ) -> Vec<PartitionPlan> {
        // Snapshot parallelizer sends insert-only rows, so split is safe for the current caller.

        let max_partitions =
            Self::max_partitions(&partitions, target_partitions, config).max(target_partitions);
        // Total work does not change when a partition is split; keep one cached value for all
        // adaptive skew checks in this drain batch.
        let total_cost = Self::total_safe_primary_cost(&partitions, config);
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
                && !Self::is_partition_skewed(
                    &partitions[index],
                    total_cost,
                    target_partitions,
                    config,
                )
            {
                break;
            }

            let Some(tail) =
                partitions[index].split(groups, &config.cost, config.min_partition_rows)
            else {
                break;
            };
            partitions.push(tail);
        }

        partitions
    }

    fn max_partitions(
        partitions: &[PartitionPlan],
        target_partitions: usize,
        config: &ChunkPartitionerRebalanceConfig,
    ) -> usize {
        let total_rows: usize = partitions.iter().map(PartitionPlan::rows).sum();
        // Derive the effective cap from the current drain batch to avoid over-splitting.
        let max_by_rows = (total_rows / config.min_partition_rows.max(1)).max(1);
        let max_by_config = target_partitions.saturating_mul(config.max_partitions_per_sinker);
        max_by_rows.min(max_by_config)
    }

    fn total_safe_primary_cost(
        partitions: &[PartitionPlan],
        config: &ChunkPartitionerRebalanceConfig,
    ) -> u64 {
        partitions
            .iter()
            .map(|partition| partition.safe_primary_cost(&config.cost))
            .sum()
    }

    fn is_partition_skewed(
        largest: &PartitionPlan,
        total_cost: u64,
        target_partitions: usize,
        config: &ChunkPartitionerRebalanceConfig,
    ) -> bool {
        // Compare the largest partition with ideal per-sinker work, not average partition size.
        let avg_cost_per_sinker =
            (total_cost / target_partitions.max(1) as u64).max(config.min_partition_rows as u64);
        let largest_cost = largest.safe_primary_cost(&config.cost);

        // Example: ratio=2.0 means "split if the largest partition is > 2x ideal work".
        (largest_cost as f64) > (avg_cost_per_sinker as f64 * config.split_skew_ratio)
    }

    fn materialize_partitions(
        data: Vec<RowData>,
        groups: &[GroupPlan],
        partitions: Vec<PartitionPlan>,
    ) -> Vec<Vec<RowData>> {
        let mut row_to_partition = vec![0; data.len()];
        for (partition_index, partition) in partitions.iter().enumerate() {
            let group = &groups[partition.group_index];
            for row_index in &group.row_indexes[partition.start..partition.end] {
                row_to_partition[*row_index] = partition_index;
            }
        }

        // Plans are built from row indexes in data, and split only subdivides existing ranges.
        // Therefore every row has exactly one final partition by construction.
        let mut sub_data: Vec<Vec<RowData>> = partitions
            .iter()
            .map(|partition| Vec::with_capacity(partition.rows()))
            .collect();
        for (row_index, row_data) in data.into_iter().enumerate() {
            sub_data[row_to_partition[row_index]].push(row_data);
        }
        sub_data
    }

    fn materialize_merged_partitions(
        data: Vec<RowData>,
        groups: &[GroupPlan],
        merged_groups: &[MergedGroupPlan],
        partitions: Vec<MergedPartitionPlan>,
    ) -> Vec<Vec<RowData>> {
        let mut row_to_partition = vec![0; data.len()];
        for (partition_index, partition) in partitions.iter().enumerate() {
            let merged_group = &merged_groups[partition.merged_group_index];
            Self::mark_merged_partition_rows(
                &mut row_to_partition,
                groups,
                merged_group,
                partition.start,
                partition.end,
                partition_index,
            );
        }

        let mut sub_data: Vec<Vec<RowData>> = partitions
            .iter()
            .map(|partition| Vec::with_capacity(partition.end - partition.start))
            .collect();
        for (row_index, row_data) in data.into_iter().enumerate() {
            sub_data[row_to_partition[row_index]].push(row_data);
        }
        sub_data
    }

    fn mark_merged_partition_rows(
        row_to_partition: &mut [usize],
        groups: &[GroupPlan],
        merged_group: &MergedGroupPlan,
        start: usize,
        end: usize,
        partition_index: usize,
    ) {
        if start >= end {
            return;
        }

        let start_group = merged_group
            .prefix_rows
            .partition_point(|rows| *rows <= start)
            .saturating_sub(1);
        let end_group = merged_group
            .prefix_rows
            .partition_point(|rows| *rows < end)
            .saturating_sub(1);

        for merged_group_offset in start_group..=end_group {
            let group_start = merged_group.prefix_rows[merged_group_offset];
            let group_end = merged_group.prefix_rows[merged_group_offset + 1];
            let slice_start = start.max(group_start) - group_start;
            let slice_end = end.min(group_end) - group_start;
            let group_index = merged_group.group_indexes[merged_group_offset];
            for row_index in &groups[group_index].row_indexes[slice_start..slice_end] {
                row_to_partition[*row_index] = partition_index;
            }
        }
    }

    pub fn partition_raw(data: Vec<DtItem>) -> anyhow::Result<Vec<Vec<DtItem>>> {
        let mut sub_data_map: HashMap<String, Vec<DtItem>> = HashMap::new();
        let default_key = "default".to_string();
        for item in data {
            if let DtData::Dml { row_data } = &item.dt_data {
                let sch_tb_chunk =
                    format!("{}.{}.{}", row_data.schema, row_data.tb, row_data.chunk_id);
                if let Some(sub_data) = sub_data_map.get_mut(&sch_tb_chunk) {
                    sub_data.push(item);
                } else {
                    sub_data_map.insert(sch_tb_chunk, vec![item]);
                }
            } else if let Some(sub_data) = sub_data_map.get_mut(&default_key) {
                sub_data.push(item);
            } else {
                sub_data_map.insert(default_key.clone(), vec![item]);
            }
        }

        Ok(sub_data_map.into_values().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dt_common::config::parallelizer_config::ChunkPartitionerRebalanceStrategy;
    use dt_common::meta::row_type::RowType;

    fn config(strategy: ChunkPartitionerRebalanceStrategy) -> ChunkPartitionerRebalanceConfig {
        ChunkPartitionerRebalanceConfig {
            strategy,
            cost: ChunkPartitionerRebalanceCost::Rows,
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

    fn data_sizes(partitions: &[Vec<RowData>]) -> Vec<Vec<usize>> {
        partitions
            .iter()
            .map(|partition| partition.iter().map(|row| row.data_size).collect())
            .collect()
    }

    fn partition_lengths(partitions: &[Vec<RowData>]) -> Vec<usize> {
        partitions.iter().map(Vec::len).collect()
    }

    fn group_plan(data_sizes: &[usize], use_bytes: bool) -> GroupPlan {
        let mut group = GroupPlan::new(use_bytes);
        for (row_index, data_size) in data_sizes.iter().enumerate() {
            group.push(row_index, *data_size as u64);
        }
        group
    }

    #[test]
    fn partition_split_aligns_left_rows_to_min_partition_rows() {
        let groups = vec![group_plan(&[1, 1, 1, 1, 1], false)];
        let mut partition = PartitionPlan::new(0, &groups[0], &ChunkPartitionerRebalanceCost::Rows);

        let tail = partition
            .split(&groups, &ChunkPartitionerRebalanceCost::Rows, 2)
            .unwrap();

        assert_eq!(partition.rows(), 2);
        assert_eq!(tail.rows(), 3);
        assert_eq!(partition.rows() % 2, 0);
    }

    #[test]
    fn partition_split_recalculates_left_bytes_after_alignment() {
        let groups = vec![group_plan(&[5, 5, 90, 10, 10], true)];
        let mut partition =
            PartitionPlan::new(0, &groups[0], &ChunkPartitionerRebalanceCost::Bytes);

        let tail = partition
            .split(&groups, &ChunkPartitionerRebalanceCost::Bytes, 2)
            .unwrap();

        assert_eq!(partition.rows(), 2);
        assert_eq!(tail.rows(), 3);
        assert_eq!(partition.bytes, 10);
        assert_eq!(tail.bytes, 110);
        assert_eq!(partition.rows() % 2, 0);
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

        let mut config = config(ChunkPartitionerRebalanceStrategy::ChunkLargestFirst);
        config.cost = ChunkPartitionerRebalanceCost::Bytes;

        let partitions = ChunkPartitioner::partition_dml(data, 2, &config).unwrap();

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

        assert_eq!(
            data_sizes(&partitions),
            vec![vec![1, 2, 3, 4], vec![5, 6, 7, 8]]
        );
        assert_eq!(partitions.iter().map(Vec::len).sum::<usize>(), 8);
        assert!(partitions
            .iter()
            .all(|partition| partition.iter().all(|row| row.chunk_id == 1)));
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
    fn partition_dml_keeps_same_schema_table_chunk_in_one_partition() {
        let data = vec![
            sized_row("schema_1", "tb_1", 7, RowType::Insert, 1),
            sized_row("schema_2", "tb_1", 7, RowType::Insert, 1),
            sized_row("schema_1", "tb_1", 7, RowType::Insert, 1),
            sized_row("schema_1", "tb_2", 7, RowType::Insert, 1),
            sized_row("schema_1", "tb_1", 8, RowType::Insert, 1),
            sized_row("schema_1", "tb_2", 7, RowType::Insert, 1),
        ];

        let partitions = ChunkPartitioner::partition_dml(
            data,
            8,
            &config(ChunkPartitionerRebalanceStrategy::None),
        )
        .unwrap();

        let partition_keys: Vec<Vec<(&str, &str, u64)>> = partitions
            .iter()
            .map(|partition| {
                partition
                    .iter()
                    .map(|row| (row.schema.as_str(), row.tb.as_str(), row.chunk_id))
                    .collect()
            })
            .collect();
        assert_eq!(
            partition_keys,
            vec![
                vec![("schema_1", "tb_1", 7), ("schema_1", "tb_1", 7)],
                vec![("schema_2", "tb_1", 7)],
                vec![("schema_1", "tb_2", 7), ("schema_1", "tb_2", 7)],
                vec![("schema_1", "tb_1", 8)],
            ]
        );
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
        config.cost = ChunkPartitionerRebalanceCost::Bytes;
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

    #[test]
    fn partition_dml_min_rows_merges_ordered_chunks_then_splits_by_min_rows() {
        let data = vec![
            row(1, RowType::Insert),
            row(1, RowType::Insert),
            row(3, RowType::Insert),
            row(3, RowType::Insert),
            row(5, RowType::Insert),
            row(5, RowType::Insert),
            row(4, RowType::Insert),
            row(4, RowType::Insert),
        ];
        let mut config = config(ChunkPartitionerRebalanceStrategy::MinRows);
        config.min_partition_rows = 3;

        let partitions = ChunkPartitioner::partition_dml(data, 2, &config).unwrap();

        assert_eq!(partition_lengths(&partitions), vec![3, 3, 2]);
        assert_eq!(
            chunk_ids(&partitions),
            vec![vec![1, 1, 3], vec![3, 4, 4], vec![5, 5]]
        );
    }

    #[test]
    fn partition_dml_group_even_splits_each_merged_group_by_target_partitions() {
        let data = (0..10)
            .map(|index| row(index / 2 + 1, RowType::Insert))
            .collect();
        let mut config = config(ChunkPartitionerRebalanceStrategy::GroupEven);
        config.min_partition_rows = 3;

        let partitions = ChunkPartitioner::partition_dml(data, 3, &config).unwrap();

        assert_eq!(partition_lengths(&partitions), vec![3, 3, 4]);
        assert_eq!(
            chunk_ids(&partitions),
            vec![vec![1, 1, 2], vec![2, 3, 3], vec![4, 4, 5, 5]]
        );
    }

    #[test]
    fn partition_dml_group_even_merges_ordered_chunks_without_crossing_tables() {
        let data = vec![
            sized_row("schema", "tb", 1, RowType::Insert, 1),
            sized_row("schema", "tb", 2, RowType::Insert, 1),
            sized_row("schema", "tb", 4, RowType::Insert, 1),
            sized_row("schema", "tb", 4, RowType::Insert, 1),
            sized_row("schema", "tb_2", 5, RowType::Insert, 1),
            sized_row("schema", "tb_2", 6, RowType::Insert, 1),
        ];
        let mut config = config(ChunkPartitionerRebalanceStrategy::GroupEven);
        config.min_partition_rows = 2;

        let partitions = ChunkPartitioner::partition_dml(data, 2, &config).unwrap();

        assert_eq!(partition_lengths(&partitions), vec![2, 2, 1, 1]);
        let partition_keys: Vec<Vec<(&str, &str, u64)>> = partitions
            .iter()
            .map(|partition| {
                partition
                    .iter()
                    .map(|row| (row.schema.as_str(), row.tb.as_str(), row.chunk_id))
                    .collect()
            })
            .collect();
        assert_eq!(
            partition_keys,
            vec![
                vec![("schema", "tb", 1), ("schema", "tb", 2)],
                vec![("schema", "tb", 4), ("schema", "tb", 4)],
                vec![("schema", "tb_2", 5)],
                vec![("schema", "tb_2", 6)],
            ]
        );
    }
}
