use std::collections::HashMap;

use dt_common::meta::{
    dt_data::{DtData, DtItem},
    row_data::RowData,
};

pub struct ChunkPartitioner {}

impl ChunkPartitioner {
    pub fn partition_dml(
        data: Vec<RowData>,
        target_partitions: usize,
    ) -> anyhow::Result<Vec<Vec<RowData>>> {
        let mut sub_data_map: HashMap<String, Vec<RowData>> = HashMap::new();
        for row_data in data {
            // One alloc per row for the composite key (same cost as the
            // original implementation). Using a tuple key would double the
            // allocations because std HashMap entry() takes the key by value.
            let sch_tb_chunk = format!("{}.{}.{}", row_data.schema, row_data.tb, row_data.chunk_id);
            sub_data_map.entry(sch_tb_chunk).or_default().push(row_data);
        }

        // FIXME: only rebalance insert-type rows, other cases may cause data inconsistency.
        // For this is only used for snapshot tasks, which has only insert-type rows, so it's safe for now.
        let (mut sub_data, bytes) =
            Self::rebalance_partitions(sub_data_map.into_values().collect(), target_partitions);

        // Sort heaviest-first so the long tail starts first and parallel sinkers stay busy.
        // Sorting an index vector (rather than the partitions themselves) keeps comparisons
        // O(1) by reusing the byte sizes we already computed during rebalance — otherwise
        // sort_by_key would re-sum each partition O(log n) times.
        let mut order: Vec<usize> = (0..sub_data.len()).collect();
        order.sort_unstable_by_key(|&i| std::cmp::Reverse(bytes[i]));
        let mut sorted: Vec<Vec<RowData>> = Vec::with_capacity(sub_data.len());
        for i in order {
            sorted.push(std::mem::take(&mut sub_data[i]));
        }
        Ok(sorted)
    }

    /// Split partitions until each is roughly an even share of total bytes.
    ///
    /// We keep splitting the heaviest partition while either:
    /// - we still have fewer partitions than `target_partitions`, or
    /// - the heaviest partition is still more than 2× the ideal byte share.
    ///
    /// Splitting is bounded at `target_partitions * 4` to avoid pathological
    /// over-splitting when one row dominates the total weight.
    /// Bytes (rather than row counts) drive the decision because per-row size
    /// can vary by orders of magnitude (think small int rows vs. blob/json),
    /// so balancing by row count would still leave a heavy long tail.
    ///
    /// Returns the partitions alongside their byte sizes so callers can sort
    /// without re-summing each partition.
    fn rebalance_partitions(
        mut sub_data: Vec<Vec<RowData>>,
        target_partitions: usize,
    ) -> (Vec<Vec<RowData>>, Vec<u64>) {
        let mut bytes: Vec<u64> = sub_data.iter().map(|p| Self::partition_bytes(p)).collect();
        if target_partitions <= 1 {
            return (sub_data, bytes);
        }

        let total: u64 = bytes.iter().sum();
        let ideal = total / target_partitions as u64;
        let imbalance_threshold = ideal.saturating_mul(2).max(1);
        let max_partitions = target_partitions.saturating_mul(4);

        loop {
            if sub_data.len() >= max_partitions {
                break;
            }

            let Some((idx, &max_bytes)) = bytes
                .iter()
                .enumerate()
                .filter(|(i, _)| sub_data[*i].len() > 1)
                // Tiebreak by row count so zero-byte partitions still split deterministically.
                .max_by_key(|(i, b)| (**b, sub_data[*i].len()))
            else {
                break;
            };

            if sub_data.len() >= target_partitions && max_bytes <= imbalance_threshold {
                break;
            }

            let split_at = Self::byte_median_split(&sub_data[idx]);
            if split_at == 0 || split_at >= sub_data[idx].len() {
                break;
            }

            let tail = sub_data[idx].split_off(split_at);
            let tail_bytes = Self::partition_bytes(&tail);
            bytes[idx] -= tail_bytes;
            bytes.push(tail_bytes);
            sub_data.push(tail);
        }

        (sub_data, bytes)
    }

    fn partition_bytes(rows: &[RowData]) -> u64 {
        rows.iter().map(|r| r.get_data_size()).sum()
    }

    /// Pick a split index so the two halves carry roughly equal byte weight.
    /// Always returns a value in `[1, rows.len() - 1]` so neither half is empty.
    /// Falls back to the row-count midpoint when every row reports zero size.
    fn byte_median_split(rows: &[RowData]) -> usize {
        let total = Self::partition_bytes(rows);
        if total == 0 {
            return ((rows.len() + 1) / 2).clamp(1, rows.len() - 1);
        }
        let half = total / 2;
        let mut acc: u64 = 0;
        for (i, r) in rows.iter().enumerate().take(rows.len() - 1) {
            acc += r.get_data_size();
            if acc >= half {
                return (i + 1).max(1);
            }
        }
        rows.len() - 1
    }

    pub fn partition_raw(data: Vec<DtItem>) -> anyhow::Result<Vec<Vec<DtItem>>> {
        // DDL / DCL / heartbeat / etc. all share one bucket so they remain ordered
        // relative to each other; only DML is grouped by chunk for parallelism.
        let mut sub_data_map: HashMap<String, Vec<DtItem>> = HashMap::new();
        let mut default_bucket: Vec<DtItem> = Vec::new();
        for item in data {
            if let DtData::Dml { row_data } = &item.dt_data {
                let sch_tb_chunk =
                    format!("{}.{}.{}", row_data.schema, row_data.tb, row_data.chunk_id);
                sub_data_map.entry(sch_tb_chunk).or_default().push(item);
            } else {
                default_bucket.push(item);
            }
        }

        let mut result: Vec<Vec<DtItem>> = sub_data_map.into_values().collect();
        if !default_bucket.is_empty() {
            result.push(default_bucket);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dt_common::meta::row_type::RowType;

    fn row(id: u64, row_type: RowType) -> RowData {
        RowData::new(
            "schema".to_string(),
            "tb".to_string(),
            id,
            row_type,
            None,
            None,
        )
    }

    #[test]
    fn partition_dml_splits_large_insert_group_when_too_few_partitions() {
        let data = vec![
            row(1, RowType::Insert),
            row(1, RowType::Insert),
            row(1, RowType::Insert),
            row(1, RowType::Insert),
        ];

        let partitions = ChunkPartitioner::partition_dml(data, 4).unwrap();

        assert_eq!(partitions.len(), 4);
        assert!(partitions.iter().all(|partition| partition.len() == 1));
    }

    #[test]
    fn partition_dml_sorts_partitions_by_size() {
        let data = vec![
            row(1, RowType::Insert),
            row(1, RowType::Insert),
            row(1, RowType::Insert),
            row(1, RowType::Insert),
            row(1, RowType::Insert),
        ];

        let partitions = ChunkPartitioner::partition_dml(data, 3).unwrap();

        let lengths: Vec<usize> = partitions.iter().map(Vec::len).collect();
        assert_eq!(lengths, vec![2, 2, 1]);
    }
}
