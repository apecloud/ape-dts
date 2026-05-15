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
            let sch_tb_chunk = format!("{}.{}.{}", row_data.schema, row_data.tb, row_data.chunk_id);
            if let Some(sub_data) = sub_data_map.get_mut(&sch_tb_chunk) {
                sub_data.push(row_data);
            } else {
                sub_data_map.insert(sch_tb_chunk, vec![row_data]);
            }
        }

        // FIXME: only rebalance insert-type rows, other cases may cause data inconsistency.
        // For this is only used for snapshot tasks, which has only insert-type rows, so it's safe for now.
        let mut sub_data =
            Self::rebalance_partitions(sub_data_map.into_values().collect(), target_partitions);

        // sort by len desc, make sure the longest partition is sinked at first, which can improve the parallelism of sinking.
        sub_data.sort_by_key(|rows| std::cmp::Reverse(rows.len()));
        Ok(sub_data)
    }

    fn rebalance_partitions(
        mut sub_data: Vec<Vec<RowData>>,
        target_partitions: usize,
    ) -> Vec<Vec<RowData>> {
        if target_partitions <= 1 || sub_data.len() >= target_partitions {
            return sub_data;
        }

        while sub_data.len() < target_partitions {
            let Some(index) = sub_data
                .iter()
                .enumerate()
                .filter(|(_, rows)| rows.len() > 1)
                .max_by_key(|(_, rows)| rows.len())
                .map(|(index, _)| index)
            else {
                break;
            };

            let split_at = (sub_data[index].len() + 1) / 2;
            let tail = sub_data[index].split_off(split_at);
            sub_data.push(tail);
        }

        sub_data
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
