use std::collections::HashMap;

use dt_common::meta::row_data::RowData;

pub(crate) struct StringKeyBasicPartitioner;

impl StringKeyBasicPartitioner {
    pub(crate) fn partition_dml(
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

        let mut sub_data =
            Self::rebalance_partitions(sub_data_map.into_values().collect(), target_partitions);
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
}
