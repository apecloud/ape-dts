use std::collections::HashMap;

use dt_common::meta::row_data::RowData;

pub struct RowDataPartitioner {
    pub extractor_parallel_size: usize,
    pub consider_chunk: bool,
}

impl RowDataPartitioner {
    pub fn new(extractor_parallel_size: usize, consider_chunk: bool) -> Self {
        Self {
            extractor_parallel_size,
            consider_chunk,
        }
    }

    pub fn partition(&self, data: Vec<RowData>) -> anyhow::Result<Vec<Vec<RowData>>> {
        let mut sub_data_map: HashMap<String, Vec<RowData>> = HashMap::new();
        for row_data in data {
            let full_tb = format!("{}.{}.{}", row_data.schema, row_data.tb, row_data.chunk_id);
            if let Some(sub_data) = sub_data_map.get_mut(&full_tb) {
                sub_data.push(row_data);
            } else {
                sub_data_map.insert(full_tb, vec![row_data]);
            }
        }

        Ok(sub_data_map.into_values().collect())
    }
}
