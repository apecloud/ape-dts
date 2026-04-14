use std::collections::HashMap;

use dt_common::meta::{dt_data::{DtData, DtItem}, row_data::RowData};

pub struct TablePartitioner {}

impl TablePartitioner {
    pub fn partition_dml(data: Vec<RowData>) -> anyhow::Result<Vec<Vec<RowData>>> {
        let mut sub_data_map: HashMap<String, Vec<RowData>> = HashMap::new();
        for row_data in data {
            // TODO: id-consecutive chunks can be merged together.
            let full_tb = format!("{}.{}.{}", row_data.schema, row_data.tb, row_data.chunk_id);
            if let Some(sub_data) = sub_data_map.get_mut(&full_tb) {
                sub_data.push(row_data);
            } else {
                sub_data_map.insert(full_tb, vec![row_data]);
            }
        }

        Ok(sub_data_map.into_values().collect())
    }

    pub fn partition_raw(data: Vec<DtItem>) -> anyhow::Result<Vec<Vec<DtItem>>> {
        let mut sub_data_map: HashMap<String, Vec<DtItem>> = HashMap::new();
        for item in data {
            if let DtData::Dml { row_data } = &item.dt_data {
                let full_tb = format!("{}.{}.{}", row_data.schema, row_data.tb, row_data.chunk_id);
                if let Some(sub_data) = sub_data_map.get_mut(&full_tb) {
                    sub_data.push(item);
                } else {
                    sub_data_map.insert(full_tb, vec![item]);
                }
            }
        }

        Ok(sub_data_map.into_values().collect())
    }
}
