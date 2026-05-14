use std::collections::HashMap;

use dt_common::meta::{
    dt_data::{DtData, DtItem},
    row_data::RowData,
};

pub struct TablePartitioner {}

impl TablePartitioner {
    pub fn partition_dml(data: Vec<RowData>) -> anyhow::Result<Vec<Vec<RowData>>> {
        let mut sub_data_map: HashMap<String, Vec<RowData>> = HashMap::new();
        for row_data in data {
            // TODO: id-consecutive chunks can be merged together.
            let sch_tb_chunk = format!("{}.{}.{}", row_data.schema, row_data.tb, row_data.chunk_id);
            if let Some(sub_data) = sub_data_map.get_mut(&sch_tb_chunk) {
                sub_data.push(row_data);
            } else {
                sub_data_map.insert(sch_tb_chunk, vec![row_data]);
            }
        }

        Ok(sub_data_map.into_values().collect())
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
