use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use dt_common::meta::{
    ddl_meta::ddl_data::DdlData,
    dt_data::{DtData, DtItem},
    dt_queue::DtQueue,
    row_data::RowData,
};
use dt_connector::Sinker;

use super::base_parallelizer::BaseParallelizer;
use crate::{DataSize, Parallelizer};

pub struct TableParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub parallel_size: usize,
}

#[async_trait]
impl Parallelizer for TableParallelizer {
    fn get_name(&self) -> String {
        "TableParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &DtQueue) -> anyhow::Result<Vec<DtItem>> {
        self.base_parallelizer.drain(buffer).await
    }

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        let data_size = DataSize {
            count: data.len() as u64,
            bytes: data.iter().map(|v| v.get_data_size()).sum(),
        };

        let sub_data = Self::partition_dml(data)?;
        self.base_parallelizer
            .sink_dml(sub_data, sinkers, self.parallel_size, false)
            .await?;

        Ok(data_size)
    }

    async fn sink_raw(
        &mut self,
        data: Vec<DtItem>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        let data_size = DataSize {
            count: data.len() as u64,
            bytes: data.iter().map(|v| v.get_data_size()).sum(),
        };

        let sub_data = Self::partition_raw(data)?;
        self.base_parallelizer
            .sink_raw(sub_data, sinkers, self.parallel_size, false)
            .await?;

        Ok(data_size)
    }

    async fn sink_ddl(
        &mut self,
        data: Vec<DdlData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        let data_size = DataSize {
            count: data.len() as u64,
            bytes: data.iter().map(|v| v.get_data_size()).sum(),
        };

        self.base_parallelizer
            .sink_ddl(vec![data], sinkers, 1, false)
            .await?;

        Ok(data_size)
    }
}

impl TableParallelizer {
    // partition dml vec into sub vecs by full table name
    fn partition_dml(data: Vec<RowData>) -> anyhow::Result<Vec<Vec<RowData>>> {
        let mut sub_data_map: HashMap<(String, String, String), Vec<RowData>> = HashMap::new();
        for row_data in data {
            let table_key = (
                row_data.db.clone(),
                row_data.schema.clone(),
                row_data.tb.clone(),
            );
            if let Some(sub_data) = sub_data_map.get_mut(&table_key) {
                sub_data.push(row_data);
            } else {
                sub_data_map.insert(table_key, vec![row_data]);
            }
        }

        Ok(sub_data_map.into_values().collect())
    }

    fn partition_raw(data: Vec<DtItem>) -> anyhow::Result<Vec<Vec<DtItem>>> {
        let mut sub_data_map: HashMap<(String, String, String), Vec<DtItem>> = HashMap::new();
        for item in data {
            if let DtData::Dml { row_data } = &item.dt_data {
                let table_key = (
                    row_data.db.clone(),
                    row_data.schema.clone(),
                    row_data.tb.clone(),
                );
                if let Some(sub_data) = sub_data_map.get_mut(&table_key) {
                    sub_data.push(item);
                } else {
                    sub_data_map.insert(table_key, vec![item]);
                }
            }
        }

        Ok(sub_data_map.into_values().collect())
    }
}

#[cfg(test)]
mod tests {
    use dt_common::meta::{row_data::RowData, row_type::RowType};

    use super::TableParallelizer;

    #[test]
    fn partition_dml_isolates_identical_schema_tables_by_db() {
        let row = |db: &str| {
            RowData::new(
                db.to_string(),
                "schema1".to_string(),
                "tb1".to_string(),
                0,
                RowType::Insert,
                None,
                None,
            )
        };

        let groups = TableParallelizer::partition_dml(vec![row("db1"), row("db2")]).unwrap();
        assert_eq!(groups.len(), 2);
        assert!(groups.iter().all(|group| group.len() == 1));
    }
}
