use std::collections::HashMap;

use async_trait::async_trait;
use dt_common::meta::{
    col_value::ColValue,
    mongo::{mongo_constant::MongoConstants, mongo_key::MongoKey},
    row_data::RowData,
    row_type::RowType,
};

use crate::{merge_parallelizer::TbMergedData, Merger};

pub struct MongoMerger {}

#[async_trait]
impl Merger for MongoMerger {
    async fn merge(&mut self, data: Vec<RowData>) -> anyhow::Result<Vec<TbMergedData>> {
        let mut tb_data_map: HashMap<String, Vec<RowData>> = HashMap::new();
        for row_data in data {
            let full_tb = format!("{}.{}", row_data.schema, row_data.tb);
            if let Some(tb_data) = tb_data_map.get_mut(&full_tb) {
                tb_data.push(row_data);
            } else {
                tb_data_map.insert(full_tb, vec![row_data]);
            }
        }

        let mut results = Vec::new();
        for (tb, tb_data) in tb_data_map.drain() {
            let (insert_rows, delete_rows, unmerged_rows) = Self::merge_row_data(tb_data)?;
            let tb_merged = TbMergedData {
                tb,
                insert_rows,
                delete_rows,
                unmerged_rows,
            };
            results.push(tb_merged);
        }
        Ok(results)
    }
}

impl MongoMerger {
    /// partition dmls of the same table into insert vec and delete vec
    #[allow(clippy::type_complexity)]
    pub fn merge_row_data(
        mut data: Vec<RowData>,
    ) -> anyhow::Result<(Vec<RowData>, Vec<RowData>, Vec<RowData>)> {
        let mut insert_map = HashMap::new();
        let mut delete_map = HashMap::new();

        while !data.is_empty() {
            let hash_key = Self::get_hash_key(&data[0]);
            if hash_key.is_none() {
                break;
            }

            let id = hash_key.unwrap();
            let row_data = data.remove(0);
            match row_data.row_type {
                RowType::Insert => {
                    insert_map.insert(id, row_data);
                }

                RowType::Delete => {
                    insert_map.remove(&id);
                    delete_map.insert(id, row_data);
                }

                RowType::Update => {
                    let before = row_data.require_before()?.clone();
                    let after: HashMap<String, ColValue> = row_data.require_after()?.clone();
                    let delete_row = RowData::new(
                        row_data.schema.clone(),
                        row_data.tb.clone(),
                        RowType::Delete,
                        Some(before),
                        None,
                    );
                    delete_map.insert(id.clone(), delete_row);

                    let insert_row = RowData::new(
                        row_data.schema,
                        row_data.tb,
                        RowType::Insert,
                        Option::None,
                        Some(after),
                    );
                    insert_map.insert(id, insert_row);
                }
            }
        }

        let inserts = insert_map.drain().map(|i| i.1).collect::<Vec<_>>();
        let deletes = delete_map.drain().map(|i| i.1).collect::<Vec<_>>();
        Ok((inserts, deletes, data))
    }

    fn get_hash_key(row_data: &RowData) -> Option<MongoKey> {
        match row_data.row_type {
            RowType::Insert => {
                if let Ok(after) = row_data.require_after() {
                    if let Some(ColValue::MongoDoc(doc)) = after.get(MongoConstants::DOC) {
                        return MongoKey::from_doc(doc);
                    }
                }
            }

            RowType::Delete => {
                if let Ok(before) = row_data.require_before() {
                    if let Some(ColValue::MongoDoc(doc)) = before.get(MongoConstants::DOC) {
                        return MongoKey::from_doc(doc);
                    }
                }
            }

            RowType::Update => {
                if let (Ok(before), Ok(after)) =
                    (row_data.require_before(), row_data.require_after())
                {
                    // for Update row_data from oplog (NOT change stream), after contains diff_doc instead of doc,
                    // in which case we can NOT transfer Update into Delete + Insert
                    if after.get(MongoConstants::DOC).is_none() {
                        return None;
                    } else if let Some(ColValue::MongoDoc(doc)) = before.get(MongoConstants::DOC) {
                        return MongoKey::from_doc(doc);
                    }
                }
            }
        }
        None
    }
}
