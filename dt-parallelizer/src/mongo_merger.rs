use std::collections::HashMap;

use async_trait::async_trait;
use dt_common::meta::{
    col_value::ColValue,
    mongo::{mongo_constant::MongoConstants, mongo_key::MongoKey},
    row_data::RowData,
    row_type::RowType,
};

use crate::{merge_parallelizer::TbMergedData, Merger};

pub struct MongoMerger;

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
        for (_, tb_data) in tb_data_map.drain() {
            let (insert_rows, delete_rows, unmerged_rows) = Self::merge_row_data(tb_data)?;
            let tb_merged = TbMergedData {
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
        data: Vec<RowData>,
    ) -> anyhow::Result<(Vec<RowData>, Vec<RowData>, Vec<RowData>)> {
        let mut insert_map = HashMap::new();
        let mut delete_map = HashMap::new();
        let mut unmerged_rows = Vec::new();
        let mut iter = data.into_iter();

        while let Some(row_data) = iter.next() {
            let Some(id) = Self::get_hash_key(&row_data) else {
                unmerged_rows.push(row_data);
                unmerged_rows.extend(iter);
                break;
            };

            if row_data.row_type == RowType::Insert {
                insert_map.insert(id, row_data);
                continue;
            }

            if row_data.row_type == RowType::Delete {
                insert_map.remove(&id);
                delete_map.insert(id, row_data);
                continue;
            }

            let schema = row_data.schema;
            let tb = row_data.tb;
            let delete_row = RowData::new(
                schema.clone(),
                tb.clone(),
                0,
                RowType::Delete,
                row_data.before,
                None,
            );
            delete_map.insert(id.clone(), delete_row);

            let insert_row = RowData::new(schema, tb, 0, RowType::Insert, None, row_data.after);
            insert_map.insert(id, insert_row);
        }

        let inserts = insert_map.drain().map(|i| i.1).collect::<Vec<_>>();
        let deletes = delete_map.drain().map(|i| i.1).collect::<Vec<_>>();
        Ok((inserts, deletes, unmerged_rows))
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
