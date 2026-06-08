use std::collections::HashMap;

use async_trait::async_trait;
use dt_common::log_debug;
use dt_common::meta::{
    rdb_meta_manager::RdbMetaManager, rdb_tb_meta::RdbTbMeta, row_data::RowData, row_type::RowType,
};

use crate::{merge_parallelizer::TbMergedData, Merger};

pub struct RdbMerger {
    pub rdb_meta_manager: RdbMetaManager,
}

#[async_trait]
impl Merger for RdbMerger {
    async fn merge(&mut self, data: Vec<RowData>) -> anyhow::Result<Vec<TbMergedData>> {
        let mut tb_data_map = HashMap::<String, RdbTbMergedData>::new();
        for row_data in data {
            let full_tb = format!("{}.{}", row_data.schema, row_data.tb);
            if let Some(merged) = tb_data_map.get_mut(&full_tb) {
                self.merge_row_data(merged, row_data).await?;
            } else {
                let mut merged = RdbTbMergedData::new();
                self.merge_row_data(&mut merged, row_data).await?;
                tb_data_map.insert(full_tb, merged);
            }
        }

        let mut results = Vec::new();
        for (_, mut rdb_tb_merged) in tb_data_map.drain() {
            let tb_merged = TbMergedData {
                insert_rows: rdb_tb_merged.get_insert_rows(),
                delete_rows: rdb_tb_merged.get_delete_rows(),
                unmerged_rows: rdb_tb_merged.get_unmerged_rows(),
            };
            results.push(tb_merged);
        }
        Ok(results)
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.rdb_meta_manager.close().await
    }
}

impl RdbMerger {
    async fn merge_row_data(
        &mut self,
        merged: &mut RdbTbMergedData,
        row_data: RowData,
    ) -> anyhow::Result<()> {
        // if the table already has some rows unmerged, then following rows also need to be unmerged.
        // all unmerged rows will be sinked serially
        if !merged.unmerged_rows.is_empty() {
            merged.unmerged_rows.push(row_data);
            return Ok(());
        }

        let tb_meta = self
            .rdb_meta_manager
            .get_tb_meta(&row_data.schema, &row_data.tb)
            .await?;
        let tb_meta = tb_meta.as_ref();

        if row_data.contains_unchanged_toast() {
            merged.unmerged_rows.push(row_data);
            return Ok(());
        }

        // case 1: table has no primary/unique key
        // case 2: any key col value is NULL
        let hash_code = Self::get_hash_code(&row_data, tb_meta).await?;
        if hash_code == 0 {
            merged.unmerged_rows.push(row_data);
            return Ok(());
        }

        match row_data.row_type {
            RowType::Delete => {
                if Self::check_collision(&merged.insert_rows, tb_meta, &row_data, hash_code)?
                    || Self::check_collision(&merged.delete_rows, tb_meta, &row_data, hash_code)?
                {
                    merged.unmerged_rows.push(row_data);
                    return Ok(());
                }
                merged.insert_rows.remove(&hash_code);
                merged.delete_rows.insert(hash_code, row_data);
            }

            RowType::Update => {
                // if pk/uk change found in any row_data, for safety, all following row_data won't be merged
                if Self::check_key_changed(tb_meta, &row_data) {
                    merged.unmerged_rows.push(row_data);
                    return Ok(());
                }

                let delete = RowData::new(
                    row_data.schema.clone(),
                    row_data.tb.clone(),
                    0,
                    RowType::Delete,
                    row_data.before,
                    None,
                );
                let insert = RowData::new(
                    row_data.schema,
                    row_data.tb,
                    0,
                    RowType::Insert,
                    None,
                    row_data.after,
                );

                let insert_hash_code = Self::get_hash_code(&insert, tb_meta).await?;

                if Self::check_collision(&merged.insert_rows, tb_meta, &insert, insert_hash_code)?
                    || Self::check_collision(&merged.delete_rows, tb_meta, &delete, hash_code)?
                {
                    let row_data = RowData::new(
                        delete.schema,
                        delete.tb,
                        0,
                        RowType::Update,
                        delete.before,
                        insert.after,
                    );
                    merged.unmerged_rows.push(row_data);
                    return Ok(());
                }
                merged.delete_rows.insert(hash_code, delete);
                merged.insert_rows.insert(insert_hash_code, insert);
            }

            RowType::Insert => {
                if Self::check_collision(&merged.insert_rows, tb_meta, &row_data, hash_code)? {
                    merged.unmerged_rows.push(row_data);
                    return Ok(());
                }
                merged.insert_rows.insert(hash_code, row_data);
            }
        }
        Ok(())
    }

    fn check_key_changed(tb_meta: &RdbTbMeta, row_data: &RowData) -> bool {
        let before = row_data.before.as_ref().unwrap();
        let after = row_data.after.as_ref().unwrap();
        for key_cols in tb_meta.key_map.values() {
            for col in key_cols {
                if before.get(col) != after.get(col) {
                    log_debug!("rdb_merger, key change found, row_data: {:?}", row_data);
                    return true;
                }
            }
        }
        false
    }

    fn check_collision(
        buffer: &HashMap<u128, RowData>,
        tb_meta: &RdbTbMeta,
        row_data: &RowData,
        hash_code: u128,
    ) -> anyhow::Result<bool> {
        if let Some(exist) = buffer.get(&hash_code) {
            let col_values = match row_data.row_type {
                RowType::Insert => row_data.require_after()?,
                _ => row_data.require_before()?,
            };

            let exist_col_values = match exist.row_type {
                RowType::Insert => exist.require_after()?,
                _ => exist.require_before()?,
            };

            for col in tb_meta.id_cols.iter() {
                if col_values.get(col) != exist_col_values.get(col) {
                    log_debug!("rdb_merger, collision found, row_data: {:?}", row_data);
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    async fn get_hash_code(row_data: &RowData, tb_meta: &RdbTbMeta) -> anyhow::Result<u128> {
        if tb_meta.key_map.is_empty() {
            return Ok(0);
        }
        row_data.get_hash_code(tb_meta)
    }
}

struct RdbTbMergedData {
    delete_rows: HashMap<u128, RowData>,
    insert_rows: HashMap<u128, RowData>,
    unmerged_rows: Vec<RowData>,
}

impl RdbTbMergedData {
    pub fn new() -> Self {
        Self {
            delete_rows: HashMap::new(),
            insert_rows: HashMap::new(),
            unmerged_rows: Vec::new(),
        }
    }

    pub fn get_delete_rows(&mut self) -> Vec<RowData> {
        self.delete_rows.drain().map(|i| i.1).collect::<Vec<_>>()
    }

    pub fn get_insert_rows(&mut self) -> Vec<RowData> {
        self.insert_rows.drain().map(|i| i.1).collect::<Vec<_>>()
    }

    pub fn get_unmerged_rows(&mut self) -> Vec<RowData> {
        self.unmerged_rows.drain(..).collect::<Vec<_>>()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dt_common::meta::{col_value::ColValue, row_type::RowType};

    use super::*;

    fn build_tb_meta() -> RdbTbMeta {
        RdbTbMeta {
            schema: "test_db".to_string(),
            tb: "test_tb".to_string(),
            cols: vec![
                "id".to_string(),
                "uk_1".to_string(),
                "uk_2".to_string(),
                "value".to_string(),
            ],
            key_map: HashMap::from([
                ("primary".to_string(), vec!["id".to_string()]),
                (
                    "uk_test".to_string(),
                    vec!["uk_1".to_string(), "uk_2".to_string()],
                ),
            ]),
            partition_col: "id".to_string(),
            id_cols: vec!["id".to_string()],
            ..Default::default()
        }
    }

    fn build_update_row(changed_col: &str) -> RowData {
        let before = HashMap::from([
            ("id".to_string(), ColValue::Long(1)),
            ("uk_1".to_string(), ColValue::Long(10)),
            ("uk_2".to_string(), ColValue::Long(20)),
            ("value".to_string(), ColValue::String("before".to_string())),
        ]);
        let mut after = before.clone();
        after.insert(
            changed_col.to_string(),
            ColValue::String("after".to_string()),
        );

        RowData::new(
            "test_db".to_string(),
            "test_tb".to_string(),
            0,
            RowType::Update,
            Some(before),
            Some(after),
        )
    }

    #[test]
    fn check_key_changed_detects_primary_key_change() {
        let tb_meta = build_tb_meta();
        let row_data = build_update_row("id");

        assert!(RdbMerger::check_key_changed(&tb_meta, &row_data));
    }

    #[test]
    fn check_key_changed_detects_unique_key_change() {
        let tb_meta = build_tb_meta();
        let row_data = build_update_row("uk_1");

        assert!(RdbMerger::check_key_changed(&tb_meta, &row_data));
    }

    #[test]
    fn check_key_changed_ignores_non_key_change() {
        let tb_meta = build_tb_meta();
        let row_data = build_update_row("value");

        assert!(!RdbMerger::check_key_changed(&tb_meta, &row_data));
    }
}
