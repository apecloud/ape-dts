use std::collections::{HashMap, HashSet};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{mysql::MySqlRow, postgres::PgRow};

use super::{
    col_value::ColValue, mysql::mysql_tb_meta::MysqlTbMeta, pg::pg_tb_meta::PgTbMeta,
    rdb_tb_meta::RdbTbMeta, row_type::RowType,
};
use crate::{
    config::config_enums::DbType,
    meta::adaptor::{
        mysql_col_value_convertor::MysqlColValueConvertor,
        pg_col_value_convertor::PgColValueConvertor,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RowData {
    pub schema: String,
    pub tb: String,
    #[serde(skip)]
    pub chunk_id: u64, // used for snapshot data
    pub row_type: RowType,
    pub before: Option<HashMap<String, ColValue>>,
    pub after: Option<HashMap<String, ColValue>>,
    pub data_size: usize,
    pub is_not_origin: bool,
}

impl std::fmt::Display for RowData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

impl RowData {
    pub fn new(
        schema: String,
        tb: String,
        chunk_id: u64,
        row_type: RowType,
        before: Option<HashMap<String, ColValue>>,
        after: Option<HashMap<String, ColValue>>,
    ) -> Self {
        let mut me = Self {
            schema,
            tb,
            chunk_id,
            row_type,
            before,
            after,
            data_size: 0,
            is_not_origin: false,
        };
        me.data_size = me.get_data_malloc_size();
        me
    }

    pub fn new_no_origin(
        schema: String,
        tb: String,
        chunk_id: u64,
        row_type: RowType,
        before: Option<HashMap<String, ColValue>>,
        after: Option<HashMap<String, ColValue>>,
    ) -> Self {
        let mut data = Self::new(schema, tb, chunk_id, row_type, before, after);
        data.is_not_origin = true;
        data
    }

    pub fn reverse(&self) -> Self {
        let row_type = match self.row_type {
            RowType::Insert => RowType::Delete,
            RowType::Update => RowType::Update,
            RowType::Delete => RowType::Insert,
        };

        Self {
            schema: self.schema.clone(),
            tb: self.tb.clone(),
            chunk_id: self.chunk_id,
            row_type,
            before: self.after.clone(),
            after: self.before.clone(),
            data_size: self.data_size,
            is_not_origin: false,
        }
    }

    pub fn split_update_row_data(self) -> (RowData, RowData) {
        let delete = RowData::new_no_origin(
            self.schema.clone(),
            self.tb.clone(),
            self.chunk_id,
            RowType::Delete,
            self.before,
            None,
        );

        let insert = RowData::new_no_origin(
            self.schema,
            self.tb,
            self.chunk_id,
            RowType::Insert,
            None,
            self.after,
        );
        (delete, insert)
    }

    pub fn from_mysql_row(
        row: &MySqlRow,
        tb_meta: &MysqlTbMeta,
        ignore_cols: &Option<&HashSet<String>>,
    ) -> Self {
        Self::from_mysql_compatible_row(row, tb_meta, ignore_cols, &DbType::Mysql)
    }

    pub fn from_mysql_compatible_row(
        row: &MySqlRow,
        tb_meta: &MysqlTbMeta,
        ignore_cols: &Option<&HashSet<String>>,
        db_type: &DbType,
    ) -> Self {
        let mut after = HashMap::new();
        for (col, col_type) in &tb_meta.col_type_map {
            if ignore_cols.as_ref().is_some_and(|cols| cols.contains(col)) {
                continue;
            }
            let col_val =
                MysqlColValueConvertor::from_query_mysql_compatible(row, col, col_type, db_type)
                    .with_context(|| {
                        format!(
                            "schema: {}, tb: {}, col: {}, col_type: {}",
                            tb_meta.basic.schema, tb_meta.basic.tb, col, col_type
                        )
                    })
                    .unwrap();
            after.insert(col.to_string(), col_val);
        }
        Self::build_insert_row_data(after, &tb_meta.basic)
    }

    pub fn from_pg_row(
        row: &PgRow,
        tb_meta: &PgTbMeta,
        ignore_cols: &Option<&HashSet<String>>,
    ) -> Self {
        let mut after = HashMap::new();
        for (col, col_type) in &tb_meta.col_type_map {
            if ignore_cols.as_ref().is_some_and(|cols| cols.contains(col)) {
                continue;
            }

            let col_value = PgColValueConvertor::from_query(row, col, col_type)
                .with_context(|| {
                    format!(
                        "schema: {}, tb: {}, col: {}, col_type: {}",
                        tb_meta.basic.schema, tb_meta.basic.tb, col, col_type
                    )
                })
                .unwrap();
            after.insert(col.to_string(), col_value);
        }
        Self::build_insert_row_data(after, &tb_meta.basic)
    }

    pub fn build_insert_row_data(after: HashMap<String, ColValue>, tb_meta: &RdbTbMeta) -> Self {
        Self::new(
            tb_meta.schema.clone(),
            tb_meta.tb.clone(),
            0,
            RowType::Insert,
            None,
            Some(after),
        )
    }

    pub fn convert_raw_string(&mut self) {
        if let Some(before) = &mut self.before {
            Self::convert_raw_string_col_values(before);
        }
        if let Some(after) = &mut self.after {
            Self::convert_raw_string_col_values(after);
        }
    }

    pub fn require_after(&self) -> anyhow::Result<&HashMap<String, ColValue>> {
        self.after.as_ref().with_context(|| {
            format!(
                "row_data after is missing, schema: {}, tb: {}",
                self.schema, self.tb
            )
        })
    }

    pub fn require_before(&self) -> anyhow::Result<&HashMap<String, ColValue>> {
        self.before.as_ref().with_context(|| {
            format!(
                "row_data before is missing, schema: {}, tb: {}",
                self.schema, self.tb
            )
        })
    }

    pub fn require_after_mut(&mut self) -> anyhow::Result<&mut HashMap<String, ColValue>> {
        self.after.as_mut().with_context(|| {
            format!(
                "row_data after is missing, schema: {}, tb: {}",
                self.schema, self.tb
            )
        })
    }

    pub fn require_before_mut(&mut self) -> anyhow::Result<&mut HashMap<String, ColValue>> {
        self.before.as_mut().with_context(|| {
            format!(
                "row_data before is missing, schema: {}, tb: {}",
                self.schema, self.tb
            )
        })
    }

    fn convert_raw_string_col_values(col_values: &mut HashMap<String, ColValue>) {
        let mut str_col_values: HashMap<String, ColValue> = HashMap::new();
        for (col, col_value) in col_values.iter() {
            if let ColValue::RawString(_) = col_value {
                if let Some(str) = col_value.to_utf8_or_hex_string() {
                    str_col_values.insert(col.into(), ColValue::String(str));
                } else {
                    str_col_values.insert(col.to_owned(), ColValue::None);
                }
            }
        }

        for (col, col_value) in str_col_values {
            col_values.insert(col, col_value);
        }
    }

    pub fn get_hash_code(&self, tb_meta: &RdbTbMeta) -> anyhow::Result<u128> {
        let col_values = match self.row_type {
            RowType::Insert => self.after.as_ref().context("row_data after is missing")?,
            _ => self.before.as_ref().context("row_data before is missing")?,
        };

        // refer to: https://docs.oracle.com/javase/6/docs/api/java/util/List.html#hashCode()
        let mut hash_code = 1u128;
        for col in tb_meta.id_cols.iter() {
            let col_hash_code = col_values
                .get(col)
                .with_context(|| format!("missing id col value: {}", col))?
                .hash_code()
                .with_context(|| {
                    format!(
                        "unhashable _id value in schema: {}, tb: {}, col: {}",
                        tb_meta.schema, tb_meta.tb, col
                    )
                })?;
            // col_hash_code is 0 if col_value is ColValue::None,
            // consider following case,
            // create table a(id int, value int, unique key(id, value));
            // insert into a values(1, NULL);
            // delete from a where (id, value) in ((1, NULL));  // this won't work
            // delete from a where id=1 and value is NULL;  // this works
            // so here return 0 to stop merging to avoid batch deleting
            if col_hash_code == 0 {
                return Ok(0);
            }
            hash_code = 31 * hash_code + col_hash_code as u128;
        }
        Ok(hash_code)
    }

    pub fn contains_unchanged_toast(&self) -> bool {
        self.after
            .as_ref()
            .is_some_and(|values| values.values().any(ColValue::is_unchanged_toast))
    }

    pub fn refresh_data_size(&mut self) {
        self.data_size = self.get_data_malloc_size();
    }

    pub fn get_data_size(&self) -> u64 {
        self.data_size as u64
    }

    fn get_data_malloc_size(&self) -> usize {
        let mut size = 0;
        // do not use mem::size_of_val() since:
        // for Pointer: it returns the size of pointer without the pointed data
        // for HashMap and Vector: it returns the size of the structure without the stored items
        if let Some(col_values) = &self.before {
            for (_, v) in col_values.iter() {
                size += v.get_malloc_size();
            }
        }
        if let Some(col_values) = &self.after {
            for (_, v) in col_values.iter() {
                size += v.get_malloc_size();
            }
        }
        // ignore other fields
        size
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_convert_raw_string_prefers_utf8() {
        let mut row_data = RowData::new(
            "db".to_string(),
            "tb".to_string(),
            RowType::Insert,
            None,
            Some(HashMap::from([(
                "c1".to_string(),
                ColValue::RawString(b"ij".to_vec()),
            )])),
        );

        row_data.convert_raw_string();

        assert_eq!(
            row_data.require_after().unwrap().get("c1"),
            Some(&ColValue::String("ij".to_string()))
        );
    }
}
