use std::collections::{HashMap, HashSet};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{mysql::MySqlRow, postgres::PgRow};
use tiberius::Row as MssqlRow;

use super::{
    col_value::ColValue, mssql::mssql_tb_meta::MssqlTbMeta, mysql::mysql_tb_meta::MysqlTbMeta,
    pg::pg_tb_meta::PgTbMeta, rdb_tb_meta::RdbTbMeta, row_type::RowType,
};
use crate::{
    config::config_enums::DbType,
    error::{DtError, DtResultExt, ErrorObject},
    meta::adaptor::{
        mssql_col_value_convertor::MssqlColValueConvertor,
        mysql_col_value_convertor::MysqlColValueConvertor,
        pg_col_value_convertor::PgColValueConvertor,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RowData {
    /// Physical database. Empty until database-aware extraction is enabled.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub db: String,
    /// Existing first-level namespace. MySQL/MongoDB still store database here for compatibility.
    pub schema: String,
    pub tb: String,
    #[serde(skip)]
    // Used by snapshot table partitioning to spread table data across sinkers by logical chunk (splitter generated)
    // or batch (from serial extracting)
    pub chunk_id: u64,
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
        db: String,
        schema: String,
        tb: String,
        chunk_id: u64,
        row_type: RowType,
        before: Option<HashMap<String, ColValue>>,
        after: Option<HashMap<String, ColValue>>,
    ) -> Self {
        let mut me = Self {
            db,
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
        db: String,
        schema: String,
        tb: String,
        chunk_id: u64,
        row_type: RowType,
        before: Option<HashMap<String, ColValue>>,
        after: Option<HashMap<String, ColValue>>,
    ) -> Self {
        let mut data = Self::new(db, schema, tb, chunk_id, row_type, before, after);
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
            db: self.db.clone(),
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
            self.db.clone(),
            self.schema.clone(),
            self.tb.clone(),
            self.chunk_id,
            RowType::Delete,
            self.before,
            None,
        );

        let insert = RowData::new_no_origin(
            self.db,
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
        chunk_id: Option<u64>,
    ) -> anyhow::Result<Self> {
        Self::from_mysql_compatible_row(row, tb_meta, ignore_cols, &DbType::Mysql, chunk_id)
    }

    pub fn from_mysql_compatible_row(
        row: &MySqlRow,
        tb_meta: &MysqlTbMeta,
        ignore_cols: &Option<&HashSet<String>>,
        db_type: &DbType,
        chunk_id: Option<u64>,
    ) -> anyhow::Result<Self> {
        let mut after = HashMap::new();
        for (col, col_type) in &tb_meta.col_type_map {
            if ignore_cols.as_ref().is_some_and(|cols| cols.contains(col)) {
                continue;
            }
            let col_val =
                MysqlColValueConvertor::from_query_mysql_compatible(row, col, col_type, db_type)
                    .context(DtError::StatementFailed(format!(
                        "failed to convert column {}.{}.{}",
                        tb_meta.basic.schema, tb_meta.basic.tb, col
                    )))
                    .object(ErrorObject {
                        schema: Some(tb_meta.basic.schema.clone()),
                        table: Some(tb_meta.basic.tb.clone()),
                        column: Some(col.clone()),
                        ..Default::default()
                    })?;
            after.insert(col.to_string(), col_val);
        }
        Ok(Self::build_insert_row_data(after, &tb_meta.basic, chunk_id))
    }

    pub fn from_pg_row(
        row: &PgRow,
        tb_meta: &PgTbMeta,
        ignore_cols: &Option<&HashSet<String>>,
        chunk_id: Option<u64>,
    ) -> anyhow::Result<Self> {
        let mut after = HashMap::new();
        for (col, col_type) in &tb_meta.col_type_map {
            if ignore_cols.as_ref().is_some_and(|cols| cols.contains(col)) {
                continue;
            }

            let col_value = PgColValueConvertor::from_query(row, col, col_type)
                .context(DtError::StatementFailed(format!(
                    "failed to convert column {}.{}.{}",
                    tb_meta.basic.schema, tb_meta.basic.tb, col
                )))
                .object(ErrorObject {
                    schema: Some(tb_meta.basic.schema.clone()),
                    table: Some(tb_meta.basic.tb.clone()),
                    column: Some(col.clone()),
                    ..Default::default()
                })?;
            after.insert(col.to_string(), col_value);
        }
        Ok(Self::build_insert_row_data(after, &tb_meta.basic, chunk_id))
    }

    pub fn from_mssql_row(
        row: &MssqlRow,
        tb_meta: &MssqlTbMeta,
        ignore_cols: &Option<&HashSet<String>>,
        chunk_id: Option<u64>,
    ) -> anyhow::Result<Self> {
        let mut after = HashMap::new();
        for col in &tb_meta.basic.cols {
            if ignore_cols.as_ref().is_some_and(|cols| cols.contains(col)) {
                continue;
            }

            let col_value =
                MssqlColValueConvertor::from_query(row, col, tb_meta.get_col_type(col)?)
                    .context(DtError::StatementFailed(format!(
                        "failed to convert column {}.{}.{}",
                        tb_meta.basic.schema, tb_meta.basic.tb, col
                    )))
                    .object(ErrorObject {
                        schema: Some(tb_meta.basic.schema.clone()),
                        table: Some(tb_meta.basic.tb.clone()),
                        column: Some(col.clone()),
                        ..Default::default()
                    })?;
            after.insert(col.clone(), col_value);
        }
        Ok(Self::build_insert_row_data(after, &tb_meta.basic, chunk_id))
    }

    pub fn build_insert_row_data(
        after: HashMap<String, ColValue>,
        tb_meta: &RdbTbMeta,
        chunk_id: Option<u64>,
    ) -> Self {
        Self::new(
            tb_meta.db.clone(),
            tb_meta.schema.clone(),
            tb_meta.tb.clone(),
            chunk_id.unwrap_or(0),
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

    #[inline(always)]
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
            String::new(),
            "db".to_string(),
            "tb".to_string(),
            0,
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

    #[test]
    fn legacy_json_defaults_db_to_empty() {
        let row_data: RowData = serde_json::from_str(
            r#"{"schema":"s1","tb":"t1","row_type":"Insert","before":null,"after":null,"data_size":0,"is_not_origin":false}"#,
        )
        .unwrap();

        assert!(row_data.db.is_empty());
        assert!(serde_json::to_value(&row_data).unwrap().get("db").is_none());

        let mut row_data_with_db = row_data;
        row_data_with_db.db = "db1".to_string();
        assert_eq!(serde_json::to_value(row_data_with_db).unwrap()["db"], "db1");
    }

    #[test]
    fn split_update_preserves_three_part_table_identity() {
        let row_data = RowData::new(
            "db1".to_string(),
            "s1".to_string(),
            "t1".to_string(),
            0,
            RowType::Update,
            Some(HashMap::new()),
            Some(HashMap::new()),
        );
        let (delete, insert) = row_data.split_update_row_data();
        assert_eq!(
            (
                delete.db.as_str(),
                delete.schema.as_str(),
                delete.tb.as_str()
            ),
            ("db1", "s1", "t1")
        );
        assert_eq!(
            (
                insert.db.as_str(),
                insert.schema.as_str(),
                insert.tb.as_str()
            ),
            ("db1", "s1", "t1")
        );
    }
}
