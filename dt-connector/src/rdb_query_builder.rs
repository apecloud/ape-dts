use std::collections::{HashMap, HashSet};

use anyhow::bail;
use dt_common::{config::config_enums::DbType, error::Error, utils::sql_util::SqlUtil};
use dt_common::{
    log_warn,
    meta::{
        adaptor::{
            pg_col_value_convertor::PgColValueConvertor,
            sqlx_ext::{SqlxMysqlExt, SqlxPgExt},
        },
        col_value::ColValue,
        mysql::{mysql_col_type::MysqlColType, mysql_tb_meta::MysqlTbMeta},
        pg::pg_tb_meta::PgTbMeta,
        rdb_tb_meta::RdbTbMeta,
        row_data::RowData,
        row_type::RowType,
    },
};
use sqlx::{mysql::MySqlArguments, postgres::PgArguments, query::Query, MySql, Postgres};

pub struct RdbQueryInfo<'a> {
    pub sql: String,
    pub cols: Vec<String>,
    pub binds: Vec<Option<&'a ColValue>>,
}

pub struct RdbQueryBuilder<'a> {
    rdb_tb_meta: &'a RdbTbMeta,
    db_type: DbType,
    ignore_cols: Option<&'a HashSet<String>>,
    pg_tb_meta: Option<&'a PgTbMeta>,
    mysql_tb_meta: Option<&'a MysqlTbMeta>,
}

impl RdbQueryBuilder<'_> {
    #[inline(always)]
    pub fn new_for_mysql<'a>(
        tb_meta: &'a MysqlTbMeta,
        ignore_cols: Option<&'a HashSet<String>>,
    ) -> RdbQueryBuilder<'a> {
        RdbQueryBuilder {
            rdb_tb_meta: &tb_meta.basic,
            pg_tb_meta: None,
            mysql_tb_meta: Some(tb_meta),
            db_type: DbType::Mysql,
            ignore_cols,
        }
    }

    #[inline(always)]
    pub fn new_for_pg<'a>(
        tb_meta: &'a PgTbMeta,
        ignore_cols: Option<&'a HashSet<String>>,
    ) -> RdbQueryBuilder<'a> {
        RdbQueryBuilder {
            rdb_tb_meta: &tb_meta.basic,
            pg_tb_meta: Some(tb_meta),
            mysql_tb_meta: None,
            db_type: DbType::Pg,
            ignore_cols,
        }
    }

    #[inline(always)]
    pub fn create_mysql_query<'a>(
        &self,
        query_info: &'a RdbQueryInfo,
    ) -> Query<'a, MySql, MySqlArguments> {
        let mut query: Query<MySql, MySqlArguments> = sqlx::query(&query_info.sql);
        let tb_meta = self.mysql_tb_meta.as_ref().unwrap();
        for i in 0..query_info.binds.len() {
            let col_type = tb_meta.get_col_type(&query_info.cols[i]).unwrap();
            query = query.bind_col_value(query_info.binds[i], col_type);
        }
        query
    }

    #[inline(always)]
    pub fn create_pg_query<'a>(
        &self,
        query_info: &'a RdbQueryInfo,
    ) -> Query<'a, Postgres, PgArguments> {
        let mut query: Query<Postgres, PgArguments> = sqlx::query(&query_info.sql);
        let tb_meta = &self.pg_tb_meta.as_ref().unwrap();
        for i in 0..query_info.binds.len() {
            let col_type = tb_meta.get_col_type(&query_info.cols[i]).unwrap();
            query = query.bind_col_value(query_info.binds[i], col_type);
        }
        query
    }

    pub fn get_query_info<'a>(
        &self,
        row_data: &'a RowData,
        replace: bool,
    ) -> anyhow::Result<RdbQueryInfo<'a>> {
        self.get_query_info_internal(row_data, replace, true)
    }

    pub fn get_query_sql(&self, row_data: &RowData, replace: bool) -> anyhow::Result<String> {
        let query_info = self.get_query_info_internal(row_data, replace, false)?;
        Ok(query_info.sql + ";")
    }

    fn get_query_info_internal<'a>(
        &self,
        row_data: &'a RowData,
        replace: bool,
        placeholder: bool,
    ) -> anyhow::Result<RdbQueryInfo<'a>> {
        match row_data.row_type {
            RowType::Insert => {
                if replace {
                    self.get_replace_query(row_data, placeholder)
                } else {
                    self.get_insert_query(row_data, placeholder)
                }
            }
            RowType::Update => {
                if replace
                    && self.db_type == DbType::Pg
                    && !row_data.contains_unchanged_toast()
                    && self.check_primary_key_changed(row_data)
                {
                    self.get_pg_pk_changed_update_replace_query(row_data, placeholder)
                } else {
                    self.get_update_query(row_data, placeholder)
                }
            }
            RowType::Delete => self.get_delete_query(row_data, placeholder),
        }
    }

    pub fn get_batch_delete_query<'a>(
        &self,
        data: &'a [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<(RdbQueryInfo<'a>, usize)> {
        let mut data_size = 0;
        let mut all_placeholders = Vec::new();
        let mut placeholder_index = 1;
        for _ in 0..batch_size {
            let mut placeholders = Vec::new();
            for col in self.rdb_tb_meta.id_cols.iter() {
                placeholders.push(self.get_placeholder(placeholder_index, col)?);
                placeholder_index += 1;
            }
            all_placeholders.push(format!("({})", placeholders.join(",")));
        }

        let sql = format!(
            "DELETE FROM {}.{} WHERE ({}) IN ({})",
            self.escape(&self.rdb_tb_meta.schema),
            self.escape(&self.rdb_tb_meta.tb),
            self.escape_cols(&self.rdb_tb_meta.id_cols).join(","),
            all_placeholders.join(",")
        );

        let mut cols = Vec::new();
        let mut binds = Vec::new();
        for row_data in data.iter().skip(start_index).take(batch_size) {
            data_size += row_data.data_size;
            let before = row_data.before.as_ref().unwrap();
            for col in self.rdb_tb_meta.id_cols.iter() {
                cols.push(col.clone());
                let col_value = before.get(col);
                if col_value.is_none() || *col_value.unwrap() == ColValue::None {
                    bail! {
                        "where col: {} is NULL, which should not happen in batch delete, sql: {}",
                        col, sql
                    }
                }
                binds.push(col_value);
            }
        }
        Ok((RdbQueryInfo { sql, cols, binds }, data_size))
    }

    pub fn get_batch_insert_query<'a>(
        &self,
        data: &'a [RowData],
        start_index: usize,
        batch_size: usize,
        replace: bool,
    ) -> anyhow::Result<(RdbQueryInfo<'a>, usize)> {
        let mut malloc_size = 0;
        let mut placeholder_index = 1;
        let mut row_values = Vec::new();
        for _ in 0..batch_size {
            let mut col_values = Vec::new();
            for col in self.rdb_tb_meta.cols.iter() {
                col_values.push(self.get_placeholder(placeholder_index, col)?);
                placeholder_index += 1;
            }
            row_values.push(format!("({})", col_values.join(",")));
        }

        let mut sql = format!(
            "INSERT INTO {}.{}({}) VALUES{}",
            self.escape(&self.rdb_tb_meta.schema),
            self.escape(&self.rdb_tb_meta.tb),
            self.escape_cols(&self.rdb_tb_meta.cols).join(","),
            row_values.join(",")
        );

        let mut cols = Vec::new();
        let mut binds = Vec::new();
        for row_data in data.iter().skip(start_index).take(batch_size) {
            malloc_size += row_data.data_size;
            let after = row_data.after.as_ref().unwrap();
            for col_name in self.rdb_tb_meta.cols.iter() {
                cols.push(col_name.clone());
                binds.push(after.get(col_name));
            }
        }

        if replace && self.mysql_tb_meta.is_some() {
            sql = format!("REPLACE{}", sql.trim_start_matches("INSERT"));
        }
        Ok((RdbQueryInfo { sql, cols, binds }, malloc_size))
    }

    fn get_replace_query<'a>(
        &self,
        row_data: &'a RowData,
        placeholder: bool,
    ) -> anyhow::Result<RdbQueryInfo<'a>> {
        if self.db_type == DbType::Pg {
            let key_cols = self
                .rdb_tb_meta
                .key_map
                .values()
                .flatten()
                .collect::<HashSet<&String>>();

            if row_data.is_not_origin {
                return self.get_pg_replace_query(row_data, placeholder, &key_cols);
            }

            self.get_pg_origin_replace_query(row_data, placeholder, &key_cols)
        } else {
            let mut query_info = self.get_insert_query(row_data, placeholder)?;
            query_info.sql = format!("REPLACE{}", query_info.sql.trim_start_matches("INSERT"));
            Ok(query_info)
        }
    }

    fn get_pg_replace_query<'a>(
        &self,
        row_data: &'a RowData,
        placeholder: bool,
        key_cols: &HashSet<&String>,
    ) -> anyhow::Result<RdbQueryInfo<'a>> {
        let mut query_info = self.get_insert_query(row_data, placeholder)?;
        let mut index = query_info.cols.len() + 1;
        let after = row_data.after.as_ref().unwrap();
        let mut set_pairs = Vec::new();
        for col in self.rdb_tb_meta.cols.iter() {
            if self.rdb_tb_meta.id_cols.contains(col) {
                continue;
            }
            if !row_data.is_not_origin && key_cols.contains(col) {
                continue;
            }
            let sql_value = self.get_sql_value(index, col, &after.get(col), placeholder)?;
            let set_pair = format!(r#""{}"={}"#, col, sql_value);
            set_pairs.push(set_pair);
            query_info.cols.push(col.clone());
            query_info.binds.push(after.get(col));
            index += 1;
        }

        let conflict_clause = if set_pairs.is_empty() {
            // when all columns are primary keys, use DO NOTHING instead of DO UPDATE SET
            "DO NOTHING".to_string()
        } else {
            format!("DO UPDATE SET {}", set_pairs.join(","))
        };

        query_info.sql = format!(
            "{} ON CONFLICT ({}) {}",
            query_info.sql,
            SqlUtil::escape_cols(&self.rdb_tb_meta.id_cols, &self.db_type).join(","),
            conflict_clause
        );
        Ok(query_info)
    }

    fn get_pg_origin_replace_query<'a>(
        &self,
        row_data: &'a RowData,
        placeholder: bool,
        key_cols: &HashSet<&String>,
    ) -> anyhow::Result<RdbQueryInfo<'a>> {
        let mut query_info = self.get_insert_query(row_data, placeholder)?;
        let primary_key_cols = self.rdb_tb_meta.key_map.get("primary");
        let after = row_data.after.as_ref().unwrap();
        let mut index = query_info.cols.len() + 1;
        let mut set_pairs = Vec::new();
        let mut where_pairs = Vec::new();

        // No primary key, but has unique keys, ignore on conflict
        // case:
        //  Full replication conflicts may come from:
        //    - Repeated inserts during resume from breakpoint, ignore
        //  Incremental replication conflicts may come from:
        //    - Pulling back to an earlier position, ignore
        //    - Dual write import in the target database, ignore
        //    In other cases, whether in serial or rdb_merge, shouldn't reach this logic.
        //    Otherwise, it may be a program bug.
        if primary_key_cols.is_none() {
            query_info.sql = format!("{} ON CONFLICT DO NOTHING", query_info.sql);
            return Ok(query_info);
        }
        let primary_key_cols = primary_key_cols.unwrap();

        if self.rdb_tb_meta.id_cols.len() != primary_key_cols.len()
            || self
                .rdb_tb_meta
                .id_cols
                .iter()
                .zip(primary_key_cols.iter())
                .any(|(id_col, primary_col)| id_col != primary_col)
        {
            query_info.sql = format!("{} ON CONFLICT DO NOTHING", query_info.sql);
            return Ok(query_info);
        }

        for col in self.rdb_tb_meta.cols.iter() {
            if self.rdb_tb_meta.id_cols.contains(col) || key_cols.contains(col) {
                continue;
            }

            let sql_value = self.get_sql_value(index, col, &after.get(col), placeholder)?;
            set_pairs.push(format!(r#"{}={}"#, self.escape(col), sql_value));
            query_info.cols.push(col.clone());
            query_info.binds.push(after.get(col));
            index += 1;
        }

        for col in self.rdb_tb_meta.id_cols.iter() {
            let sql_value = self.get_sql_value(index, col, &after.get(col), placeholder)?;
            where_pairs.push(format!(r#"{}={}"#, self.escape(col), sql_value));
            query_info.cols.push(col.clone());
            query_info.binds.push(after.get(col));
            index += 1;
        }

        if set_pairs.is_empty() || where_pairs.is_empty() {
            log_warn!(
                "schema: {}, tb: {}, no set or where pairs, will do nothing when conflict",
                self.rdb_tb_meta.schema,
                self.rdb_tb_meta.tb
            );
            query_info.sql = format!("{} ON CONFLICT DO NOTHING", query_info.sql);
            return Ok(query_info);
        }

        query_info.sql = format!(
            "WITH inserted AS ({insert_sql} ON CONFLICT DO NOTHING RETURNING 1) \
            UPDATE {schema}.{tb} SET {set_sql} WHERE {where_sql} AND NOT EXISTS (SELECT 1 FROM inserted)",
            insert_sql = query_info.sql,
            schema = self.escape(&self.rdb_tb_meta.schema),
            tb = self.escape(&self.rdb_tb_meta.tb),
            set_sql = set_pairs.join(","),
            where_sql = where_pairs.join(" AND ")
        );
        Ok(query_info)
    }

    fn get_insert_query<'a>(
        &self,
        row_data: &'a RowData,
        placeholder: bool,
    ) -> anyhow::Result<RdbQueryInfo<'a>> {
        let mut cols = Vec::new();
        let mut binds = Vec::new();
        let after = row_data.after.as_ref().unwrap();
        for col_name in self.rdb_tb_meta.cols.iter() {
            cols.push(col_name.clone());
            binds.push(after.get(col_name));
        }

        let mut col_values = Vec::new();
        for i in 0..self.rdb_tb_meta.cols.len() {
            let sql_value =
                self.get_sql_value(i + 1, &self.rdb_tb_meta.cols[i], &binds[i], placeholder)?;
            col_values.push(sql_value);
        }

        let sql = format!(
            "INSERT INTO {}.{}({}) VALUES({})",
            self.escape(&self.rdb_tb_meta.schema),
            self.escape(&self.rdb_tb_meta.tb),
            self.escape_cols(&self.rdb_tb_meta.cols).join(","),
            col_values.join(",")
        );

        Ok(RdbQueryInfo { sql, cols, binds })
    }

    fn get_delete_query<'a>(
        &self,
        row_data: &'a RowData,
        placeholder: bool,
    ) -> anyhow::Result<RdbQueryInfo<'a>> {
        let before = row_data.before.as_ref().unwrap();
        let (where_sql, not_null_cols) = self.get_where_info(1, before, placeholder)?;
        let escaped_schema = self.escape(&self.rdb_tb_meta.schema);
        let escaped_tb = self.escape(&self.rdb_tb_meta.tb);
        let mut sql = format!(
            "DELETE FROM {}.{} WHERE {}",
            escaped_schema, escaped_tb, where_sql
        );
        if self.rdb_tb_meta.key_map.is_empty() {
            if self.db_type == DbType::Pg {
                sql = format!(
                    "DELETE FROM {schema}.{tb} WHERE ctid IN (SELECT ctid FROM {schema}.{tb} WHERE {where_sql} LIMIT 1)",
                    schema = escaped_schema,
                    tb = escaped_tb,
                    where_sql = where_sql,
                );
            } else {
                sql += " LIMIT 1";
            }
        }

        let mut cols = Vec::new();
        let mut binds = Vec::new();
        for col_name in not_null_cols.iter() {
            cols.push(col_name.clone());
            binds.push(before.get(col_name));
        }
        Ok(RdbQueryInfo { sql, cols, binds })
    }

    fn get_update_query<'a>(
        &self,
        row_data: &'a RowData,
        placeholder: bool,
    ) -> anyhow::Result<RdbQueryInfo<'a>> {
        let before = row_data.before.as_ref().unwrap();
        let after = row_data.after.as_ref().unwrap();

        let mut index = 1;
        let mut set_cols = Vec::new();
        let mut set_pairs = Vec::new();
        for (col, col_value) in after.iter() {
            if col_value.is_unchanged_toast() {
                continue;
            }
            set_cols.push(col.clone());
            let sql_value = self.get_sql_value(index, col, &after.get(col), placeholder)?;
            set_pairs.push(format!("{}={}", self.escape(col), sql_value));
            index += 1;
        }

        if set_pairs.is_empty() {
            bail! {Error::Unexpected(format!(
                "schema: {}, tb: {}, no cols in after, which should not happen in update",
                self.rdb_tb_meta.schema, self.rdb_tb_meta.tb
            ))}
        }

        let (where_sql, not_null_cols) = self.get_where_info(index, before, placeholder)?;
        let escaped_schema = self.escape(&self.rdb_tb_meta.schema);
        let escaped_tb = self.escape(&self.rdb_tb_meta.tb);
        let mut sql = format!(
            "UPDATE {}.{} SET {} WHERE {}",
            escaped_schema,
            escaped_tb,
            set_pairs.join(","),
            where_sql,
        );
        if self.rdb_tb_meta.key_map.is_empty() {
            if self.db_type == DbType::Pg {
                sql = format!(
                    "UPDATE {schema}.{tb} SET {set_sql} WHERE ctid IN (SELECT ctid FROM {schema}.{tb} WHERE {where_sql} LIMIT 1)",
                    schema = escaped_schema,
                    tb = escaped_tb,
                    set_sql = set_pairs.join(","),
                    where_sql = where_sql,
                );
            } else {
                sql += " LIMIT 1";
            }
        }

        let mut cols = set_cols.clone();
        let mut binds = Vec::new();
        for col_name in set_cols.iter() {
            binds.push(after.get(col_name));
        }
        for col_name in not_null_cols.iter() {
            cols.push(col_name.clone());
            binds.push(before.get(col_name));
        }
        Ok(RdbQueryInfo { sql, cols, binds })
    }

    fn get_pg_pk_changed_update_replace_query<'a>(
        &self,
        row_data: &'a RowData,
        placeholder: bool,
    ) -> anyhow::Result<RdbQueryInfo<'a>> {
        let before = row_data.before.as_ref().unwrap();
        let after = row_data.after.as_ref().unwrap();

        let mut delete_where = Vec::new();
        let mut cols = Vec::new();
        let mut binds = Vec::new();
        let mut index = 1;
        for col in self.rdb_tb_meta.id_cols.iter() {
            let sql_value = self.get_sql_value(index, col, &before.get(col), placeholder)?;
            delete_where.push(format!(r#"{}={}"#, self.escape(col), sql_value));
            cols.push(col.clone());
            binds.push(before.get(col));
            index += 1;
        }

        let mut insert_values = Vec::new();
        for col in self.rdb_tb_meta.cols.iter() {
            let sql_value = self.get_sql_value(index, col, &after.get(col), placeholder)?;
            insert_values.push(sql_value);
            cols.push(col.clone());
            binds.push(after.get(col));
            index += 1;
        }

        let mut set_pairs = Vec::new();
        for col in self.rdb_tb_meta.cols.iter() {
            if self.rdb_tb_meta.id_cols.contains(col) {
                continue;
            }
            set_pairs.push(format!(r#"{col}=EXCLUDED.{col}"#, col = self.escape(col),));
        }

        let conflict_clause = if set_pairs.is_empty() {
            "DO NOTHING".to_string()
        } else {
            format!("DO UPDATE SET {}", set_pairs.join(","))
        };

        let sql = format!(
            "WITH deleted AS (DELETE FROM {schema}.{tb} WHERE {delete_where}) \
            INSERT INTO {schema}.{tb}({insert_cols}) VALUES({insert_values}) \
            ON CONFLICT ({conflict_cols}) {conflict_clause}",
            schema = self.escape(&self.rdb_tb_meta.schema),
            tb = self.escape(&self.rdb_tb_meta.tb),
            delete_where = delete_where.join(" AND "),
            insert_cols = self.escape_cols(&self.rdb_tb_meta.cols).join(","),
            insert_values = insert_values.join(","),
            conflict_cols = self.escape_cols(&self.rdb_tb_meta.id_cols).join(","),
            conflict_clause = conflict_clause,
        );

        Ok(RdbQueryInfo { sql, cols, binds })
    }

    pub fn get_select_query<'a>(&self, row_data: &'a RowData) -> anyhow::Result<RdbQueryInfo<'a>> {
        let after = row_data.after.as_ref().unwrap();
        let (where_sql, not_null_cols) = self.get_where_info(1, after, true)?;
        let mut sql = format!(
            "SELECT {} FROM {}.{} WHERE {}",
            self.build_extract_cols_str()?,
            self.escape(&self.rdb_tb_meta.schema),
            self.escape(&self.rdb_tb_meta.tb),
            where_sql,
        );

        if self.rdb_tb_meta.key_map.is_empty() {
            sql += " LIMIT 1";
        }

        let mut cols = Vec::new();
        let mut binds = Vec::new();
        for col_name in not_null_cols.iter() {
            cols.push(col_name.clone());
            binds.push(after.get(col_name));
        }
        Ok(RdbQueryInfo { sql, cols, binds })
    }

    pub fn get_batch_select_query<'a>(
        &self,
        data: &'a [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<RdbQueryInfo<'a>> {
        let where_sql = self.get_where_in_info(batch_size)?;
        let sql = format!(
            "SELECT {} FROM {}.{} WHERE {}",
            self.build_extract_cols_str()?,
            self.escape(&self.rdb_tb_meta.schema),
            self.escape(&self.rdb_tb_meta.tb),
            where_sql,
        );

        let mut cols = Vec::new();
        let mut binds = Vec::new();
        for row_data in data.iter().skip(start_index).take(batch_size) {
            let after = row_data.after.as_ref().unwrap();
            for col in self.rdb_tb_meta.id_cols.iter() {
                cols.push(col.clone());
                let col_value = after.get(col);
                if col_value.is_none() || *col_value.unwrap() == ColValue::None {
                    bail! {
                        "schema: {}, tb: {}, where col: {} is NULL, which should not happen in batch select",
                        self.rdb_tb_meta.schema, self.rdb_tb_meta.tb, col
                    }
                }
                binds.push(col_value);
            }
        }
        Ok(RdbQueryInfo { sql, cols, binds })
    }

    pub fn build_extract_cols_str(&self) -> anyhow::Result<String> {
        let mut extract_cols = Vec::new();
        for col in self.rdb_tb_meta.cols.iter() {
            if self.ignore_cols.is_some_and(|cols| cols.contains(col)) {
                continue;
            }

            if let Some(tb_meta) = self.pg_tb_meta {
                let col_type = tb_meta.get_col_type(col)?;
                let extract_type = PgColValueConvertor::get_extract_type(col_type);
                let extract_col = if extract_type.is_empty() {
                    self.escape(col)
                } else {
                    format!("{}::{}", self.escape(col), extract_type)
                };
                extract_cols.push(extract_col);
            } else {
                extract_cols.push(self.escape(col));
            }
        }
        Ok(extract_cols.join(","))
    }

    fn get_where_info(
        &self,
        mut index: usize,
        col_value_map: &HashMap<String, ColValue>,
        placeholder: bool,
    ) -> anyhow::Result<(String, Vec<String>)> {
        let mut where_sql = String::new();
        let mut not_null_cols = Vec::new();

        for col in self.rdb_tb_meta.id_cols.iter() {
            if !where_sql.is_empty() {
                where_sql += " AND";
            }

            let escaped_col = self.escape(col);
            let col_value = col_value_map.get(col);
            if let Some(value) = col_value {
                if *value == ColValue::None {
                    where_sql = format!("{} {} IS NULL", where_sql, escaped_col);
                } else {
                    let sql_value =
                        self.get_sql_value(index, col, &col_value_map.get(col), placeholder)?;
                    where_sql = format!("{} {} = {}", where_sql, escaped_col, sql_value);
                    not_null_cols.push(col.clone());
                }
            } else {
                where_sql = format!("{} {} IS NULL", where_sql, escaped_col);
            }

            index += 1;
        }
        Ok((where_sql.trim_start().into(), not_null_cols))
    }

    fn get_where_in_info(&self, batch_size: usize) -> anyhow::Result<String> {
        let mut all_placeholders = Vec::new();
        let mut placeholder_index = 1;
        for _ in 0..batch_size {
            let mut placeholders = Vec::new();
            for col in self.rdb_tb_meta.id_cols.iter() {
                placeholders.push(self.get_placeholder(placeholder_index, col)?);
                placeholder_index += 1;
            }
            all_placeholders.push(format!("({})", placeholders.join(",")));
        }

        Ok(format!(
            "({}) IN ({})",
            self.escape_cols(&self.rdb_tb_meta.id_cols).join(","),
            all_placeholders.join(",")
        ))
    }

    fn get_sql_value(
        &self,
        index: usize,
        col: &str,
        col_value: &Option<&ColValue>,
        placeholder: bool,
    ) -> anyhow::Result<String> {
        if placeholder {
            return self.get_placeholder(index, col);
        }

        if col_value.is_none() {
            return Ok("NULL".to_string());
        }
        if col_value.unwrap().is_unchanged_toast() {
            bail! {Error::Unexpected(format!(
                "schema: {}, tb: {}, col: {}, UnchangedToast should not be converted to sql value directly",
                self.rdb_tb_meta.schema, self.rdb_tb_meta.tb, col
            ))}
        }

        if self.mysql_tb_meta.is_some() {
            return self.get_mysql_sql_value(col, col_value.unwrap());
        }

        Ok(self.get_pg_sql_value(col_value.unwrap()))
    }

    fn get_pg_sql_value(&self, col_value: &ColValue) -> String {
        match col_value {
            ColValue::Blob(v) => format!(r#"'\x{}'"#, hex::encode(v)),

            _ => {
                if let Some(str) = col_value.to_option_string() {
                    format!(r#"'{}'"#, str.replace('\'', "\'\'"))
                } else {
                    "NULL".to_string()
                }
            }
        }
    }

    fn get_mysql_sql_value(&self, col: &str, col_value: &ColValue) -> anyhow::Result<String> {
        let (value, is_hex_str) = match col_value {
            // varchar, char, tinytext, mediumtext, longtext, text
            ColValue::RawString(v) => SqlUtil::binary_to_str(v),

            // tinyblob, mediumblob, longblob, blob, varbinary, binary
            ColValue::Blob(v) => (hex::encode(v), true),

            _ => {
                if let Some(v) = col_value.to_option_string() {
                    (v, false)
                } else {
                    return Ok("NULL".to_string());
                }
            }
        };

        if is_hex_str {
            return Ok(format!("x'{}'", value));
        }

        let col_type = self.mysql_tb_meta.unwrap().get_col_type(col)?;
        let is_str = match col_type {
            MysqlColType::DateTime { .. }
            | MysqlColType::Time { .. }
            | MysqlColType::Date { .. }
            | MysqlColType::Timestamp { .. }
            | MysqlColType::Binary { .. }
            | MysqlColType::VarBinary { .. }
            | MysqlColType::Json => true,
            MysqlColType::Enum { .. } => !matches!(col_value, ColValue::Enum(_)),
            MysqlColType::Set { .. } => !matches!(col_value, ColValue::Set(_)),
            _ => col_type.is_string(),
        };

        if is_str {
            // INSERT INTO tb1 VALUES(1, 'abc''');
            Ok(format!(r#"'{}'"#, value.replace('\'', "\'\'")))
        } else {
            Ok(value)
        }
    }

    fn get_placeholder(&self, index: usize, col: &str) -> anyhow::Result<String> {
        if let Some(tb_meta) = self.pg_tb_meta {
            let col_type = tb_meta.get_col_type(col)?;
            if col_type.schema_name != "pg_catalog" {
                // for user-defined types, we need to add schema name as prefix, otherwise it will cause error
                return Ok(format!(
                    "${}::\"{}\".\"{}\"",
                    index, col_type.schema_name, col_type.alias
                ));
            }
            // TODO: workaround for types like bit(3)
            let col_type_name = if col_type.alias == "bit" {
                "varbit"
            } else {
                &col_type.alias
            };
            return Ok(format!("${}::{}", index, col_type_name));
        }

        Ok("?".to_string())
    }

    fn escape(&self, origin: &str) -> String {
        SqlUtil::escape_by_db_type(origin, &self.db_type)
    }

    fn escape_cols(&self, cols: &Vec<String>) -> Vec<String> {
        SqlUtil::escape_cols(cols, &self.db_type)
    }

    fn check_primary_key_changed(&self, row_data: &RowData) -> bool {
        let Some(primary_key_cols) = self.rdb_tb_meta.key_map.get("primary") else {
            return false;
        };
        if self.rdb_tb_meta.id_cols.len() != primary_key_cols.len()
            || self
                .rdb_tb_meta
                .id_cols
                .iter()
                .zip(primary_key_cols.iter())
                .any(|(id_col, primary_col)| id_col != primary_col)
        {
            return false;
        }

        let before = row_data.before.as_ref().unwrap();
        let after = row_data.after.as_ref().unwrap();
        primary_key_cols
            .iter()
            .any(|col| before.get(col) != after.get(col))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dt_common::meta::{
        col_value::ColValue,
        pg::{pg_col_type::PgColType, pg_tb_meta::PgTbMeta, pg_value_type::PgValueType},
        rdb_tb_meta::RdbTbMeta,
        row_data::RowData,
        row_type::RowType,
    };

    use super::RdbQueryBuilder;

    fn build_pg_col_type(alias: &str) -> PgColType {
        PgColType {
            value_type: PgValueType::from_alias(alias),
            name: alias.to_string(),
            alias: alias.to_string(),
            oid: 0,
            parent_oid: 0,
            element_oid: 0,
            category: "S".to_string(),
            enum_values: None,
            schema_name: "pg_catalog".to_string(),
        }
    }

    fn build_pg_tb_meta() -> PgTbMeta {
        let mut key_map = HashMap::new();
        key_map.insert("primary".to_string(), vec!["id".to_string()]);
        key_map.insert("uk_code".to_string(), vec!["code".to_string()]);

        let mut col_type_map = HashMap::new();
        col_type_map.insert("id".to_string(), build_pg_col_type("int4"));
        col_type_map.insert("code".to_string(), build_pg_col_type("text"));
        col_type_map.insert("name".to_string(), build_pg_col_type("text"));

        PgTbMeta {
            basic: RdbTbMeta {
                schema: "public".to_string(),
                tb: "t1".to_string(),
                cols: vec!["id".to_string(), "code".to_string(), "name".to_string()],
                col_origin_type_map: HashMap::new(),
                key_map,
                order_col: Some("id".to_string()),
                partition_col: "id".to_string(),
                id_cols: vec!["id".to_string()],
                foreign_keys: vec![],
                ref_by_foreign_keys: vec![],
            },
            oid: 1,
            col_type_map,
        }
    }

    fn build_pg_tb_meta_without_primary() -> PgTbMeta {
        let mut key_map = HashMap::new();
        key_map.insert("uk_code".to_string(), vec!["code".to_string()]);

        let mut col_type_map = HashMap::new();
        col_type_map.insert("id".to_string(), build_pg_col_type("int4"));
        col_type_map.insert("code".to_string(), build_pg_col_type("text"));
        col_type_map.insert("name".to_string(), build_pg_col_type("text"));

        PgTbMeta {
            basic: RdbTbMeta {
                schema: "public".to_string(),
                tb: "t1".to_string(),
                cols: vec!["id".to_string(), "code".to_string(), "name".to_string()],
                col_origin_type_map: HashMap::new(),
                key_map,
                order_col: Some("code".to_string()),
                partition_col: "code".to_string(),
                id_cols: vec!["code".to_string()],
                foreign_keys: vec![],
                ref_by_foreign_keys: vec![],
            },
            oid: 1,
            col_type_map,
        }
    }

    fn build_pg_tb_meta_with_invalid_id_cols() -> PgTbMeta {
        let mut tb_meta = build_pg_tb_meta();
        tb_meta.basic.id_cols = vec!["code".to_string()];
        tb_meta
    }

    fn build_pg_tb_meta_without_keys() -> PgTbMeta {
        let mut col_type_map = HashMap::new();
        col_type_map.insert("id".to_string(), build_pg_col_type("int4"));
        col_type_map.insert("code".to_string(), build_pg_col_type("text"));
        col_type_map.insert("name".to_string(), build_pg_col_type("text"));

        PgTbMeta {
            basic: RdbTbMeta {
                schema: "public".to_string(),
                tb: "t1".to_string(),
                cols: vec!["id".to_string(), "code".to_string(), "name".to_string()],
                col_origin_type_map: HashMap::new(),
                key_map: HashMap::new(),
                order_col: None,
                partition_col: "id".to_string(),
                id_cols: vec!["id".to_string(), "code".to_string(), "name".to_string()],
                foreign_keys: vec![],
                ref_by_foreign_keys: vec![],
            },
            oid: 1,
            col_type_map,
        }
    }

    fn build_insert_row_data(is_not_origin: bool) -> RowData {
        let mut after = HashMap::new();
        after.insert("id".to_string(), ColValue::Long(1));
        after.insert("code".to_string(), ColValue::String("xx".to_string()));
        after.insert("name".to_string(), ColValue::String("n1".to_string()));

        if is_not_origin {
            RowData::new_no_origin(
                "public".to_string(),
                "t1".to_string(),
                RowType::Insert,
                None,
                Some(after),
            )
        } else {
            RowData::new(
                "public".to_string(),
                "t1".to_string(),
                RowType::Insert,
                None,
                Some(after),
            )
        }
    }

    fn build_pk_changed_update_row_data() -> RowData {
        let mut before = HashMap::new();
        before.insert("id".to_string(), ColValue::Long(1));
        before.insert("code".to_string(), ColValue::String("xx".to_string()));
        before.insert("name".to_string(), ColValue::String("n1".to_string()));

        let mut after = HashMap::new();
        after.insert("id".to_string(), ColValue::Long(2));
        after.insert("code".to_string(), ColValue::String("xx".to_string()));
        after.insert("name".to_string(), ColValue::String("n2".to_string()));

        RowData::new(
            "public".to_string(),
            "t1".to_string(),
            RowType::Update,
            Some(before),
            Some(after),
        )
    }

    fn build_update_row_data_with_unchanged_toast(pk_changed: bool) -> RowData {
        let mut before = HashMap::new();
        before.insert("id".to_string(), ColValue::Long(1));
        before.insert("code".to_string(), ColValue::String("xx".to_string()));
        before.insert("name".to_string(), ColValue::String("n1".to_string()));

        let mut after = HashMap::new();
        after.insert(
            "id".to_string(),
            ColValue::Long(if pk_changed { 2 } else { 1 }),
        );
        after.insert("code".to_string(), ColValue::UnchangedToast);
        after.insert("name".to_string(), ColValue::String("n2".to_string()));

        RowData::new(
            "public".to_string(),
            "t1".to_string(),
            RowType::Update,
            Some(before),
            Some(after),
        )
    }

    fn build_update_row_data_with_only_unchanged_toast() -> RowData {
        let mut before = HashMap::new();
        before.insert("id".to_string(), ColValue::Long(1));
        before.insert("code".to_string(), ColValue::String("xx".to_string()));
        before.insert("name".to_string(), ColValue::String("n1".to_string()));

        let mut after = HashMap::new();
        after.insert("id".to_string(), ColValue::Long(1));
        after.insert("code".to_string(), ColValue::UnchangedToast);
        after.insert("name".to_string(), ColValue::UnchangedToast);

        RowData::new(
            "public".to_string(),
            "t1".to_string(),
            RowType::Update,
            Some(before),
            Some(after),
        )
    }

    fn build_plain_update_row_data() -> RowData {
        let mut before = HashMap::new();
        before.insert("id".to_string(), ColValue::Long(1));
        before.insert("code".to_string(), ColValue::String("xx".to_string()));
        before.insert("name".to_string(), ColValue::String("n1".to_string()));

        let mut after = HashMap::new();
        after.insert("id".to_string(), ColValue::Long(1));
        after.insert("code".to_string(), ColValue::String("xx".to_string()));
        after.insert("name".to_string(), ColValue::String("n2".to_string()));

        RowData::new(
            "public".to_string(),
            "t1".to_string(),
            RowType::Update,
            Some(before),
            Some(after),
        )
    }

    fn build_delete_row_data() -> RowData {
        let mut before = HashMap::new();
        before.insert("id".to_string(), ColValue::Long(1));
        before.insert("code".to_string(), ColValue::String("xx".to_string()));
        before.insert("name".to_string(), ColValue::String("n1".to_string()));

        RowData::new(
            "public".to_string(),
            "t1".to_string(),
            RowType::Delete,
            Some(before),
            None,
        )
    }

    #[test]
    fn test_pg_origin_replace_query_skips_any_unique_conflict() {
        let tb_meta = build_pg_tb_meta();
        let row_data = build_insert_row_data(false);
        let builder = RdbQueryBuilder::new_for_pg(&tb_meta, None);

        let query_info = builder.get_query_info(&row_data, true).unwrap();

        assert!(query_info.sql.contains("WITH inserted AS (INSERT INTO"));
        assert!(query_info
            .sql
            .contains("ON CONFLICT DO NOTHING RETURNING 1"));
        assert!(query_info
            .sql
            .contains(r#"UPDATE "public"."t1" SET "name"=$4::text"#));
        assert!(query_info.sql.contains(r#"WHERE "id"=$5::int4"#));
        assert!(!query_info.sql.contains(r#""code"=$"#));
    }

    #[test]
    fn test_pg_non_origin_replace_query_still_updates_unique_cols() {
        let tb_meta = build_pg_tb_meta();
        let row_data = build_insert_row_data(true);
        let builder = RdbQueryBuilder::new_for_pg(&tb_meta, None);

        let query_info = builder.get_query_info(&row_data, true).unwrap();

        assert!(query_info
            .sql
            .contains(r#"ON CONFLICT ("id") DO UPDATE SET"#));
        assert!(query_info.sql.contains(r#""code"=$4::text"#));
        assert!(query_info.sql.contains(r#""name"=$5::text"#));
        assert!(!query_info.sql.contains("WITH inserted AS"));
    }

    #[test]
    fn test_pg_origin_replace_query_without_primary_does_nothing_on_conflict() {
        let tb_meta = build_pg_tb_meta_without_primary();
        let row_data = build_insert_row_data(false);
        let builder = RdbQueryBuilder::new_for_pg(&tb_meta, None);

        let query_info = builder.get_query_info(&row_data, true).unwrap();

        assert_eq!(
            query_info.sql,
            r#"INSERT INTO "public"."t1"("id","code","name") VALUES($1::int4,$2::text,$3::text) ON CONFLICT DO NOTHING"#
        );
    }

    #[test]
    fn test_pg_origin_replace_query_with_invalid_id_cols_does_nothing_on_conflict() {
        let tb_meta = build_pg_tb_meta_with_invalid_id_cols();
        let row_data = build_insert_row_data(false);
        let builder = RdbQueryBuilder::new_for_pg(&tb_meta, None);

        let query_info = builder.get_query_info(&row_data, true).unwrap();

        assert_eq!(
            query_info.sql,
            r#"INSERT INTO "public"."t1"("id","code","name") VALUES($1::int4,$2::text,$3::text) ON CONFLICT DO NOTHING"#
        );
    }

    #[test]
    fn test_pg_replace_pk_changed_update_rewrites_to_delete_insert() {
        let tb_meta = build_pg_tb_meta();
        let row_data = build_pk_changed_update_row_data();
        let builder = RdbQueryBuilder::new_for_pg(&tb_meta, None);

        let query_info = builder.get_query_info(&row_data, true).unwrap();

        assert!(query_info
            .sql
            .contains(r#"WITH deleted AS (DELETE FROM "public"."t1" WHERE "id"=$1::int4)"#));
        assert!(query_info.sql.contains(
            r#"INSERT INTO "public"."t1"("id","code","name") VALUES($2::int4,$3::text,$4::text)"#
        ));
        assert!(query_info.sql.contains(
            r#"ON CONFLICT ("id") DO UPDATE SET "code"=EXCLUDED."code","name"=EXCLUDED."name""#
        ));
    }

    #[test]
    fn test_pg_update_query_skips_unchanged_toast_cols() {
        let tb_meta = build_pg_tb_meta();
        let row_data = build_update_row_data_with_unchanged_toast(false);
        let builder = RdbQueryBuilder::new_for_pg(&tb_meta, None);

        let query_info = builder.get_query_info(&row_data, false).unwrap();

        assert!(query_info.sql.contains(r#""name"="#));
        assert!(!query_info.sql.contains(r#""code"="#));
    }

    #[test]
    fn test_pg_replace_pk_changed_update_with_unchanged_toast_falls_back_to_update() {
        let tb_meta = build_pg_tb_meta();
        let row_data = build_update_row_data_with_unchanged_toast(true);
        let builder = RdbQueryBuilder::new_for_pg(&tb_meta, None);

        let query_info = builder.get_query_info(&row_data, true).unwrap();

        assert!(query_info.sql.starts_with(r#"UPDATE "public"."t1" SET"#));
        assert!(!query_info.sql.contains("WITH deleted AS"));
        assert!(!query_info.sql.contains(r#""code"="#));
    }

    #[test]
    fn test_pg_update_query_with_only_unchanged_toast_does_not_include_toast_cols() {
        let tb_meta = build_pg_tb_meta();
        let row_data = build_update_row_data_with_only_unchanged_toast();
        let builder = RdbQueryBuilder::new_for_pg(&tb_meta, None);

        let query_info = builder.get_query_info(&row_data, false).unwrap();

        assert!(query_info.sql.starts_with(r#"UPDATE "public"."t1" SET"#));
        assert!(query_info.sql.contains(r#""id"="#));
        assert!(!query_info.sql.contains(r#""code"="#));
        assert!(!query_info.sql.contains(r#""name"="#));
    }

    #[test]
    fn test_pg_delete_without_keys_uses_ctid_limit_one() {
        let tb_meta = build_pg_tb_meta_without_keys();
        let row_data = build_delete_row_data();
        let builder = RdbQueryBuilder::new_for_pg(&tb_meta, None);

        let query_info = builder.get_query_info(&row_data, false).unwrap();

        assert!(query_info.sql.contains(r#"DELETE FROM "public"."t1" WHERE ctid IN ("#));
        assert!(query_info.sql.contains(r#"SELECT ctid FROM "public"."t1" WHERE"#));
        assert!(query_info.sql.contains("LIMIT 1"));
    }

    #[test]
    fn test_pg_update_without_keys_uses_ctid_limit_one() {
        let tb_meta = build_pg_tb_meta_without_keys();
        let row_data = build_plain_update_row_data();
        let builder = RdbQueryBuilder::new_for_pg(&tb_meta, None);

        let query_info = builder.get_query_info(&row_data, false).unwrap();

        assert!(query_info.sql.starts_with(r#"UPDATE "public"."t1" SET"#));
        assert!(query_info.sql.contains(r#"WHERE ctid IN (SELECT ctid FROM "public"."t1" WHERE"#));
        assert!(query_info.sql.contains("LIMIT 1"));
    }
}
