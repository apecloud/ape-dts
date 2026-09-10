use std::collections::{HashMap, HashSet};

use anyhow::bail;
use dt_common::{
    config::config_enums::DbType,
    error::{DtError, DtOptionExt},
    meta::{
        adaptor::pg_col_value_convertor::PgColValueConvertor,
        mssql::{mssql_query_builder::MssqlTableSqlBuilder, mssql_tb_meta::MssqlTbMeta},
        mysql::mysql_tb_meta::MysqlTbMeta,
        pg::pg_tb_meta::PgTbMeta,
        rdb_tb_meta::{RdbTbMeta, SortDirection},
    },
    utils::sql_util::SqlUtil,
};

use super::rdb_snapshot_query::RdbSnapshotQuery;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OrderKeyPredicateType {
    #[default]
    None,
    /// Strictly after the cursor in ORDER BY order.
    After,
    /// At or before the cursor in ORDER BY order.
    AtOrBefore,
    Range,
    IsNull,
}

pub struct RdbSnapshotExtractStatement<'a> {
    db_type: DbType,
    rdb_tb_meta: &'a RdbTbMeta,
    pg_tb_meta: Option<&'a PgTbMeta>,
    mysql_tb_meta: Option<&'a MysqlTbMeta>,
    mssql_tb_meta: Option<&'a MssqlTbMeta>,
    order_cols: Option<&'a [String]>,
    order_col_attrs: Option<&'a HashMap<String, SortDirection>>,
    ignore_cols: Option<&'a HashSet<String>>,
    where_condition: Option<&'a String>,
    limit: usize,
    predicate_type: OrderKeyPredicateType,
}

impl<'r> From<&'r MysqlTbMeta> for RdbSnapshotExtractStatement<'r> {
    fn from(mysql_tb_meta: &'r MysqlTbMeta) -> Self {
        RdbSnapshotExtractStatement {
            db_type: DbType::Mysql,
            rdb_tb_meta: &mysql_tb_meta.basic,
            mysql_tb_meta: Some(mysql_tb_meta),
            pg_tb_meta: None,
            mssql_tb_meta: None,
            order_cols: None,
            order_col_attrs: None,
            ignore_cols: None,
            where_condition: None,
            limit: 0,
            predicate_type: OrderKeyPredicateType::None,
        }
    }
}

impl<'r> From<&'r PgTbMeta> for RdbSnapshotExtractStatement<'r> {
    fn from(pg_tb_meta: &'r PgTbMeta) -> Self {
        RdbSnapshotExtractStatement {
            db_type: DbType::Pg,
            rdb_tb_meta: &pg_tb_meta.basic,
            mysql_tb_meta: None,
            pg_tb_meta: Some(pg_tb_meta),
            mssql_tb_meta: None,
            order_cols: None,
            order_col_attrs: None,
            ignore_cols: None,
            where_condition: None,
            limit: 0,
            predicate_type: OrderKeyPredicateType::None,
        }
    }
}

impl<'r> From<&'r MssqlTbMeta> for RdbSnapshotExtractStatement<'r> {
    fn from(mssql_tb_meta: &'r MssqlTbMeta) -> Self {
        RdbSnapshotExtractStatement {
            db_type: DbType::Mssql,
            rdb_tb_meta: &mssql_tb_meta.basic,
            mysql_tb_meta: None,
            pg_tb_meta: None,
            mssql_tb_meta: Some(mssql_tb_meta),
            order_cols: None,
            order_col_attrs: None,
            ignore_cols: None,
            where_condition: None,
            limit: 0,
            predicate_type: OrderKeyPredicateType::None,
        }
    }
}

impl<'r> RdbSnapshotExtractStatement<'r> {
    #[inline(always)]
    pub fn with_ignore_cols(mut self, ignore_cols: &'r HashSet<String>) -> Self {
        self.ignore_cols = Some(ignore_cols);
        self
    }

    #[inline(always)]
    pub fn with_order_cols(mut self, order_cols: &'r [String]) -> Self {
        self.order_cols = Some(order_cols);
        self
    }

    #[inline(always)]
    pub fn with_order_col_attrs(mut self, attrs: &'r HashMap<String, SortDirection>) -> Self {
        self.order_col_attrs = Some(attrs);
        self
    }

    #[inline(always)]
    pub fn with_where_condition(mut self, where_condition: &'r String) -> Self {
        self.where_condition = Some(where_condition);
        self
    }

    #[inline(always)]
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    #[inline(always)]
    pub fn with_predicate_type(mut self, predicate_type: OrderKeyPredicateType) -> Self {
        self.predicate_type = predicate_type;
        self
    }

    pub fn build(&self) -> anyhow::Result<RdbSnapshotQuery> {
        let mut cols = Vec::new();
        for col in self.order_cols.unwrap_or_default() {
            self.direction(col)?;
        }
        let extract_cols_str = self.build_extract_cols_str()?;
        let top = if matches!(self.db_type, DbType::Mssql) && self.limit > 0 {
            format!("TOP ({}) ", self.limit)
        } else {
            String::new()
        };
        let mut sql = format!(
            "SELECT {}{} FROM {}",
            top,
            extract_cols_str,
            self.table_name()
        );
        let mut predicates: Vec<String> = Vec::new();
        match self.where_condition {
            Some(where_condition) if !where_condition.is_empty() => {
                predicates.push(format!("({where_condition})"));
            }
            _ => (),
        }
        if let Some(order_cols) = self.order_cols.filter(|cols| !cols.is_empty()) {
            let predicate = match self.predicate_type {
                OrderKeyPredicateType::After => {
                    self.build_order_col_predicate(order_cols, true, &mut cols)?
                }
                OrderKeyPredicateType::AtOrBefore => {
                    self.build_order_col_predicate(order_cols, false, &mut cols)?
                }
                OrderKeyPredicateType::Range => {
                    let start = self.build_order_col_predicate(order_cols, true, &mut cols)?;
                    let end = self.build_order_col_predicate(order_cols, false, &mut cols)?;
                    format!("{start} AND {end}")
                }
                _ => String::new(),
            };
            if !predicate.is_empty() {
                predicates.push(predicate);
            }
            match self.predicate_type {
                OrderKeyPredicateType::After
                | OrderKeyPredicateType::AtOrBefore
                | OrderKeyPredicateType::Range
                | OrderKeyPredicateType::None => {
                    let null_predicate = self.build_null_predicate(order_cols, false)?;
                    if !null_predicate.is_empty() {
                        predicates.push(null_predicate);
                    }
                }
                OrderKeyPredicateType::IsNull => {
                    let null_predicate = self.build_null_predicate(order_cols, true)?;
                    if !null_predicate.is_empty() {
                        predicates.push(if predicates.is_empty() {
                            null_predicate
                        } else {
                            format!("({})", null_predicate)
                        });
                    }
                }
            }
        }

        if !predicates.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&predicates.join(" AND "));
        }
        if let Some(order_cols) = self.order_cols.filter(|cols| !cols.is_empty()) {
            sql.push_str(" ORDER BY ");
            sql.push_str(&self.build_order_by_clause(order_cols)?);
        }
        if self.limit > 0 && !matches!(self.db_type, DbType::Mssql) {
            sql.push_str(&format!(" LIMIT {}", self.limit));
        }
        Ok(RdbSnapshotQuery { sql, cols })
    }

    fn build_extract_cols_str(&self) -> anyhow::Result<String> {
        if let Some(tb_meta) = self.mssql_tb_meta {
            let mut ignore_cols = self.ignore_cols.cloned().unwrap_or_default();
            for col in self.order_cols.unwrap_or_default() {
                ignore_cols.remove(col);
            }
            return MssqlTableSqlBuilder::new(tb_meta, Some(&ignore_cols)).build_extract_cols_str();
        }
        let mut extract_cols = Vec::new();
        for col in self.rdb_tb_meta.cols.iter() {
            if self.ignore_cols.is_some_and(|cols| cols.contains(col))
                && !self.order_cols.is_some_and(|cols| cols.contains(col))
            {
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
                let col_type = self
                    .mysql_tb_meta
                    .or_dt_error(DtError::InvariantViolated(
                        "MySQL table metadata is missing while building snapshot SQL".to_string(),
                    ))?
                    .get_col_type(col)?;
                let extract_col = if col_type.is_spatial() {
                    SqlUtil::mysql_spatial_as_text_expr(&self.escape(col), &self.escape(col))
                } else {
                    self.escape(col)
                };
                extract_cols.push(extract_col);
            }
        }
        Ok(extract_cols.join(","))
    }

    #[inline(always)]
    fn build_order_col_str(&self, order_cols: &[String]) -> String {
        order_cols
            .iter()
            .map(|col| self.escape(col).to_string())
            .collect::<Vec<String>>()
            .join(", ")
    }

    fn build_placeholder(&self, col: &str, cols: &mut Vec<String>) -> anyhow::Result<String> {
        cols.push(col.to_string());
        let index = cols.len();
        if let Some(tb_meta) = self.pg_tb_meta {
            Ok(format!(
                "${index}::{}",
                tb_meta.get_col_type(col)?.get_alias()
            ))
        } else if let Some(tb_meta) = self.mysql_tb_meta {
            Ok(SqlUtil::mysql_comparison_placeholder(
                tb_meta.get_col_type(col)?,
            ))
        } else if self.mssql_tb_meta.is_some() {
            Ok(format!("@P{index}"))
        } else {
            bail!(DtError::InvariantViolated(format!(
                "unsupported snapshot database: {:?}",
                self.db_type
            )))
        }
    }

    fn direction(&self, col: &str) -> anyhow::Result<SortDirection> {
        self.order_col_attrs
            .and_then(|attrs| attrs.get(col))
            .copied()
            .or_dt_error(DtError::InvariantViolated(format!(
                "missing snapshot sort direction for column {col}"
            )))
    }

    fn comparison_operator(
        &self,
        col: &str,
        after: bool,
        inclusive: bool,
    ) -> anyhow::Result<&'static str> {
        let greater = after == (self.direction(col)? == SortDirection::Asc);
        Ok(match (greater, inclusive) {
            (true, false) => ">",
            (true, true) => ">=",
            (false, false) => "<",
            (false, true) => "<=",
        })
    }

    fn build_order_col_predicate(
        &self,
        order_cols: &[String],
        after: bool,
        cols: &mut Vec<String>,
    ) -> anyhow::Result<String> {
        let first = &order_cols[0];
        let first_direction = self.direction(first)?;
        let uniform = order_cols.iter().all(|col| {
            self.direction(col)
                .is_ok_and(|direction| direction == first_direction)
        });
        if order_cols.len() == 1 || (uniform && self.mssql_tb_meta.is_none()) {
            let names = self.build_order_col_str(order_cols);
            let placeholders = order_cols
                .iter()
                .map(|col| self.build_placeholder(col, cols))
                .collect::<anyhow::Result<Vec<_>>>()?
                .join(", ");
            let operator = self.comparison_operator(first, after, !after)?;
            return Ok(if order_cols.len() == 1 {
                format!("{names} {operator} {placeholders}")
            } else {
                format!("({names}) {operator} ({placeholders})")
            });
        }

        // Compare each column only after all preceding columns compare equal.
        // Only the final alternative is inclusive for AtOrBefore.
        let mut alternatives = Vec::with_capacity(order_cols.len());
        for (index, col) in order_cols.iter().enumerate() {
            let mut terms = Vec::with_capacity(index + 1);
            for prefix in &order_cols[..index] {
                terms.push(format!(
                    "{} = {}",
                    self.escape(prefix),
                    self.build_placeholder(prefix, cols)?
                ));
            }
            let operator =
                self.comparison_operator(col, after, !after && index + 1 == order_cols.len())?;
            terms.push(format!(
                "{} {operator} {}",
                self.escape(col),
                self.build_placeholder(col, cols)?
            ));
            alternatives.push(format!("({})", terms.join(" AND ")));
        }
        Ok(format!("({})", alternatives.join(" OR ")))
    }

    fn build_null_predicate(&self, order_cols: &[String], is_null: bool) -> anyhow::Result<String> {
        let null_check = if is_null { "IS NULL" } else { "IS NOT NULL" };
        let join_str = if is_null { "OR" } else { "AND" };
        if order_cols.is_empty() {
            Ok(String::new())
        } else {
            // col_1 IS NOT NULL AND col_2 IS NOT NULL AND col_3 IS NOT NULL
            // col_1 IS NULL OR col_2 IS NULL OR col_3 IS NULL
            Ok(order_cols
                .iter()
                .filter(|&col| self.rdb_tb_meta.is_col_nullable(col))
                .map(|col| format!(r#"{} {}"#, self.escape(col), null_check))
                .collect::<Vec<String>>()
                .join(&format!(" {} ", join_str)))
        }
    }

    fn build_order_by_clause(&self, order_cols: &[String]) -> anyhow::Result<String> {
        order_cols
            .iter()
            .map(|col| {
                Ok(format!(
                    "{}.{} {}",
                    self.table_name(),
                    self.escape(col),
                    self.direction(col)?
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()
            .map(|cols| cols.join(", "))
    }

    #[inline(always)]
    fn escape(&self, token: &str) -> String {
        SqlUtil::escape_by_db_type(token, &self.db_type)
    }

    fn table_name(&self) -> String {
        SqlUtil::render_rdb_table(
            &self.db_type,
            &self.rdb_tb_meta.db,
            &self.rdb_tb_meta.schema,
            &self.rdb_tb_meta.tb,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dt_common::meta::{
        mssql::{mssql_col_type::MssqlColType, mssql_tb_meta::MssqlTbMeta},
        mysql::mysql_col_type::MysqlColType,
        mysql::mysql_tb_meta::MysqlTbMeta,
        pg::pg_col_type::PgColType,
        pg::pg_tb_meta::PgTbMeta,
        pg::pg_value_type::PgValueType,
        rdb_tb_meta::RdbTbMeta,
    };

    use super::*;

    fn create_mysql_tb_meta() -> MysqlTbMeta {
        let cols = vec![
            "id".to_string(),
            "price".to_string(),
            "username".to_string(),
            "bio".to_string(),
            "large_blob".to_string(),
        ];

        let mut nullable_cols = HashSet::new();
        nullable_cols.insert("price".to_string());
        nullable_cols.insert("bio".to_string());
        nullable_cols.insert("large_blob".to_string());

        let basic = RdbTbMeta {
            schema: "test_schema".to_string(),
            tb: "test_table".to_string(),
            cols,
            nullable_cols,
            ..Default::default()
        };

        let mut col_type_map = HashMap::new();
        col_type_map.insert("id".to_string(), MysqlColType::BigInt { unsigned: false });
        col_type_map.insert("price".to_string(), MysqlColType::Double);
        col_type_map.insert(
            "username".to_string(),
            MysqlColType::Varchar {
                length: 100,
                charset: "utf8mb4".to_string(),
            },
        );
        col_type_map.insert(
            "bio".to_string(),
            MysqlColType::Text {
                length: 65535,
                charset: "utf8mb4".to_string(),
            },
        );
        col_type_map.insert("large_blob".to_string(), MysqlColType::Blob);

        MysqlTbMeta {
            basic,
            col_type_map,
        }
    }

    fn create_mysql_time_order_tb_meta() -> MysqlTbMeta {
        let cols = vec![
            "time_col".to_string(),
            "year_col".to_string(),
            "val".to_string(),
        ];

        let basic = RdbTbMeta {
            schema: "test_schema".to_string(),
            tb: "time_order_table".to_string(),
            cols,
            ..Default::default()
        };

        let mut col_type_map = HashMap::new();
        col_type_map.insert("time_col".to_string(), MysqlColType::Time { precision: 6 });
        col_type_map.insert("year_col".to_string(), MysqlColType::Year);
        col_type_map.insert("val".to_string(), MysqlColType::Int { unsigned: false });

        MysqlTbMeta {
            basic,
            col_type_map,
        }
    }

    fn create_mysql_spatial_tb_meta() -> MysqlTbMeta {
        let basic = RdbTbMeta {
            schema: "test_schema".to_string(),
            tb: "spatial_table".to_string(),
            cols: vec!["id".to_string(), "shape".to_string()],
            order_cols: vec!["id".to_string()],
            ..Default::default()
        };
        let col_type_map = HashMap::from([
            ("id".to_string(), MysqlColType::Int { unsigned: false }),
            ("shape".to_string(), MysqlColType::Point),
        ]);
        MysqlTbMeta {
            basic,
            col_type_map,
        }
    }

    fn create_pg_tb_meta() -> PgTbMeta {
        let cols = vec![
            "id".to_string(),
            "price".to_string(),
            "username".to_string(),
            "bio".to_string(),
            "large_blob".to_string(),
        ];

        let mut nullable_cols = HashSet::new();
        nullable_cols.insert("price".to_string());
        nullable_cols.insert("bio".to_string());
        nullable_cols.insert("large_blob".to_string());

        let basic = RdbTbMeta {
            schema: "test_schema".to_string(),
            tb: "test_table".to_string(),
            cols,
            nullable_cols,
            ..Default::default()
        };

        let mut col_type_map = HashMap::new();
        // Integer type
        col_type_map.insert(
            "id".to_string(),
            PgColType {
                value_type: PgValueType::Int64,
                name: "bigint".to_string(),
                alias: "int8".to_string(),
                oid: 20,
                parent_oid: 0,
                element_oid: 0,
                category: "N".to_string(),
                enum_values: None,
                schema_name: "pg_catalog".to_string(),
                typmod: 0,
            },
        );
        // Float type
        col_type_map.insert(
            "price".to_string(),
            PgColType {
                value_type: PgValueType::Float64,
                name: "double precision".to_string(),
                alias: "float8".to_string(),
                oid: 701,
                parent_oid: 0,
                element_oid: 0,
                category: "N".to_string(),
                enum_values: None,
                schema_name: "pg_catalog".to_string(),
                typmod: 0,
            },
        );
        // Text types
        col_type_map.insert(
            "username".to_string(),
            PgColType {
                value_type: PgValueType::String,
                name: "character varying".to_string(),
                alias: "varchar".to_string(),
                oid: 1043,
                parent_oid: 0,
                element_oid: 0,
                category: "S".to_string(),
                enum_values: None,
                schema_name: "pg_catalog".to_string(),
                typmod: 0,
            },
        );
        col_type_map.insert(
            "bio".to_string(),
            PgColType {
                value_type: PgValueType::String,
                name: "text".to_string(),
                alias: "text".to_string(),
                oid: 25,
                parent_oid: 0,
                element_oid: 0,
                category: "S".to_string(),
                enum_values: None,
                schema_name: "pg_catalog".to_string(),
                typmod: 0,
            },
        );
        // Binary type
        col_type_map.insert(
            "large_blob".to_string(),
            PgColType {
                value_type: PgValueType::Bytes,
                name: "bytea".to_string(),
                alias: "bytea".to_string(),
                oid: 17,
                parent_oid: 0,
                element_oid: 0,
                category: "U".to_string(),
                enum_values: None,
                schema_name: "pg_catalog".to_string(),
                typmod: 0,
            },
        );

        PgTbMeta {
            basic,
            oid: 16384,
            col_type_map,
        }
    }

    fn create_mssql_tb_meta() -> MssqlTbMeta {
        let cols = vec![
            "tenant_id".to_string(),
            "id".to_string(),
            "name".to_string(),
        ];
        let nullable_cols = HashSet::from(["tenant_id".to_string(), "name".to_string()]);
        let basic = RdbTbMeta {
            schema: "test_schema".to_string(),
            tb: "test_table".to_string(),
            cols,
            nullable_cols,
            ..Default::default()
        };
        let col_type_map = HashMap::from([
            ("tenant_id".to_string(), MssqlColType::Int4),
            ("id".to_string(), MssqlColType::Int8),
            ("name".to_string(), MssqlColType::NVarchar),
        ]);
        MssqlTbMeta {
            basic,
            col_type_map,
            ..Default::default()
        }
    }

    fn create_pg_bit_order_tb_meta() -> PgTbMeta {
        let cols = vec!["bit_col".to_string(), "bit_array_col".to_string()];
        let mut nullable_cols = HashSet::new();
        nullable_cols.insert("bit_array_col".to_string());

        let basic = RdbTbMeta {
            schema: "test_schema".to_string(),
            tb: "bit_order_table".to_string(),
            cols,
            nullable_cols,
            ..Default::default()
        };

        let mut col_type_map = HashMap::new();
        col_type_map.insert(
            "bit_col".to_string(),
            PgColType {
                value_type: PgValueType::String,
                name: "bit".to_string(),
                alias: "bit".to_string(),
                oid: 1560,
                parent_oid: 0,
                element_oid: 0,
                category: "V".to_string(),
                enum_values: None,
                schema_name: "pg_catalog".to_string(),
                typmod: 10,
            },
        );
        col_type_map.insert(
            "bit_array_col".to_string(),
            PgColType {
                value_type: PgValueType::String,
                name: "_bit".to_string(),
                alias: "_bit".to_string(),
                oid: 1561,
                parent_oid: 0,
                element_oid: 1560,
                category: "A".to_string(),
                enum_values: None,
                schema_name: "pg_catalog".to_string(),
                typmod: 10,
            },
        );

        PgTbMeta {
            basic,
            oid: 16385,
            col_type_map,
        }
    }

    #[derive(Default)]
    struct Case {
        name: &'static str,
        fixture: &'static str,
        order_cols: &'static [(&'static str, SortDirection)],
        predicate: OrderKeyPredicateType,
        where_condition: &'static str,
        ignore_cols: &'static [&'static str],
        limit: usize,
        expected_sql: &'static str,
        expected_cols: &'static [&'static str],
    }

    fn create_mssql_binary_tb_meta() -> MssqlTbMeta {
        let mut meta = create_mssql_tb_meta();
        meta.basic.cols.push("shape".to_string());
        meta.col_type_map
            .insert("shape".to_string(), MssqlColType::AssemblyUdt);
        meta
    }

    #[test]
    fn test_build() {
        use OrderKeyPredicateType::*;
        use SortDirection::*;
        let cases = [
            Case {
                name: "mysql_single_order_col_after",
                fixture: "mysql_tb_meta",
                order_cols: &[("id", Asc)],
                predicate: After,
                limit: 100,
                expected_sql: r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE `id` > ? ORDER BY `test_schema`.`test_table`.`id` ASC LIMIT 100"#,
                expected_cols: &[r#"id"#],
                ..Default::default()
            },
            Case {
                name: "mysql_spatial_extract_preserves_srid",
                fixture: "mysql_spatial_tb_meta",
                expected_sql: r#"SELECT `id`,CONCAT(ST_SRID(`shape`), '|', ST_AsText(`shape`)) AS `shape` FROM `test_schema`.`spatial_table`"#,
                expected_cols: &[],
                ..Default::default()
            },
            Case {
                name: "mysql_multiple_order_cols_after",
                fixture: "mysql_tb_meta",
                order_cols: &[
                    ("id", Asc),
                    ("price", Asc),
                    ("username", Asc),
                    ("bio", Asc),
                    ("large_blob", Asc),
                ],
                predicate: After,
                limit: 100,
                expected_sql: r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE (`id`, `price`, `username`, `bio`, `large_blob`) > (?, ?, ?, ?, ?) AND `price` IS NOT NULL AND `bio` IS NOT NULL AND `large_blob` IS NOT NULL ORDER BY `test_schema`.`test_table`.`id` ASC, `test_schema`.`test_table`.`price` ASC, `test_schema`.`test_table`.`username` ASC, `test_schema`.`test_table`.`bio` ASC, `test_schema`.`test_table`.`large_blob` ASC LIMIT 100"#,
                expected_cols: &[
                    r#"id"#,
                    r#"price"#,
                    r#"username"#,
                    r#"bio"#,
                    r#"large_blob"#,
                ],
                ..Default::default()
            },
            Case {
                name: "mysql_time_order_col_after_casts_placeholder",
                fixture: "mysql_time_order_tb_meta",
                order_cols: &[("time_col", Asc), ("year_col", Asc)],
                predicate: After,
                limit: 4,
                expected_sql: r#"SELECT `time_col`,`year_col`,`val` FROM `test_schema`.`time_order_table` WHERE (`time_col`, `year_col`) > (CAST(? AS TIME(6)), ?) ORDER BY `test_schema`.`time_order_table`.`time_col` ASC, `test_schema`.`time_order_table`.`year_col` ASC LIMIT 4"#,
                expected_cols: &[r#"time_col"#, r#"year_col"#],
                ..Default::default()
            },
            Case {
                name: "mysql_single_order_col_range",
                fixture: "mysql_tb_meta",
                order_cols: &[("id", Asc)],
                predicate: Range,
                expected_sql: r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE `id` > ? AND `id` <= ? ORDER BY `test_schema`.`test_table`.`id` ASC"#,
                expected_cols: &[r#"id"#, r#"id"#],
                ..Default::default()
            },
            Case {
                name: "mysql_time_order_col_range_casts_placeholder",
                fixture: "mysql_time_order_tb_meta",
                order_cols: &[("time_col", Asc), ("year_col", Asc)],
                predicate: Range,
                expected_sql: r#"SELECT `time_col`,`year_col`,`val` FROM `test_schema`.`time_order_table` WHERE (`time_col`, `year_col`) > (CAST(? AS TIME(6)), ?) AND (`time_col`, `year_col`) <= (CAST(? AS TIME(6)), ?) ORDER BY `test_schema`.`time_order_table`.`time_col` ASC, `test_schema`.`time_order_table`.`year_col` ASC"#,
                expected_cols: &[r#"time_col"#, r#"year_col"#, r#"time_col"#, r#"year_col"#],
                ..Default::default()
            },
            Case {
                name: "mysql_multiple_order_cols_range",
                fixture: "mysql_tb_meta",
                order_cols: &[
                    ("id", Asc),
                    ("price", Asc),
                    ("username", Asc),
                    ("bio", Asc),
                    ("large_blob", Asc),
                ],
                predicate: Range,
                expected_sql: r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE (`id`, `price`, `username`, `bio`, `large_blob`) > (?, ?, ?, ?, ?) AND (`id`, `price`, `username`, `bio`, `large_blob`) <= (?, ?, ?, ?, ?) AND `price` IS NOT NULL AND `bio` IS NOT NULL AND `large_blob` IS NOT NULL ORDER BY `test_schema`.`test_table`.`id` ASC, `test_schema`.`test_table`.`price` ASC, `test_schema`.`test_table`.`username` ASC, `test_schema`.`test_table`.`bio` ASC, `test_schema`.`test_table`.`large_blob` ASC"#,
                expected_cols: &[
                    r#"id"#,
                    r#"price"#,
                    r#"username"#,
                    r#"bio"#,
                    r#"large_blob"#,
                    r#"id"#,
                    r#"price"#,
                    r#"username"#,
                    r#"bio"#,
                    r#"large_blob"#,
                ],
                ..Default::default()
            },
            Case {
                name: "mysql_null_predicate_with_nullable_cols",
                fixture: "mysql_tb_meta",
                order_cols: &[
                    ("id", Asc),
                    ("price", Asc),
                    ("username", Asc),
                    ("bio", Asc),
                    ("large_blob", Asc),
                ],
                predicate: AtOrBefore,
                expected_sql: r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE (`id`, `price`, `username`, `bio`, `large_blob`) <= (?, ?, ?, ?, ?) AND `price` IS NOT NULL AND `bio` IS NOT NULL AND `large_blob` IS NOT NULL ORDER BY `test_schema`.`test_table`.`id` ASC, `test_schema`.`test_table`.`price` ASC, `test_schema`.`test_table`.`username` ASC, `test_schema`.`test_table`.`bio` ASC, `test_schema`.`test_table`.`large_blob` ASC"#,
                expected_cols: &[
                    r#"id"#,
                    r#"price"#,
                    r#"username"#,
                    r#"bio"#,
                    r#"large_blob"#,
                ],
                ..Default::default()
            },
            Case {
                name: "mysql_is_null_predicate",
                fixture: "mysql_tb_meta",
                order_cols: &[
                    ("id", Asc),
                    ("price", Asc),
                    ("username", Asc),
                    ("bio", Asc),
                    ("large_blob", Asc),
                ],
                predicate: IsNull,
                expected_sql: r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE `price` IS NULL OR `bio` IS NULL OR `large_blob` IS NULL ORDER BY `test_schema`.`test_table`.`id` ASC, `test_schema`.`test_table`.`price` ASC, `test_schema`.`test_table`.`username` ASC, `test_schema`.`test_table`.`bio` ASC, `test_schema`.`test_table`.`large_blob` ASC"#,
                expected_cols: &[],
                ..Default::default()
            },
            Case {
                name: "mysql_is_null_predicate_with_where_condition",
                fixture: "mysql_tb_meta",
                order_cols: &[
                    ("id", Asc),
                    ("price", Asc),
                    ("username", Asc),
                    ("bio", Asc),
                    ("large_blob", Asc),
                ],
                predicate: IsNull,
                where_condition: r#"id > 100"#,
                limit: 100,
                expected_sql: r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE (id > 100) AND (`price` IS NULL OR `bio` IS NULL OR `large_blob` IS NULL) ORDER BY `test_schema`.`test_table`.`id` ASC, `test_schema`.`test_table`.`price` ASC, `test_schema`.`test_table`.`username` ASC, `test_schema`.`test_table`.`bio` ASC, `test_schema`.`test_table`.`large_blob` ASC LIMIT 100"#,
                expected_cols: &[],
                ..Default::default()
            },
            Case {
                name: "pg_single_order_col_after",
                fixture: "pg_tb_meta",
                order_cols: &[("id", Asc)],
                predicate: After,
                limit: 100,
                expected_sql: r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE "id" > $1::int8 ORDER BY "test_schema"."test_table"."id" ASC LIMIT 100"#,
                expected_cols: &[r#"id"#],
                ..Default::default()
            },
            Case {
                name: "pg_multiple_order_cols_after",
                fixture: "pg_tb_meta",
                order_cols: &[
                    ("id", Asc),
                    ("price", Asc),
                    ("username", Asc),
                    ("bio", Asc),
                    ("large_blob", Asc),
                ],
                predicate: After,
                limit: 100,
                expected_sql: r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE ("id", "price", "username", "bio", "large_blob") > ($1::int8, $2::float8, $3::varchar, $4::text, $5::bytea) AND "price" IS NOT NULL AND "bio" IS NOT NULL AND "large_blob" IS NOT NULL ORDER BY "test_schema"."test_table"."id" ASC, "test_schema"."test_table"."price" ASC, "test_schema"."test_table"."username" ASC, "test_schema"."test_table"."bio" ASC, "test_schema"."test_table"."large_blob" ASC LIMIT 100"#,
                expected_cols: &[
                    r#"id"#,
                    r#"price"#,
                    r#"username"#,
                    r#"bio"#,
                    r#"large_blob"#,
                ],
                ..Default::default()
            },
            Case {
                name: "pg_bit_order_col_after_uses_bit_typmod_placeholder",
                fixture: "pg_bit_order_tb_meta",
                order_cols: &[("bit_col", Asc), ("bit_array_col", Asc)],
                predicate: After,
                limit: 100,
                expected_sql: r#"SELECT "bit_col"::text,"bit_array_col"::text FROM "test_schema"."bit_order_table" WHERE ("bit_col", "bit_array_col") > ($1::bit(10), $2::bit(10)[]) AND "bit_array_col" IS NOT NULL ORDER BY "test_schema"."bit_order_table"."bit_col" ASC, "test_schema"."bit_order_table"."bit_array_col" ASC LIMIT 100"#,
                expected_cols: &[r#"bit_col"#, r#"bit_array_col"#],
                ..Default::default()
            },
            Case {
                name: "pg_single_order_col_at_or_before",
                fixture: "pg_tb_meta",
                order_cols: &[("id", Asc)],
                predicate: AtOrBefore,
                expected_sql: r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE "id" <= $1::int8 ORDER BY "test_schema"."test_table"."id" ASC"#,
                expected_cols: &[r#"id"#],
                ..Default::default()
            },
            Case {
                name: "pg_multiple_order_cols_range",
                fixture: "pg_tb_meta",
                order_cols: &[
                    ("id", Asc),
                    ("price", Asc),
                    ("username", Asc),
                    ("bio", Asc),
                    ("large_blob", Asc),
                ],
                predicate: Range,
                expected_sql: r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE ("id", "price", "username", "bio", "large_blob") > ($1::int8, $2::float8, $3::varchar, $4::text, $5::bytea) AND ("id", "price", "username", "bio", "large_blob") <= ($6::int8, $7::float8, $8::varchar, $9::text, $10::bytea) AND "price" IS NOT NULL AND "bio" IS NOT NULL AND "large_blob" IS NOT NULL ORDER BY "test_schema"."test_table"."id" ASC, "test_schema"."test_table"."price" ASC, "test_schema"."test_table"."username" ASC, "test_schema"."test_table"."bio" ASC, "test_schema"."test_table"."large_blob" ASC"#,
                expected_cols: &[
                    r#"id"#,
                    r#"price"#,
                    r#"username"#,
                    r#"bio"#,
                    r#"large_blob"#,
                    r#"id"#,
                    r#"price"#,
                    r#"username"#,
                    r#"bio"#,
                    r#"large_blob"#,
                ],
                ..Default::default()
            },
            Case {
                name: "pg_null_predicate_with_nullable_cols",
                fixture: "pg_tb_meta",
                order_cols: &[
                    ("id", Asc),
                    ("price", Asc),
                    ("username", Asc),
                    ("bio", Asc),
                    ("large_blob", Asc),
                ],
                predicate: AtOrBefore,
                expected_sql: r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE ("id", "price", "username", "bio", "large_blob") <= ($1::int8, $2::float8, $3::varchar, $4::text, $5::bytea) AND "price" IS NOT NULL AND "bio" IS NOT NULL AND "large_blob" IS NOT NULL ORDER BY "test_schema"."test_table"."id" ASC, "test_schema"."test_table"."price" ASC, "test_schema"."test_table"."username" ASC, "test_schema"."test_table"."bio" ASC, "test_schema"."test_table"."large_blob" ASC"#,
                expected_cols: &[
                    r#"id"#,
                    r#"price"#,
                    r#"username"#,
                    r#"bio"#,
                    r#"large_blob"#,
                ],
                ..Default::default()
            },
            Case {
                name: "pg_is_null_predicate",
                fixture: "pg_tb_meta",
                order_cols: &[
                    ("id", Asc),
                    ("price", Asc),
                    ("username", Asc),
                    ("bio", Asc),
                    ("large_blob", Asc),
                ],
                predicate: IsNull,
                expected_sql: r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE "price" IS NULL OR "bio" IS NULL OR "large_blob" IS NULL ORDER BY "test_schema"."test_table"."id" ASC, "test_schema"."test_table"."price" ASC, "test_schema"."test_table"."username" ASC, "test_schema"."test_table"."bio" ASC, "test_schema"."test_table"."large_blob" ASC"#,
                expected_cols: &[],
                ..Default::default()
            },
            Case {
                name: "pg_is_null_predicate_with_where_condition",
                fixture: "pg_tb_meta",
                order_cols: &[
                    ("id", Asc),
                    ("price", Asc),
                    ("username", Asc),
                    ("bio", Asc),
                    ("large_blob", Asc),
                ],
                predicate: IsNull,
                where_condition: r#"id > 100"#,
                limit: 100,
                expected_sql: r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE (id > 100) AND ("price" IS NULL OR "bio" IS NULL OR "large_blob" IS NULL) ORDER BY "test_schema"."test_table"."id" ASC, "test_schema"."test_table"."price" ASC, "test_schema"."test_table"."username" ASC, "test_schema"."test_table"."bio" ASC, "test_schema"."test_table"."large_blob" ASC LIMIT 100"#,
                expected_cols: &[],
                ..Default::default()
            },
            Case {
                name: "mysql_with_where_condition",
                fixture: "mysql_tb_meta",
                order_cols: &[("id", Asc)],
                predicate: After,
                where_condition: r#"id > 1000"#,
                expected_sql: r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE (id > 1000) AND `id` > ? ORDER BY `test_schema`.`test_table`.`id` ASC"#,
                expected_cols: &[r#"id"#],
                ..Default::default()
            },
            Case {
                name: "pg_with_where_condition",
                fixture: "pg_tb_meta",
                order_cols: &[("id", Asc)],
                predicate: After,
                where_condition: r#"id > 1000"#,
                expected_sql: r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE (id > 1000) AND "id" > $1::int8 ORDER BY "test_schema"."test_table"."id" ASC"#,
                expected_cols: &[r#"id"#],
                ..Default::default()
            },
            Case {
                name: "mysql_no_order_cols",
                fixture: "mysql_tb_meta",
                expected_sql: r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table`"#,
                expected_cols: &[],
                ..Default::default()
            },
            Case {
                name: "pg_no_order_cols",
                fixture: "pg_tb_meta",
                expected_sql: r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table""#,
                expected_cols: &[],
                ..Default::default()
            },
            Case {
                name: "no_limit",
                fixture: "mysql_tb_meta",
                order_cols: &[("id", Asc)],
                predicate: After,
                expected_sql: r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE `id` > ? ORDER BY `test_schema`.`test_table`.`id` ASC"#,
                expected_cols: &[r#"id"#],
                ..Default::default()
            },
            Case {
                name: "mysql_only_where_condition",
                fixture: "mysql_tb_meta",
                where_condition: r#"price > 100.0"#,
                expected_sql: r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE (price > 100.0)"#,
                expected_cols: &[],
                ..Default::default()
            },
            Case {
                name: "pg_only_where_condition",
                fixture: "pg_tb_meta",
                where_condition: r#"price > 100.0"#,
                expected_sql: r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE (price > 100.0)"#,
                expected_cols: &[],
                ..Default::default()
            },
            Case {
                name: "mysql_single_non_nullable_order_col",
                fixture: "mysql_tb_meta",
                order_cols: &[("username", Asc)],
                predicate: After,
                limit: 50,
                expected_sql: r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE `username` > ? ORDER BY `test_schema`.`test_table`.`username` ASC LIMIT 50"#,
                expected_cols: &[r#"username"#],
                ..Default::default()
            },
            Case {
                name: "pg_single_non_nullable_order_col",
                fixture: "pg_tb_meta",
                order_cols: &[("username", Asc)],
                predicate: After,
                limit: 50,
                expected_sql: r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE "username" > $1::varchar ORDER BY "test_schema"."test_table"."username" ASC LIMIT 50"#,
                expected_cols: &[r#"username"#],
                ..Default::default()
            },
            Case {
                name: "empty_where_condition",
                fixture: "mysql_tb_meta",
                order_cols: &[("id", Asc)],
                predicate: After,
                expected_sql: r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE `id` > ? ORDER BY `test_schema`.`test_table`.`id` ASC"#,
                expected_cols: &[r#"id"#],
                ..Default::default()
            },
            Case {
                name: "limit_zero",
                fixture: "mysql_tb_meta",
                order_cols: &[("id", Asc)],
                predicate: After,
                expected_sql: r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE `id` > ? ORDER BY `test_schema`.`test_table`.`id` ASC"#,
                expected_cols: &[r#"id"#],
                ..Default::default()
            },
            Case {
                name: "mysql_with_ignore_cols",
                fixture: "mysql_tb_meta",
                ignore_cols: &[r#"bio"#, r#"large_blob"#],
                expected_sql: r#"SELECT `id`,`price`,`username` FROM `test_schema`.`test_table`"#,
                expected_cols: &[],
                ..Default::default()
            },
            Case {
                name: "pg_with_ignore_cols",
                fixture: "pg_tb_meta",
                ignore_cols: &[r#"bio"#, r#"large_blob"#],
                expected_sql: r#"SELECT "id"::int8,"price"::float8,"username"::text FROM "test_schema"."test_table""#,
                expected_cols: &[],
                ..Default::default()
            },
            Case {
                name: "mysql_with_ignore_cols_and_order",
                fixture: "mysql_tb_meta",
                order_cols: &[("id", Asc)],
                predicate: After,
                ignore_cols: &[r#"large_blob"#],
                limit: 50,
                expected_sql: r#"SELECT `id`,`price`,`username`,`bio` FROM `test_schema`.`test_table` WHERE `id` > ? ORDER BY `test_schema`.`test_table`.`id` ASC LIMIT 50"#,
                expected_cols: &[r#"id"#],
                ..Default::default()
            },
            Case {
                name: "pg_with_ignore_cols_and_order",
                fixture: "pg_tb_meta",
                order_cols: &[("id", Asc)],
                predicate: After,
                ignore_cols: &[r#"large_blob"#],
                limit: 50,
                expected_sql: r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text FROM "test_schema"."test_table" WHERE "id" > $1::int8 ORDER BY "test_schema"."test_table"."id" ASC LIMIT 50"#,
                expected_cols: &[r#"id"#],
                ..Default::default()
            },
            Case {
                name: "mysql_predicate_type_none_with_nullable_cols",
                fixture: "mysql_tb_meta",
                order_cols: &[
                    ("id", Asc),
                    ("price", Asc),
                    ("username", Asc),
                    ("bio", Asc),
                    ("large_blob", Asc),
                ],
                limit: 100,
                expected_sql: r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE `price` IS NOT NULL AND `bio` IS NOT NULL AND `large_blob` IS NOT NULL ORDER BY `test_schema`.`test_table`.`id` ASC, `test_schema`.`test_table`.`price` ASC, `test_schema`.`test_table`.`username` ASC, `test_schema`.`test_table`.`bio` ASC, `test_schema`.`test_table`.`large_blob` ASC LIMIT 100"#,
                expected_cols: &[],
                ..Default::default()
            },
            Case {
                name: "pg_predicate_type_none_with_nullable_cols",
                fixture: "pg_tb_meta",
                order_cols: &[
                    ("id", Asc),
                    ("price", Asc),
                    ("username", Asc),
                    ("bio", Asc),
                    ("large_blob", Asc),
                ],
                limit: 100,
                expected_sql: r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE "price" IS NOT NULL AND "bio" IS NOT NULL AND "large_blob" IS NOT NULL ORDER BY "test_schema"."test_table"."id" ASC, "test_schema"."test_table"."price" ASC, "test_schema"."test_table"."username" ASC, "test_schema"."test_table"."bio" ASC, "test_schema"."test_table"."large_blob" ASC LIMIT 100"#,
                expected_cols: &[],
                ..Default::default()
            },
            Case {
                name: "mysql_predicate_type_none_single_non_nullable_col",
                fixture: "mysql_tb_meta",
                order_cols: &[("id", Asc)],
                limit: 100,
                expected_sql: r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` ORDER BY `test_schema`.`test_table`.`id` ASC LIMIT 100"#,
                expected_cols: &[],
                ..Default::default()
            },
            Case {
                name: "mysql_predicate_type_none_with_where_condition",
                fixture: "mysql_tb_meta",
                order_cols: &[("id", Asc), ("price", Asc), ("bio", Asc)],
                where_condition: r#"id > 100"#,
                limit: 100,
                expected_sql: r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE (id > 100) AND `price` IS NOT NULL AND `bio` IS NOT NULL ORDER BY `test_schema`.`test_table`.`id` ASC, `test_schema`.`test_table`.`price` ASC, `test_schema`.`test_table`.`bio` ASC LIMIT 100"#,
                expected_cols: &[],
                ..Default::default()
            },
            Case {
                name: "mssql_multiple_order_cols_after_with_top_and_where_condition",
                fixture: "mssql_tb_meta",
                order_cols: &[("id", Asc), ("tenant_id", Asc)],
                predicate: After,
                where_condition: r#"[name] <> N'ignored'"#,
                limit: 25,
                expected_sql: r#"SELECT TOP (25) [tenant_id],[id],[name] FROM [test_schema].[test_table] WHERE ([name] <> N'ignored') AND (([id] > @P1) OR ([id] = @P2 AND [tenant_id] > @P3)) AND [tenant_id] IS NOT NULL ORDER BY [test_schema].[test_table].[id] ASC, [test_schema].[test_table].[tenant_id] ASC"#,
                expected_cols: &[r#"id"#, r#"id"#, r#"tenant_id"#],
                ..Default::default()
            },
            Case {
                name: "mssql_multiple_order_cols_range_uses_distinct_parameters",
                fixture: "mssql_tb_meta",
                order_cols: &[("id", Asc), ("tenant_id", Asc)],
                predicate: Range,
                expected_sql: r#"SELECT [tenant_id],[id],[name] FROM [test_schema].[test_table] WHERE (([id] > @P1) OR ([id] = @P2 AND [tenant_id] > @P3)) AND (([id] < @P4) OR ([id] = @P5 AND [tenant_id] <= @P6)) AND [tenant_id] IS NOT NULL ORDER BY [test_schema].[test_table].[id] ASC, [test_schema].[test_table].[tenant_id] ASC"#,
                expected_cols: &[
                    r#"id"#,
                    r#"id"#,
                    r#"tenant_id"#,
                    r#"id"#,
                    r#"id"#,
                    r#"tenant_id"#,
                ],
                ..Default::default()
            },
            Case {
                name: "mssql_binary_transfer_col_uses_supported_projection",
                fixture: "mssql_binary_tb_meta",
                expected_sql: r#"SELECT [tenant_id],[id],[name],CONVERT(varbinary(max), [shape]) AS [shape] FROM [test_schema].[test_table]"#,
                expected_cols: &[],
                ..Default::default()
            },
        ];
        for case in cases {
            let mysql = match case.fixture {
                "mysql_time_order_tb_meta" => create_mysql_time_order_tb_meta(),
                "mysql_spatial_tb_meta" => create_mysql_spatial_tb_meta(),
                _ => create_mysql_tb_meta(),
            };
            let pg = if case.fixture == "pg_bit_order_tb_meta" {
                create_pg_bit_order_tb_meta()
            } else {
                create_pg_tb_meta()
            };
            let mssql = if case.fixture == "mssql_binary_tb_meta" {
                create_mssql_binary_tb_meta()
            } else {
                create_mssql_tb_meta()
            };
            let stmt = if case.fixture.starts_with("mysql") {
                RdbSnapshotExtractStatement::from(&mysql)
            } else if case.fixture.starts_with("pg") {
                RdbSnapshotExtractStatement::from(&pg)
            } else {
                RdbSnapshotExtractStatement::from(&mssql)
            };
            let order_cols = case
                .order_cols
                .iter()
                .map(|(col, _)| col.to_string())
                .collect::<Vec<_>>();
            // Deliberately insert attributes in reverse order: the Vec controls SQL order.
            let attrs = case
                .order_cols
                .iter()
                .rev()
                .map(|(col, direction)| (col.to_string(), *direction))
                .collect();
            let ignore_cols = case.ignore_cols.iter().map(|col| col.to_string()).collect();
            let where_condition = case.where_condition.to_string();
            let stmt = stmt
                .with_order_cols(&order_cols)
                .with_order_col_attrs(&attrs)
                .with_ignore_cols(&ignore_cols)
                .with_where_condition(&where_condition)
                .with_limit(case.limit)
                .with_predicate_type(case.predicate);
            let query = stmt.build().unwrap();
            assert_eq!(query.sql, case.expected_sql, "{}", case.name);
            assert_eq!(query.cols, case.expected_cols, "{}", case.name);
            assert_eq!(query, stmt.build().unwrap(), "repeat build: {}", case.name);
            if matches!(case.predicate, Range) {
                let midpoint = query.cols.len() / 2;
                assert_eq!(
                    query.cols[..midpoint],
                    query.cols[midpoint..],
                    "{}",
                    case.name
                );
            }
        }
    }
    #[test]
    fn test_directional_predicates() {
        use OrderKeyPredicateType::*;
        use SortDirection::*;
        struct PredicateCase {
            db_type: DbType,
            directions: &'static [SortDirection],
            predicate: OrderKeyPredicateType,
            expected_predicate: &'static str,
            expected_cols: &'static [&'static str],
        }
        let cases = [
            PredicateCase { db_type: DbType::Mysql, directions: &[Desc], predicate: After, expected_predicate: "`a` < ?", expected_cols: &["a"] },
            PredicateCase { db_type: DbType::Mysql, directions: &[Desc], predicate: AtOrBefore, expected_predicate: "`a` >= ?", expected_cols: &["a"] },
            PredicateCase { db_type: DbType::Mysql, directions: &[Desc, Desc], predicate: Range, expected_predicate: "(`a`, `b`) < (?, ?) AND (`a`, `b`) >= (?, ?)", expected_cols: &["a", "b", "a", "b"] },
            PredicateCase { db_type: DbType::Mysql, directions: &[Asc, Desc], predicate: After, expected_predicate: "((`a` > ?) OR (`a` = ? AND `b` < ?))", expected_cols: &["a", "a", "b"] },
            PredicateCase { db_type: DbType::Mysql, directions: &[Asc, Desc], predicate: AtOrBefore, expected_predicate: "((`a` < ?) OR (`a` = ? AND `b` >= ?))", expected_cols: &["a", "a", "b"] },
            PredicateCase { db_type: DbType::Mysql, directions: &[Desc, Asc], predicate: Range, expected_predicate: "((`a` < ?) OR (`a` = ? AND `b` > ?)) AND ((`a` > ?) OR (`a` = ? AND `b` <= ?))", expected_cols: &["a", "a", "b", "a", "a", "b"] },
            PredicateCase { db_type: DbType::Mysql, directions: &[Asc, Desc, Asc], predicate: After, expected_predicate: "((`a` > ?) OR (`a` = ? AND `b` < ?) OR (`a` = ? AND `b` = ? AND `c` > ?))", expected_cols: &["a", "a", "b", "a", "b", "c"] },
            PredicateCase { db_type: DbType::Pg, directions: &[Desc], predicate: Range, expected_predicate: r#""a" < $1::int4 AND "a" >= $2::int4"#, expected_cols: &["a", "a"] },
            PredicateCase { db_type: DbType::Pg, directions: &[Desc, Desc], predicate: Range, expected_predicate: r#"("a", "b") < ($1::int4, $2::int4) AND ("a", "b") >= ($3::int4, $4::int4)"#, expected_cols: &["a", "b", "a", "b"] },
            PredicateCase { db_type: DbType::Pg, directions: &[Asc, Desc], predicate: After, expected_predicate: r#"(("a" > $1::int4) OR ("a" = $2::int4 AND "b" < $3::int4))"#, expected_cols: &["a", "a", "b"] },
            PredicateCase { db_type: DbType::Pg, directions: &[Asc, Desc], predicate: AtOrBefore, expected_predicate: r#"(("a" < $1::int4) OR ("a" = $2::int4 AND "b" >= $3::int4))"#, expected_cols: &["a", "a", "b"] },
            PredicateCase { db_type: DbType::Pg, directions: &[Desc, Asc], predicate: Range, expected_predicate: r#"(("a" < $1::int4) OR ("a" = $2::int4 AND "b" > $3::int4)) AND (("a" > $4::int4) OR ("a" = $5::int4 AND "b" <= $6::int4))"#, expected_cols: &["a", "a", "b", "a", "a", "b"] },
            PredicateCase { db_type: DbType::Pg, directions: &[Desc, Asc, Desc], predicate: AtOrBefore, expected_predicate: r#"(("a" > $1::int4) OR ("a" = $2::int4 AND "b" < $3::int4) OR ("a" = $4::int4 AND "b" = $5::int4 AND "c" >= $6::int4))"#, expected_cols: &["a", "a", "b", "a", "b", "c"] },
            PredicateCase { db_type: DbType::Mssql, directions: &[Desc], predicate: Range, expected_predicate: "[a] < @P1 AND [a] >= @P2", expected_cols: &["a", "a"] },
            PredicateCase { db_type: DbType::Mssql, directions: &[Desc, Desc], predicate: AtOrBefore, expected_predicate: "(([a] > @P1) OR ([a] = @P2 AND [b] >= @P3))", expected_cols: &["a", "a", "b"] },
            PredicateCase { db_type: DbType::Mssql, directions: &[Asc, Desc], predicate: After, expected_predicate: "(([a] > @P1) OR ([a] = @P2 AND [b] < @P3))", expected_cols: &["a", "a", "b"] },
            PredicateCase { db_type: DbType::Mssql, directions: &[Asc, Desc], predicate: Range, expected_predicate: "(([a] > @P1) OR ([a] = @P2 AND [b] < @P3)) AND (([a] < @P4) OR ([a] = @P5 AND [b] >= @P6))", expected_cols: &["a", "a", "b", "a", "a", "b"] },
            PredicateCase { db_type: DbType::Mssql, directions: &[Desc, Asc, Desc], predicate: AtOrBefore, expected_predicate: "(([a] > @P1) OR ([a] = @P2 AND [b] < @P3) OR ([a] = @P4 AND [b] = @P5 AND [c] >= @P6))", expected_cols: &["a", "a", "b", "a", "b", "c"] },
        ];
        let cols = ["a", "b", "c"].map(str::to_string).to_vec();
        let basic = RdbTbMeta {
            db: "db".into(),
            schema: "s".into(),
            tb: "t".into(),
            cols: cols.clone(),
            ..Default::default()
        };
        let mysql = MysqlTbMeta {
            basic: basic.clone(),
            col_type_map: cols
                .iter()
                .map(|col| (col.clone(), MysqlColType::Int { unsigned: false }))
                .collect(),
        };
        let mut pg = create_pg_tb_meta();
        let mut col_type = pg.col_type_map["id"].clone();
        col_type.alias = "int4".into();
        col_type.value_type = PgValueType::Int32;
        pg.basic = basic.clone();
        pg.col_type_map = cols
            .iter()
            .map(|col| (col.clone(), col_type.clone()))
            .collect();
        let mut mssql = create_mssql_tb_meta();
        mssql.basic = basic;
        mssql.col_type_map = cols
            .iter()
            .map(|col| (col.clone(), MssqlColType::Int4))
            .collect();
        for case in cases {
            let stmt = match case.db_type {
                DbType::Mysql => RdbSnapshotExtractStatement::from(&mysql),
                DbType::Pg => RdbSnapshotExtractStatement::from(&pg),
                DbType::Mssql => RdbSnapshotExtractStatement::from(&mssql),
                _ => unreachable!(),
            };
            let order_cols = &cols[..case.directions.len()];
            let attrs = order_cols
                .iter()
                .zip(case.directions)
                .rev()
                .map(|(col, direction)| (col.clone(), *direction))
                .collect();
            let where_condition = "a = 1 OR a = 2".to_string();
            let ignore_cols = HashSet::from(["a".to_string()]);
            let stmt = stmt
                .with_order_cols(order_cols)
                .with_order_col_attrs(&attrs)
                .with_where_condition(&where_condition)
                .with_ignore_cols(&ignore_cols)
                .with_predicate_type(case.predicate)
                .with_limit(2);
            let query = stmt.build().unwrap();
            let (select, rest) = query.sql.split_once(" WHERE ").unwrap();
            // Ignoring a cursor column only affects output RowData, not this SELECT.
            assert!(select.contains(&stmt.escape("a")));
            if matches!(case.db_type, DbType::Mssql) {
                assert!(select.ends_with("FROM [db].[s].[t]"));
                assert!(select.starts_with("SELECT TOP (2)"));
            }
            let (predicate, order_by) = rest.split_once(" ORDER BY ").unwrap();
            assert_eq!(
                predicate,
                format!("({where_condition}) AND {}", case.expected_predicate),
                "{:?} {:?}",
                case.db_type,
                case.directions
            );
            for (col, direction) in order_cols.iter().zip(case.directions) {
                assert!(order_by.contains(&format!(
                    "{}.{} {direction}",
                    stmt.table_name(),
                    stmt.escape(col)
                )));
            }
            assert_eq!(query.cols, case.expected_cols);
            assert_eq!(query, stmt.build().unwrap());
        }
    }

    #[test]
    fn test_missing_order_attributes() {
        let meta = create_mysql_tb_meta();
        let cols = vec!["id".to_string(), "username".to_string()];
        for attrs in [
            HashMap::new(),
            HashMap::from([("id".to_string(), SortDirection::Asc)]),
        ] {
            assert!(RdbSnapshotExtractStatement::from(&meta)
                .with_order_cols(&cols)
                .with_order_col_attrs(&attrs)
                .build()
                .is_err());
        }
        assert!(RdbSnapshotExtractStatement::from(&meta)
            .with_order_cols(&cols)
            .build()
            .is_err());
    }
}
