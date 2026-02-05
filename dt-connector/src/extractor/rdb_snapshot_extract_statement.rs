use std::cell::Cell;
use std::collections::HashSet;

use anyhow::bail;
use dt_common::{
    config::config_enums::DbType,
    meta::{
        adaptor::pg_col_value_convertor::PgColValueConvertor, mysql::mysql_tb_meta::MysqlTbMeta,
        pg::pg_tb_meta::PgTbMeta, rdb_tb_meta::RdbTbMeta,
    },
    utils::sql_util::SqlUtil,
};

pub enum OrderKeyPredicateType {
    None,
    GreaterThan,
    LessThanOrEqual,
    Range,
    IsNull,
}

pub struct RdbSnapshotExtractStatement<'a> {
    db_type: DbType,
    rdb_tb_meta: &'a RdbTbMeta,
    pg_tb_meta: Option<&'a PgTbMeta>,
    mysql_tb_meta: Option<&'a MysqlTbMeta>,
    order_cols: Option<&'a Vec<String>>,
    ignore_cols: Option<&'a HashSet<String>>,
    where_condition: Option<&'a String>,
    limit: usize,
    predicate_type: OrderKeyPredicateType,
    placeholder_index: Cell<usize>,
}

impl<'r> From<&'r MysqlTbMeta> for RdbSnapshotExtractStatement<'r> {
    fn from(mysql_tb_meta: &'r MysqlTbMeta) -> Self {
        RdbSnapshotExtractStatement {
            db_type: DbType::Mysql,
            rdb_tb_meta: &mysql_tb_meta.basic,
            mysql_tb_meta: Some(mysql_tb_meta),
            pg_tb_meta: None,
            order_cols: None,
            ignore_cols: None,
            where_condition: None,
            limit: 0,
            predicate_type: OrderKeyPredicateType::None,
            placeholder_index: Cell::new(0),
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
            order_cols: None,
            ignore_cols: None,
            where_condition: None,
            limit: 0,
            predicate_type: OrderKeyPredicateType::None,
            placeholder_index: Cell::new(0),
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
    pub fn with_order_cols(mut self, order_cols: &'r Vec<String>) -> Self {
        self.order_cols = Some(order_cols);
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

    pub fn build(&self) -> anyhow::Result<String> {
        let extract_cols_str = self.build_extract_cols_str()?;
        let mut sql = format!(
            "SELECT {} FROM {}.{}",
            extract_cols_str,
            self.escape(&self.rdb_tb_meta.schema),
            self.escape(&self.rdb_tb_meta.tb)
        );
        let mut predicates: Vec<String> = Vec::new();
        match self.where_condition {
            Some(where_condition) if !where_condition.is_empty() => {
                predicates.push(where_condition.clone());
            }
            _ => (),
        }
        if let Some(order_cols) = self.order_cols.filter(|cols| !cols.is_empty()) {
            let predicate = match self.predicate_type {
                OrderKeyPredicateType::GreaterThan => {
                    self.build_order_col_predicate_gt(order_cols)?
                }
                OrderKeyPredicateType::LessThanOrEqual => {
                    self.build_order_col_predicate_le(order_cols)?
                }
                OrderKeyPredicateType::Range => self.build_order_col_predicate_range(order_cols)?,
                _ => String::new(),
            };
            if !predicate.is_empty() {
                predicates.push(predicate);
            }
            match self.predicate_type {
                OrderKeyPredicateType::GreaterThan
                | OrderKeyPredicateType::LessThanOrEqual
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

        predicates = predicates
            .into_iter()
            .filter(|p| !p.is_empty())
            .collect::<Vec<_>>();
        if !predicates.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&predicates.join(" AND "));
        }
        if let Some(order_cols) = self.order_cols.filter(|cols| !cols.is_empty()) {
            if !order_cols.is_empty() {
                sql.push_str(" ORDER BY ");
                sql.push_str(&self.build_order_by_clause(order_cols)?);
            }
        }
        if self.limit > 0 {
            sql.push_str(&format!(" LIMIT {}", self.limit));
        }
        Ok(sql)
    }

    fn build_extract_cols_str(&self) -> anyhow::Result<String> {
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

    #[inline(always)]
    fn build_order_col_str(&self, order_cols: &[String]) -> String {
        order_cols
            .iter()
            .map(|col| self.escape(col).to_string())
            .collect::<Vec<String>>()
            .join(", ")
    }

    fn build_place_holder_str(&self, order_cols: &[String]) -> anyhow::Result<String> {
        if let Some(pg_tb_meta) = self.pg_tb_meta {
            // PostgreSQL: $1::type, $2::type, ...
            Ok(order_cols
                .iter()
                .map(|col| {
                    let col_type = pg_tb_meta.get_col_type(col).unwrap();
                    let idx = self.placeholder_index.get() + 1;
                    self.placeholder_index.set(idx);
                    format!(r#"${}::{}"#, idx, col_type.alias)
                })
                .collect::<Vec<String>>()
                .join(", "))
        } else if self.mysql_tb_meta.is_some() {
            // MySQL: ?, ?, ...
            Ok(order_cols
                .iter()
                .map(|_| "?".to_string())
                .collect::<Vec<String>>()
                .join(", "))
        } else {
            bail!(
                "unsupported db type: {:?} for building placeholder string",
                self.db_type
            )
        }
    }

    fn build_order_col_predicate_range(&self, order_cols: &[String]) -> anyhow::Result<String> {
        let len = order_cols.len();
        if len == 0 {
            return Ok(String::new());
        }
        // (col_1, col_2, col_3) > (?, ?, ?) AND (col_1, col_2, col_3) <= (?, ?, ?)
        let order_col_str = self.build_order_col_str(order_cols);
        let place_holder_str1 = self.build_place_holder_str(order_cols)?;
        let place_holder_str2 = self.build_place_holder_str(order_cols)?;
        if len == 1 {
            return Ok(format!(
                r#"{} > {} AND {} <= {}"#,
                &order_col_str, &place_holder_str1, &order_col_str, &place_holder_str2
            ));
        }
        Ok(format!(
            r#"({}) > ({}) AND ({}) <= ({})"#,
            &order_col_str, &place_holder_str1, &order_col_str, &place_holder_str2,
        ))
    }
    fn build_order_col_predicate_gt(&self, order_cols: &[String]) -> anyhow::Result<String> {
        let len = order_cols.len();
        if len == 0 {
            return Ok(String::new());
        }
        // (col_1, col_2, col_3) > (?, ?, ?)
        let order_col_str = self.build_order_col_str(order_cols);
        let place_holder_str = self.build_place_holder_str(order_cols)?;
        if len == 1 {
            return Ok(format!(r#"{} > {}"#, &order_col_str, &place_holder_str));
        }
        Ok(format!(r#"({}) > ({})"#, &order_col_str, &place_holder_str))
    }

    fn build_order_col_predicate_le(&self, order_cols: &[String]) -> anyhow::Result<String> {
        let len = order_cols.len();
        if len == 0 {
            return Ok(String::new());
        }
        // (col_1, col_2, col_3) <= (?, ?, ?)
        let order_col_str = self.build_order_col_str(order_cols);
        let place_holder_str = self.build_place_holder_str(order_cols)?;
        if len == 1 {
            return Ok(format!(r#"{} <= {}"#, &order_col_str, &place_holder_str));
        }
        Ok(format!(
            r#"({}) <= ({})"#,
            &order_col_str, &place_holder_str
        ))
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
        match order_cols.len() {
            0 => Ok(String::new()),
            1 => Ok(format!(
                "{}.{}.{} ASC",
                self.escape(&self.rdb_tb_meta.schema),
                self.escape(&self.rdb_tb_meta.tb),
                self.escape(&order_cols[0])
            )),
            _ => {
                // col_1 ASC, col_2 ASC, col_3 ASC
                // (col_1, col_2, col_3) ASC does not trigger index scan sometimes
                Ok(order_cols
                    .iter()
                    .map(|col| {
                        format!(
                            "{}.{}.{} ASC",
                            self.escape(&self.rdb_tb_meta.schema),
                            self.escape(&self.rdb_tb_meta.tb),
                            self.escape(col)
                        )
                    })
                    .collect::<Vec<String>>()
                    .join(", "))
            }
        }
    }

    #[inline(always)]
    fn escape(&self, token: &str) -> String {
        SqlUtil::escape_by_db_type(token, &self.db_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dt_common::meta::{
        mysql::mysql_col_type::MysqlColType, mysql::mysql_tb_meta::MysqlTbMeta,
        pg::pg_col_type::PgColType, pg::pg_tb_meta::PgTbMeta, pg::pg_value_type::PgValueType,
        rdb_tb_meta::RdbTbMeta,
    };
    use std::collections::HashMap;

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
            },
        );

        PgTbMeta {
            basic,
            oid: 16384,
            col_type_map,
        }
    }

    #[test]
    fn test_mysql_single_order_col_gt() {
        let mysql_meta = create_mysql_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&mysql_meta);
        let order_cols = vec!["id".to_string()];
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_predicate_type(OrderKeyPredicateType::GreaterThan)
            .with_limit(100);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE `id` > ? ORDER BY `id` ASC LIMIT 100"#
        );
    }

    #[test]
    fn test_mysql_multiple_order_cols_gt() {
        let mysql_meta = create_mysql_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&mysql_meta);
        let order_cols = vec![
            "id".to_string(),
            "price".to_string(),
            "username".to_string(),
            "bio".to_string(),
            "large_blob".to_string(),
        ];
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_predicate_type(OrderKeyPredicateType::GreaterThan)
            .with_limit(100);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE (`id`, `price`, `username`, `bio`, `large_blob`) > (?, ?, ?, ?, ?) AND `price` IS NOT NULL AND `bio` IS NOT NULL AND `large_blob` IS NOT NULL ORDER BY `id` ASC, `price` ASC, `username` ASC, `bio` ASC, `large_blob` ASC LIMIT 100"#
        );
    }

    #[test]
    fn test_mysql_single_order_col_range() {
        let mysql_meta = create_mysql_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&mysql_meta);
        let order_cols = vec!["id".to_string()];
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_predicate_type(OrderKeyPredicateType::Range);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE `id` > ? AND `id` <= ? ORDER BY `id` ASC"#
        );
    }

    #[test]
    fn test_mysql_multiple_order_cols_range() {
        let mysql_meta = create_mysql_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&mysql_meta);
        let order_cols = vec![
            "id".to_string(),
            "price".to_string(),
            "username".to_string(),
            "bio".to_string(),
            "large_blob".to_string(),
        ];
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_predicate_type(OrderKeyPredicateType::Range);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE (`id`, `price`, `username`, `bio`, `large_blob`) > (?, ?, ?, ?, ?) AND (`id`, `price`, `username`, `bio`, `large_blob`) <= (?, ?, ?, ?, ?) AND `price` IS NOT NULL AND `bio` IS NOT NULL AND `large_blob` IS NOT NULL ORDER BY `id` ASC, `price` ASC, `username` ASC, `bio` ASC, `large_blob` ASC"#
        );
    }

    #[test]
    fn test_mysql_null_predicate_with_nullable_cols() {
        let mysql_meta = create_mysql_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&mysql_meta);
        let order_cols = vec![
            "id".to_string(),
            "price".to_string(),
            "username".to_string(),
            "bio".to_string(),
            "large_blob".to_string(),
        ];
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_predicate_type(OrderKeyPredicateType::LessThanOrEqual);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE (`id`, `price`, `username`, `bio`, `large_blob`) <= (?, ?, ?, ?, ?) AND `price` IS NOT NULL AND `bio` IS NOT NULL AND `large_blob` IS NOT NULL ORDER BY `id` ASC, `price` ASC, `username` ASC, `bio` ASC, `large_blob` ASC"#
        );
    }

    #[test]
    fn test_mysql_is_null_predicate() {
        let mysql_meta = create_mysql_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&mysql_meta);
        let order_cols = vec![
            "id".to_string(),
            "price".to_string(),
            "username".to_string(),
            "bio".to_string(),
            "large_blob".to_string(),
        ];
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_predicate_type(OrderKeyPredicateType::IsNull);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE `price` IS NULL OR `bio` IS NULL OR `large_blob` IS NULL ORDER BY `id` ASC, `price` ASC, `username` ASC, `bio` ASC, `large_blob` ASC"#
        );
    }

    #[test]
    fn test_mysql_is_null_predicate_with_where_condition() {
        let mysql_meta = create_mysql_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&mysql_meta);
        let order_cols = vec![
            "id".to_string(),
            "price".to_string(),
            "username".to_string(),
            "bio".to_string(),
            "large_blob".to_string(),
        ];
        let where_condition = "id > 100".to_string();
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::IsNull)
            .with_limit(100);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE id > 100 AND (`price` IS NULL OR `bio` IS NULL OR `large_blob` IS NULL) ORDER BY `id` ASC, `price` ASC, `username` ASC, `bio` ASC, `large_blob` ASC LIMIT 100"#
        );
    }

    #[test]
    fn test_pg_single_order_col_gt() {
        let pg_meta = create_pg_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&pg_meta);
        let order_cols = vec!["id".to_string()];
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_predicate_type(OrderKeyPredicateType::GreaterThan)
            .with_limit(100);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE "id" > $1::int8 ORDER BY "id" ASC LIMIT 100"#
        );
    }

    #[test]
    fn test_pg_multiple_order_cols_gt() {
        let pg_meta = create_pg_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&pg_meta);
        let order_cols = vec![
            "id".to_string(),
            "price".to_string(),
            "username".to_string(),
            "bio".to_string(),
            "large_blob".to_string(),
        ];
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_predicate_type(OrderKeyPredicateType::GreaterThan)
            .with_limit(100);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE ("id", "price", "username", "bio", "large_blob") > ($1::int8, $2::float8, $3::varchar, $4::text, $5::bytea) AND "price" IS NOT NULL AND "bio" IS NOT NULL AND "large_blob" IS NOT NULL ORDER BY "id" ASC, "price" ASC, "username" ASC, "bio" ASC, "large_blob" ASC LIMIT 100"#
        );
    }

    #[test]
    fn test_pg_single_order_col_le() {
        let pg_meta = create_pg_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&pg_meta);
        let order_cols = vec!["id".to_string()];
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_predicate_type(OrderKeyPredicateType::LessThanOrEqual);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE "id" <= $1::int8 ORDER BY "id" ASC"#
        );
    }

    #[test]
    fn test_pg_multiple_order_cols_range() {
        let pg_meta = create_pg_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&pg_meta);
        let order_cols = vec![
            "id".to_string(),
            "price".to_string(),
            "username".to_string(),
            "bio".to_string(),
            "large_blob".to_string(),
        ];
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_predicate_type(OrderKeyPredicateType::Range);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE ("id", "price", "username", "bio", "large_blob") > ($1::int8, $2::float8, $3::varchar, $4::text, $5::bytea) AND ("id", "price", "username", "bio", "large_blob") <= ($6::int8, $7::float8, $8::varchar, $9::text, $10::bytea) AND "price" IS NOT NULL AND "bio" IS NOT NULL AND "large_blob" IS NOT NULL ORDER BY "id" ASC, "price" ASC, "username" ASC, "bio" ASC, "large_blob" ASC"#
        );
    }

    #[test]
    fn test_pg_null_predicate_with_nullable_cols() {
        let pg_meta = create_pg_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&pg_meta);
        let order_cols = vec![
            "id".to_string(),
            "price".to_string(),
            "username".to_string(),
            "bio".to_string(),
            "large_blob".to_string(),
        ];
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_predicate_type(OrderKeyPredicateType::LessThanOrEqual);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE ("id", "price", "username", "bio", "large_blob") <= ($1::int8, $2::float8, $3::varchar, $4::text, $5::bytea) AND "price" IS NOT NULL AND "bio" IS NOT NULL AND "large_blob" IS NOT NULL ORDER BY "id" ASC, "price" ASC, "username" ASC, "bio" ASC, "large_blob" ASC"#
        );
    }

    #[test]
    fn test_pg_is_null_predicate() {
        let pg_meta = create_pg_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&pg_meta);
        let order_cols = vec![
            "id".to_string(),
            "price".to_string(),
            "username".to_string(),
            "bio".to_string(),
            "large_blob".to_string(),
        ];
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_predicate_type(OrderKeyPredicateType::IsNull);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE "price" IS NULL OR "bio" IS NULL OR "large_blob" IS NULL ORDER BY "id" ASC, "price" ASC, "username" ASC, "bio" ASC, "large_blob" ASC"#
        );
    }

    #[test]
    fn test_pg_is_null_predicate_with_where_condition() {
        let pg_meta = create_pg_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&pg_meta);
        let order_cols = vec![
            "id".to_string(),
            "price".to_string(),
            "username".to_string(),
            "bio".to_string(),
            "large_blob".to_string(),
        ];
        let where_condition = "id > 100".to_string();
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::IsNull)
            .with_limit(100);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE id > 100 AND ("price" IS NULL OR "bio" IS NULL OR "large_blob" IS NULL) ORDER BY "id" ASC, "price" ASC, "username" ASC, "bio" ASC, "large_blob" ASC LIMIT 100"#
        );
    }

    #[test]
    fn test_mysql_with_where_condition() {
        let mysql_meta = create_mysql_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&mysql_meta);
        let order_cols = vec!["id".to_string()];
        let where_condition = "id > 1000".to_string();
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::GreaterThan);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE id > 1000 AND `id` > ? ORDER BY `id` ASC"#
        );
    }

    #[test]
    fn test_pg_with_where_condition() {
        let pg_meta = create_pg_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&pg_meta);
        let order_cols = vec!["id".to_string()];
        let where_condition = "id > 1000".to_string();
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::GreaterThan);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE id > 1000 AND "id" > $1::int8 ORDER BY "id" ASC"#
        );
    }

    // Boundary tests
    #[test]
    fn test_mysql_no_order_cols() {
        let mysql_meta = create_mysql_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&mysql_meta);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table`"#
        );
    }

    #[test]
    fn test_pg_no_order_cols() {
        let pg_meta = create_pg_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&pg_meta);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table""#
        );
    }

    #[test]
    fn test_no_limit() {
        let mysql_meta = create_mysql_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&mysql_meta);
        let order_cols = vec!["id".to_string()];
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_predicate_type(OrderKeyPredicateType::GreaterThan);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE `id` > ? ORDER BY `id` ASC"#
        );
    }

    #[test]
    fn test_mysql_only_where_condition() {
        let mysql_meta = create_mysql_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&mysql_meta);
        let where_condition = "price > 100.0".to_string();
        let stmt = stmt.with_where_condition(&where_condition);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE price > 100.0"#
        );
    }

    #[test]
    fn test_pg_only_where_condition() {
        let pg_meta = create_pg_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&pg_meta);
        let where_condition = "price > 100.0".to_string();
        let stmt = stmt.with_where_condition(&where_condition);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE price > 100.0"#
        );
    }

    #[test]
    fn test_mysql_single_non_nullable_order_col() {
        let mysql_meta = create_mysql_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&mysql_meta);
        let order_cols = vec!["username".to_string()];
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_predicate_type(OrderKeyPredicateType::GreaterThan)
            .with_limit(50);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE `username` > ? ORDER BY `username` ASC LIMIT 50"#
        );
    }

    #[test]
    fn test_pg_single_non_nullable_order_col() {
        let pg_meta = create_pg_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&pg_meta);
        let order_cols = vec!["username".to_string()];
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_predicate_type(OrderKeyPredicateType::GreaterThan)
            .with_limit(50);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE "username" > $1::varchar ORDER BY "username" ASC LIMIT 50"#
        );
    }

    #[test]
    fn test_empty_where_condition() {
        let mysql_meta = create_mysql_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&mysql_meta);
        let order_cols = vec!["id".to_string()];
        let where_condition = "".to_string();
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::GreaterThan);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE `id` > ? ORDER BY `id` ASC"#
        );
    }

    #[test]
    fn test_limit_zero() {
        let mysql_meta = create_mysql_tb_meta();
        let stmt: RdbSnapshotExtractStatement = (&mysql_meta).into();
        let order_cols = vec!["id".to_string()];
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_predicate_type(OrderKeyPredicateType::GreaterThan)
            .with_limit(0);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE `id` > ? ORDER BY `id` ASC"#
        );
    }

    #[test]
    fn test_mysql_with_ignore_cols() {
        let mysql_meta = create_mysql_tb_meta();
        let stmt: RdbSnapshotExtractStatement = (&mysql_meta).into();
        let mut ignore_cols = HashSet::new();
        ignore_cols.insert("bio".to_string());
        ignore_cols.insert("large_blob".to_string());
        let stmt = stmt.with_ignore_cols(&ignore_cols);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT `id`,`price`,`username` FROM `test_schema`.`test_table`"#
        );
    }

    #[test]
    fn test_pg_with_ignore_cols() {
        let pg_meta = create_pg_tb_meta();
        let stmt: RdbSnapshotExtractStatement = (&pg_meta).into();
        let mut ignore_cols = HashSet::new();
        ignore_cols.insert("bio".to_string());
        ignore_cols.insert("large_blob".to_string());
        let stmt = stmt.with_ignore_cols(&ignore_cols);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT "id"::int8,"price"::float8,"username"::text FROM "test_schema"."test_table""#
        );
    }

    #[test]
    fn test_mysql_with_ignore_cols_and_order() {
        let mysql_meta = create_mysql_tb_meta();
        let stmt: RdbSnapshotExtractStatement = (&mysql_meta).into();
        let mut ignore_cols = HashSet::new();
        ignore_cols.insert("large_blob".to_string());
        let order_cols = vec!["id".to_string()];
        let stmt = stmt
            .with_ignore_cols(&ignore_cols)
            .with_order_cols(&order_cols)
            .with_predicate_type(OrderKeyPredicateType::GreaterThan)
            .with_limit(50);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT `id`,`price`,`username`,`bio` FROM `test_schema`.`test_table` WHERE `id` > ? ORDER BY `id` ASC LIMIT 50"#
        );
    }

    #[test]
    fn test_pg_with_ignore_cols_and_order() {
        let pg_meta = create_pg_tb_meta();
        let stmt: RdbSnapshotExtractStatement = (&pg_meta).into();
        let mut ignore_cols = HashSet::new();
        ignore_cols.insert("large_blob".to_string());
        let order_cols = vec!["id".to_string()];
        let stmt = stmt
            .with_ignore_cols(&ignore_cols)
            .with_order_cols(&order_cols)
            .with_predicate_type(OrderKeyPredicateType::GreaterThan)
            .with_limit(50);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text FROM "test_schema"."test_table" WHERE "id" > $1::int8 ORDER BY "id" ASC LIMIT 50"#
        );
    }

    #[test]
    fn test_mysql_predicate_type_none_with_nullable_cols() {
        let mysql_meta = create_mysql_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&mysql_meta);
        let order_cols = vec![
            "id".to_string(),
            "price".to_string(),
            "username".to_string(),
            "bio".to_string(),
            "large_blob".to_string(),
        ];
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_predicate_type(OrderKeyPredicateType::None)
            .with_limit(100);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE `price` IS NOT NULL AND `bio` IS NOT NULL AND `large_blob` IS NOT NULL ORDER BY `id` ASC, `price` ASC, `username` ASC, `bio` ASC, `large_blob` ASC LIMIT 100"#
        );
    }

    #[test]
    fn test_pg_predicate_type_none_with_nullable_cols() {
        let pg_meta = create_pg_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&pg_meta);
        let order_cols = vec![
            "id".to_string(),
            "price".to_string(),
            "username".to_string(),
            "bio".to_string(),
            "large_blob".to_string(),
        ];
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_predicate_type(OrderKeyPredicateType::None)
            .with_limit(100);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT "id"::int8,"price"::float8,"username"::text,"bio"::text,"large_blob"::bytea FROM "test_schema"."test_table" WHERE "price" IS NOT NULL AND "bio" IS NOT NULL AND "large_blob" IS NOT NULL ORDER BY "id" ASC, "price" ASC, "username" ASC, "bio" ASC, "large_blob" ASC LIMIT 100"#
        );
    }

    #[test]
    fn test_mysql_predicate_type_none_single_non_nullable_col() {
        let mysql_meta = create_mysql_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&mysql_meta);
        let order_cols = vec!["id".to_string()];
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_predicate_type(OrderKeyPredicateType::None)
            .with_limit(100);

        let sql = stmt.build().unwrap();
        // id is not nullable, so no IS NOT NULL predicate
        assert_eq!(
            sql,
            r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` ORDER BY `id` ASC LIMIT 100"#
        );
    }

    #[test]
    fn test_mysql_predicate_type_none_with_where_condition() {
        let mysql_meta = create_mysql_tb_meta();
        let stmt = RdbSnapshotExtractStatement::from(&mysql_meta);
        let order_cols = vec!["id".to_string(), "price".to_string(), "bio".to_string()];
        let where_condition = "id > 100".to_string();
        let stmt = stmt
            .with_order_cols(&order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::None)
            .with_limit(100);

        let sql = stmt.build().unwrap();
        assert_eq!(
            sql,
            r#"SELECT `id`,`price`,`username`,`bio`,`large_blob` FROM `test_schema`.`test_table` WHERE id > 100 AND `price` IS NOT NULL AND `bio` IS NOT NULL ORDER BY `id` ASC, `price` ASC, `bio` ASC LIMIT 100"#
        );
    }
}
