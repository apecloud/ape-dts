#![cfg(any)]
use anyhow::bail;
use dt_common::{
    config::config_enums::DbType,
    meta::{mysql::mysql_tb_meta::MysqlTbMeta, pg::pg_tb_meta::PgTbMeta, rdb_tb_meta::RdbTbMeta},
    utils::sql_util::SqlUtil,
};

pub struct RdbSnapshotExtractStatement<'a> {
    db_type: DbType,
    rdb_tb_meta: &'a RdbTbMeta,
    pg_tb_meta: Option<&'a PgTbMeta>,
    mysql_tb_meta: Option<&'a MysqlTbMeta>,

    order_cols: Vec<String>,
    where_condition: String,
    limit: usize,
    predicate_type: PredicateType,
}

pub enum PredicateType {
    None,
    GreaterThan,
    LessThanOrEqual,
    Range,
    IsNull,
}

impl RdbSnapshotExtractStatement<'_> {
    pub fn new_for_mysql<'a>(
        db_type: DbType,
        mysql_tb_meta: &'a MysqlTbMeta,
    ) -> RdbSnapshotExtractStatement<'a> {
        RdbSnapshotExtractStatement {
            db_type,
            rdb_tb_meta: &mysql_tb_meta.basic,
            mysql_tb_meta: Some(mysql_tb_meta),
            pg_tb_meta: None,
            order_cols: vec![],
            where_condition: String::new(),
            limit: 0,
            predicate_type: PredicateType::None,
        }
    }

    pub fn new_for_pg<'a>(
        db_type: DbType,
        pg_tb_meta: &'a PgTbMeta,
    ) -> RdbSnapshotExtractStatement<'a> {
        RdbSnapshotExtractStatement {
            db_type,
            rdb_tb_meta: &pg_tb_meta.basic,
            mysql_tb_meta: None,
            pg_tb_meta: Some(pg_tb_meta),
            order_cols: vec![],
            where_condition: String::new(),
            limit: 0,
            predicate_type: PredicateType::None,
        }
    }

    pub fn with_order_cols(&mut self, order_cols: Vec<String>) -> &mut RdbSnapshotExtractStatement {
        self.order_cols = order_cols;
        self
    }

    pub fn with_where_condition(
        &mut self,
        where_condition: String,
    ) -> &mut RdbSnapshotExtractStatement {
        self.where_condition = where_condition;
        self
    }

    pub fn with_limit(&mut self, limit: usize) -> &mut RdbSnapshotExtractStatement {
        self.limit = limit;
        self
    }

    pub fn with_predicate_type(
        &mut self,
        predicate_type: PredicateType,
    ) -> &mut RdbSnapshotExtractStatement {
        self.predicate_type = predicate_type;
        self
    }

    pub fn build(&mut self, tb_meta: &RdbTbMeta) -> anyhow::Result<String> {
        todo!()
    }

    #[inline(always)]
    fn build_order_col_str(&mut self, order_cols: &[String]) -> String {
        order_cols
            .iter()
            .map(|col| format!("{}", self.quote(col)))
            .collect::<Vec<String>>()
            .join(", ")
    }

    #[inline(always)]
    fn build_place_holder_str(&mut self, order_cols: &[String]) -> String {
        order_cols
            .iter()
            .map(|_| "?".to_string())
            .collect::<Vec<String>>()
            .join(", ")
    }

    fn build_order_col_predicate_range(order_cols: &[String]) -> anyhow::Result<String> {
        if order_cols.is_empty() {
            bail!("order cols is empty");
        } else if order_cols.len() == 1 {
            // col_1 > ? AND col_1 <= ?
            Ok(format!(
                r#"`{}` > ? AND `{}` <= ?"#,
                &order_cols[0], &order_cols[0],
            ))
        } else {
            // (col_1, col_2, col_3) > (?, ?, ?) AND (col_1, col_2, col_3) <= (?, ?, ?)
            let order_col_str = Self::build_order_col_str(order_cols);
            let place_holder_str = Self::build_place_holder_str(order_cols);
            Ok(format!(
                r#"({}) > ({}) AND ({}) <= ({})"#,
                &order_col_str, &place_holder_str, &order_col_str, &place_holder_str,
            ))
        }
    }

    fn build_order_col_predicate_gt(order_cols: &[String]) -> anyhow::Result<String> {
        if order_cols.is_empty() {
            bail!("order cols is empty");
        } else if order_cols.len() == 1 {
            // col_1 > ?
            Ok(format!(r#"`{}` > ?"#, &order_cols[0],))
        } else {
            // (col_1, col_2, col_3) > (?, ?, ?)
            let order_col_str = Self::build_order_col_str(order_cols);
            let place_holder_str = Self::build_place_holder_str(order_cols);
            Ok(format!(r#"({}) > ({})"#, order_col_str, place_holder_str))
        }
    }

    fn build_order_col_predicate_le(order_cols: &[String]) -> anyhow::Result<String> {
        if order_cols.is_empty() {
            bail!("order cols is empty");
        } else if order_cols.len() == 1 {
            // col_1 <= ?
            Ok(format!(r#"`{}` <= ?"#, &order_cols[0],))
        } else {
            // (col_1, col_2, col_3) <= (?, ?, ?)
            let order_col_str = Self::build_order_col_str(order_cols);
            let place_holder_str = Self::build_place_holder_str(order_cols);
            Ok(format!(r#"({}) <= ({})"#, order_col_str, place_holder_str))
        }
    }

    fn build_order_by_clause(order_cols: &[String]) -> anyhow::Result<String> {
        if order_cols.is_empty() {
            bail!("order cols is empty");
        } else if order_cols.len() == 1 {
            Ok(format!(r#"`{}` ASC"#, &order_cols[0]))
        } else {
            // col_1 ASC, col_2 ASC, col_3 ASC
            // (col_1, col_2, col_3) ASC does not trigger index scan sometimes
            Ok(order_cols
                .iter()
                .map(|col| format!(r#"`{}` ASC"#, col))
                .collect::<Vec<String>>()
                .join(", "))
        }
    }

    fn quote(&self, token: &str) -> String {
        SqlUtil::escape_by_db_type(token, &self.db_type)
    }
}
