use anyhow::{Context, Ok};
use mongodb::bson::Document;
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};

use crate::{
    check_log::check_log::{CheckLog, DiffColValue},
    rdb_query_builder::RdbQueryBuilder,
    rdb_router::RdbRouter,
    sinker::mongo::mongo_cmd,
};
use dt_common::meta::{
    col_value::ColValue, mongo::mongo_constant::MongoConstants, mysql::mysql_tb_meta::MysqlTbMeta,
    pg::pg_tb_meta::PgTbMeta, rdb_meta_manager::RdbMetaManager, rdb_tb_meta::RdbTbMeta,
    row_data::RowData, row_type::RowType,
    struct_meta::statement::struct_statement::StructStatement,
};
use dt_common::{log_diff, log_extra, log_miss, log_sql, rdb_filter::RdbFilter};

pub enum ReviseSqlMeta<'a> {
    Mysql(&'a MysqlTbMeta),
    Pg(&'a PgTbMeta),
    Mongo,
}

pub struct ReviseSqlContext<'a> {
    pub meta: ReviseSqlMeta<'a>,
    pub match_full_row: bool,
}

impl<'a> ReviseSqlContext<'a> {
    pub fn mysql(meta: &'a MysqlTbMeta, match_full_row: bool) -> Self {
        Self {
            meta: ReviseSqlMeta::Mysql(meta),
            match_full_row,
        }
    }

    pub fn pg(meta: &'a PgTbMeta, match_full_row: bool) -> Self {
        Self {
            meta: ReviseSqlMeta::Pg(meta),
            match_full_row,
        }
    }

    pub fn mongo() -> Self {
        Self {
            meta: ReviseSqlMeta::Mongo,
            match_full_row: false,
        }
    }

    pub fn build_miss_sql(&self, src_row_data: &RowData) -> anyhow::Result<Option<String>> {
        let after = match &src_row_data.after {
            Some(after) if !after.is_empty() => after.clone(),
            _ => return Ok(None),
        };

        // 1. mongo
        if let ReviseSqlMeta::Mongo = self.meta {
            return Ok(mongo_cmd::build_insert_cmd(src_row_data));
        }
        // 2. rdb

        let mut insert_row = RowData::new(
            src_row_data.schema.clone(),
            src_row_data.tb.clone(),
            RowType::Insert,
            None,
            Some(after),
        );
        insert_row.refresh_data_size();

        self.build_insert_query(&insert_row)
    }

    pub fn build_diff_sql(
        &self,
        src_row_data: &RowData,
        dst_row_data: &RowData,
        diff_col_values: &HashMap<String, DiffColValue>,
    ) -> anyhow::Result<Option<String>> {
        if diff_col_values.is_empty() {
            return Ok(None);
        }
        // 1. mongo
        if let ReviseSqlMeta::Mongo = self.meta {
            return Ok(mongo_cmd::build_update_cmd(src_row_data, diff_col_values));
        }
        // 2. rdb
        let Some(src_after) = src_row_data.require_after().ok() else {
            return Ok(None);
        };
        let update_after: HashMap<_, _> = diff_col_values
            .keys()
            .filter_map(|col| src_after.get(col).map(|v| (col.clone(), v.clone())))
            .collect();
        if update_after.is_empty() {
            return Ok(None);
        }

        let Some(update_before) = dst_row_data
            .require_after()
            .ok()
            .or_else(|| dst_row_data.require_before().ok())
            .filter(|m| !m.is_empty())
            .cloned()
        else {
            return Ok(None);
        };

        let mut update_row = RowData::new(
            src_row_data.schema.clone(),
            src_row_data.tb.clone(),
            RowType::Update,
            Some(update_before),
            Some(update_after),
        );
        update_row.refresh_data_size();

        self.build_update_query(&update_row)
    }

    fn build_insert_query(&self, row_data: &RowData) -> anyhow::Result<Option<String>> {
        match self.meta {
            ReviseSqlMeta::Mysql(tb_meta) => RdbQueryBuilder::new_for_mysql(tb_meta, None)
                .get_query_sql(row_data, false)
                .map(Some),
            ReviseSqlMeta::Pg(tb_meta) => RdbQueryBuilder::new_for_pg(tb_meta, None)
                .get_query_sql(row_data, false)
                .map(Some),
            ReviseSqlMeta::Mongo => unreachable!("Mongo should be handled in build_miss_sql"),
        }
    }

    fn build_update_query(&self, row_data: &RowData) -> anyhow::Result<Option<String>> {
        match self.meta {
            ReviseSqlMeta::Mysql(tb_meta) => {
                let meta_cow = if self.match_full_row {
                    let mut owned = tb_meta.clone();
                    owned.basic.id_cols = owned.basic.cols.clone();
                    Cow::Owned(owned)
                } else {
                    Cow::Borrowed(tb_meta)
                };

                RdbQueryBuilder::new_for_mysql(meta_cow.as_ref(), None)
                    .get_query_sql(row_data, false)
                    .map(Some)
            }
            ReviseSqlMeta::Pg(tb_meta) => {
                let meta_cow = if self.match_full_row {
                    let mut owned = tb_meta.clone();
                    owned.basic.id_cols = owned.basic.cols.clone();
                    Cow::Owned(owned)
                } else {
                    Cow::Borrowed(tb_meta)
                };

                RdbQueryBuilder::new_for_pg(meta_cow.as_ref(), None)
                    .get_query_sql(row_data, false)
                    .map(Some)
            }
            ReviseSqlMeta::Mongo => unreachable!("Mongo should be handled in build_miss_sql"),
        }
    }
}

pub struct BaseChecker {}

pub struct BatchCompareRange {
    pub start_index: usize,
    pub batch_size: usize,
}

pub struct BatchCompareContext<'ctx> {
    pub dst_tb_meta: &'ctx RdbTbMeta,
    pub extractor_meta_manager: &'ctx mut RdbMetaManager,
    pub reverse_router: &'ctx RdbRouter,
    pub output_full_row: bool,
    pub revise_ctx: Option<&'ctx ReviseSqlContext<'ctx>>,
}

impl BaseChecker {
    pub async fn batch_compare_row_data_items(
        src_data: &[RowData],
        dst_row_data_map: &HashMap<u128, RowData>,
        range: BatchCompareRange,
        ctx: BatchCompareContext<'_>,
    ) -> anyhow::Result<(Vec<CheckLog>, Vec<CheckLog>, usize)> {
        let BatchCompareContext {
            dst_tb_meta,
            extractor_meta_manager,
            reverse_router,
            output_full_row,
            revise_ctx,
        } = ctx;

        let start = range.start_index;
        let end = (start + range.batch_size).min(src_data.len());

        if start >= src_data.len() {
            return Ok((Vec::new(), Vec::new(), 0));
        }

        let target_slice = &src_data[start..end];
        let mut miss = Vec::new();
        let mut diff = Vec::new();
        let mut sql_count = 0;

        for src_row_data in target_slice {
            let hash_code = src_row_data.get_hash_code(dst_tb_meta)?;
            // src_row_data is already routed, so here we call get_hash_code by dst_tb_meta
            match dst_row_data_map.get(&hash_code) {
                Some(dst_row_data) => {
                    let diff_col_values = Self::compare_row_data(src_row_data, dst_row_data)?;
                    if diff_col_values.is_empty() {
                        continue;
                    }

                    if let Some(revise_sql) = revise_ctx
                        .as_ref()
                        .map(|ctx| ctx.build_diff_sql(src_row_data, dst_row_data, &diff_col_values))
                        .transpose()?
                        .flatten()
                    {
                        log_sql!("{}", revise_sql);
                        sql_count += 1;
                    }

                    let diff_log = Self::build_diff_log(
                        src_row_data,
                        dst_row_data,
                        diff_col_values,
                        extractor_meta_manager,
                        reverse_router,
                        output_full_row,
                    )
                    .await?;
                    diff.push(diff_log);
                }
                None => {
                    if let Some(revise_sql) = revise_ctx
                        .as_ref()
                        .map(|ctx| ctx.build_miss_sql(src_row_data))
                        .transpose()?
                        .flatten()
                    {
                        log_sql!("{}", revise_sql);
                        sql_count += 1;
                    }

                    let miss_log = Self::build_miss_log(
                        src_row_data,
                        extractor_meta_manager,
                        reverse_router,
                        output_full_row,
                    )
                    .await?;
                    miss.push(miss_log);
                }
            }
        }

        Ok((miss, diff, sql_count))
    }

    pub fn compare_row_data(
        src_row_data: &RowData,
        dst_row_data: &RowData,
    ) -> anyhow::Result<HashMap<String, DiffColValue>> {
        let src = src_row_data
            .after
            .as_ref()
            .context("src row data after is missing")?;
        let dst = dst_row_data
            .after
            .as_ref()
            .context("dst row data after is missing")?;

        let mut diff_col_values = HashMap::new();
        for (col, src_val) in src {
            let dst_val = dst.get(col);
            let maybe_diff = match dst_val {
                Some(dst_val) if src_val == dst_val => None,
                Some(dst_val) => {
                    let src_type = Self::col_value_type_name(src_val);
                    let dst_type = Self::col_value_type_name(dst_val);
                    let type_diff = src_type != dst_type;
                    let src_type = type_diff.then(|| src_type.to_string());
                    let dst_type = type_diff.then(|| dst_type.to_string());

                    Some(DiffColValue {
                        src: src_val.to_option_string(),
                        dst: dst_val.to_option_string(),
                        src_type,
                        dst_type,
                    })
                }
                None => Some(DiffColValue {
                    src: src_val.to_option_string(),
                    dst: None,
                    src_type: Some(Self::col_value_type_name(src_val).to_string()),
                    dst_type: None,
                }),
            };

            if let Some(diff_entry) = maybe_diff {
                diff_col_values.insert(col.to_owned(), diff_entry);
            }
        }

        let should_expand_doc = diff_col_values.contains_key(MongoConstants::DOC)
            && [src_row_data, dst_row_data].iter().any(|row| {
                matches!(
                    row.after.as_ref().and_then(|m| m.get(MongoConstants::DOC)),
                    Some(ColValue::MongoDoc(_))
                )
            });

        if should_expand_doc {
            diff_col_values =
                Self::expand_mongo_doc_diff(src_row_data, dst_row_data, diff_col_values);
        }

        Ok(diff_col_values)
    }

    pub fn log_dml(miss: &[CheckLog], diff: &[CheckLog]) {
        for log in miss {
            log_miss!("{}", log);
        }
        for log in diff {
            log_diff!("{}", log);
        }
    }

    pub fn compare_struct(
        src_statement: &mut StructStatement,
        dst_statement: &mut StructStatement,
        filter: &RdbFilter,
    ) -> anyhow::Result<(usize, usize, usize)> {
        if matches!(dst_statement, StructStatement::Unknown) {
            log_miss!("{:?}", src_statement.to_sqls(filter)?);
            return Ok((1, 0, 0));
        }

        let src_sqls: HashMap<_, _> = src_statement.to_sqls(filter)?.into_iter().collect();
        let dst_sqls: HashMap<_, _> = dst_statement.to_sqls(filter)?.into_iter().collect();

        let mut miss_count = 0;
        let mut diff_count = 0;
        let mut extra_count = 0;

        for (key, src_sql) in &src_sqls {
            if let Some(dst_sql) = dst_sqls.get(key) {
                if src_sql != dst_sql {
                    log_diff!("key: {}, src_sql: {}", key, src_sql);
                    log_diff!("key: {}, dst_sql: {}", key, dst_sql);
                    diff_count += 1;
                }
            } else {
                log_miss!("key: {}, src_sql: {}", key, src_sql);
                miss_count += 1;
            }
        }

        for (key, dst_sql) in &dst_sqls {
            if !src_sqls.contains_key(key) {
                log_extra!("key: {}, dst_sql: {}", key, dst_sql);
                extra_count += 1;
            }
        }

        Ok((miss_count, diff_count, extra_count))
    }

    pub async fn build_miss_log(
        src_row_data: &RowData,
        extractor_meta_manager: &mut RdbMetaManager,
        reverse_router: &RdbRouter,
        output_full_row: bool,
    ) -> anyhow::Result<CheckLog> {
        let reverse_src_row_data = reverse_router.route_row(src_row_data.clone());

        let is_routed = src_row_data.schema != reverse_src_row_data.schema
            || src_row_data.tb != reverse_src_row_data.tb;

        // None if not routed
        let target_schema = is_routed.then(|| src_row_data.schema.clone());
        let target_tb = is_routed.then(|| src_row_data.tb.clone());

        let src_tb_meta = extractor_meta_manager
            .get_tb_meta(&reverse_src_row_data.schema, &reverse_src_row_data.tb)
            .await?;

        let id_col_values = Self::build_id_col_values(&reverse_src_row_data, src_tb_meta)
            .context("Failed to build ID col values")?;

        let src_row = output_full_row
            .then(|| Self::clone_row_values(&reverse_src_row_data))
            .flatten();

        Ok(CheckLog {
            schema: reverse_src_row_data.schema,
            tb: reverse_src_row_data.tb,
            target_schema,
            target_tb,
            id_col_values,
            diff_col_values: HashMap::new(),
            src_row,
            dst_row: None,
        })
    }

    pub async fn build_diff_log(
        src_row_data: &RowData,
        dst_row_data: &RowData,
        diff_col_values: HashMap<String, DiffColValue>,
        extractor_meta_manager: &mut RdbMetaManager,
        reverse_router: &RdbRouter,
        output_full_row: bool,
    ) -> anyhow::Result<CheckLog> {
        let mut log = Self::build_miss_log(
            src_row_data,
            extractor_meta_manager,
            reverse_router,
            output_full_row,
        )
        .await?;

        let mapped_diff_values = if let Some(col_map) =
            reverse_router.get_col_map(&src_row_data.schema, &src_row_data.tb)
        {
            let mut mapped = HashMap::with_capacity(diff_col_values.len());
            for (col, val) in diff_col_values {
                let mapped_col = col_map.get(&col).unwrap_or(&col).to_owned();
                mapped.insert(mapped_col, val);
            }
            mapped
        } else {
            diff_col_values
        };

        log.diff_col_values = mapped_diff_values;

        if output_full_row {
            let reverse_dst_row_data = reverse_router.route_row(dst_row_data.clone());
            log.dst_row = Self::clone_row_values(&reverse_dst_row_data);
        }

        Ok(log)
    }

    pub fn build_mongo_miss_log(
        src_row_data: RowData,
        tb_meta: &RdbTbMeta,
        reverse_router: &RdbRouter,
        output_full_row: bool,
    ) -> anyhow::Result<CheckLog> {
        let reverse_src_row_data = reverse_router.route_row(src_row_data.clone());

        let (target_schema, target_tb) = if src_row_data.schema != reverse_src_row_data.schema
            || src_row_data.tb != reverse_src_row_data.tb
        {
            (
                Some(src_row_data.schema.clone()),
                Some(src_row_data.tb.clone()),
            )
        } else {
            (None, None)
        };

        let id_col_values =
            Self::build_id_col_values(&reverse_src_row_data, tb_meta).unwrap_or_default();

        let src_row = if output_full_row {
            Self::clone_row_values(&reverse_src_row_data)
        } else {
            None
        };

        Ok(CheckLog {
            schema: reverse_src_row_data.schema,
            tb: reverse_src_row_data.tb,
            target_schema,
            target_tb,
            id_col_values,
            diff_col_values: HashMap::new(),
            src_row,
            dst_row: None,
        })
    }

    pub fn build_mongo_diff_log(
        src_row_data: RowData,
        dst_row_data: RowData,
        diff_col_values: HashMap<String, DiffColValue>,
        tb_meta: &RdbTbMeta,
        reverse_router: &RdbRouter,
        output_full_row: bool,
    ) -> anyhow::Result<CheckLog> {
        let mut diff_log =
            Self::build_mongo_miss_log(src_row_data, tb_meta, reverse_router, output_full_row)?;

        diff_log.diff_col_values = diff_col_values;

        if output_full_row {
            let reverse_dst_row_data = reverse_router.route_row(dst_row_data);
            diff_log.dst_row = Self::clone_row_values(&reverse_dst_row_data);
        }

        Ok(diff_log)
    }

    fn clone_row_values(row_data: &RowData) -> Option<HashMap<String, ColValue>> {
        match row_data.row_type {
            RowType::Insert | RowType::Update => row_data.after.clone(),
            RowType::Delete => row_data.before.clone(),
        }
    }

    fn col_value_type_name(value: &ColValue) -> &'static str {
        match value {
            ColValue::None => "None",
            ColValue::Bool(_) => "Bool",
            ColValue::Tiny(_) => "Tiny",
            ColValue::UnsignedTiny(_) => "UnsignedTiny",
            ColValue::Short(_) => "Short",
            ColValue::UnsignedShort(_) => "UnsignedShort",
            ColValue::Long(_) => "Long",
            ColValue::UnsignedLong(_) => "UnsignedLong",
            ColValue::LongLong(_) => "LongLong",
            ColValue::UnsignedLongLong(_) => "UnsignedLongLong",
            ColValue::Float(_) => "Float",
            ColValue::Double(_) => "Double",
            ColValue::Decimal(_) => "Decimal",
            ColValue::Time(_) => "Time",
            ColValue::Date(_) => "Date",
            ColValue::DateTime(_) => "DateTime",
            ColValue::Timestamp(_) => "Timestamp",
            ColValue::Year(_) => "Year",
            ColValue::String(_) => "String",
            ColValue::RawString(_) => "RawString",
            ColValue::Blob(_) => "Blob",
            ColValue::Bit(_) => "Bit",
            ColValue::Set(_) => "Set",
            ColValue::Enum(_) => "Enum",
            ColValue::Set2(_) => "Set2",
            ColValue::Enum2(_) => "Enum2",
            ColValue::Json(_) => "Json",
            ColValue::Json2(_) => "Json2",
            ColValue::Json3(_) => "Json3",
            ColValue::MongoDoc(_) => "MongoDoc",
        }
    }

    fn build_id_col_values(
        row_data: &RowData,
        tb_meta: &RdbTbMeta,
    ) -> Option<HashMap<String, Option<String>>> {
        let mut id_col_values = HashMap::new();
        let after = row_data.require_after().ok()?;

        for col in tb_meta.id_cols.iter() {
            let val = after.get(col)?.to_option_string();
            id_col_values.insert(col.to_owned(), val);
        }
        Some(id_col_values)
    }

    pub fn expand_mongo_doc_diff(
        src_row_data: &RowData,
        dst_row_data: &RowData,
        mut diff_col_values: HashMap<String, DiffColValue>,
    ) -> HashMap<String, DiffColValue> {
        // avoid output full mongo document to diff
        diff_col_values.remove(MongoConstants::DOC);

        let doc_from_row = |row: &RowData| {
            row.after
                .as_ref()
                .and_then(|after| after.get(MongoConstants::DOC))
                .and_then(|val| match val {
                    ColValue::MongoDoc(doc) => Some(doc.clone()),
                    _ => None,
                })
        };

        let src_doc = doc_from_row(src_row_data);
        let dst_doc = doc_from_row(dst_row_data);

        let keys: BTreeSet<_> = src_doc
            .iter()
            .flat_map(Document::keys)
            .cloned()
            .chain(dst_doc.iter().flat_map(Document::keys).cloned())
            .collect();

        for key in keys {
            let src_value = src_doc.as_ref().and_then(|d| d.get(&key));
            let dst_value = dst_doc.as_ref().and_then(|d| d.get(&key));
            let src_type_name = src_value.map(mongo_cmd::bson_type_name).unwrap_or("None");
            let dst_type_name = dst_value.map(mongo_cmd::bson_type_name).unwrap_or("None");
            let type_diff = src_type_name != dst_type_name;
            let value_diff = src_value != dst_value;

            if value_diff || type_diff {
                diff_col_values.insert(
                    key,
                    DiffColValue {
                        src: src_value.map(mongo_cmd::bson_to_log_literal),
                        dst: dst_value.map(mongo_cmd::bson_to_log_literal),
                        src_type: type_diff.then(|| src_type_name.to_string()),
                        dst_type: type_diff.then(|| dst_type_name.to_string()),
                    },
                );
            }
        }

        diff_col_values
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dt_common::meta::{
        col_value::ColValue,
        mongo::mongo_constant::MongoConstants,
        mysql::mysql_col_type::MysqlColType,
        pg::{pg_col_type::PgColType, pg_tb_meta::PgTbMeta, pg_value_type::PgValueType},
        rdb_tb_meta::RdbTbMeta,
    };
    use mongodb::bson::{doc, oid::ObjectId};
    use std::collections::{HashMap, HashSet};

    fn build_tb_meta() -> MysqlTbMeta {
        let mut col_type_map = HashMap::new();
        col_type_map.insert("id".into(), MysqlColType::Int { unsigned: false });
        col_type_map.insert(
            "name".into(),
            MysqlColType::Varchar {
                length: 255,
                charset: "utf8mb4".into(),
            },
        );
        MysqlTbMeta {
            basic: RdbTbMeta {
                schema: "test_db".into(),
                tb: "test_tb".into(),
                cols: vec!["id".into(), "name".into()],
                nullable_cols: HashSet::new(),
                col_origin_type_map: HashMap::new(),
                key_map: HashMap::new(),
                order_cols: Vec::new(),
                order_cols_are_nullable: false,
                partition_col: "id".into(),
                id_cols: vec!["id".into()],
                foreign_keys: Vec::new(),
                ref_by_foreign_keys: Vec::new(),
            },
            col_type_map,
        }
    }

    fn build_row_data(id: i64, name: &str) -> RowData {
        let mut after = HashMap::new();
        after.insert("id".into(), ColValue::LongLong(id));
        after.insert("name".into(), ColValue::String(name.into()));
        RowData::new(
            "test_db".into(),
            "test_tb".into(),
            RowType::Insert,
            None,
            Some(after),
        )
    }

    fn build_pg_tb_meta() -> PgTbMeta {
        let mut col_type_map = HashMap::new();
        col_type_map.insert(
            "id".into(),
            PgColType {
                value_type: PgValueType::Int64,
                name: "int8".into(),
                alias: "int8".into(),
                oid: 20,
                parent_oid: 0,
                element_oid: 0,
                category: "N".into(),
                enum_values: None,
            },
        );
        col_type_map.insert(
            "name".into(),
            PgColType {
                value_type: PgValueType::String,
                name: "varchar".into(),
                alias: "varchar".into(),
                oid: 1043,
                parent_oid: 0,
                element_oid: 0,
                category: "S".into(),
                enum_values: None,
            },
        );
        PgTbMeta {
            basic: RdbTbMeta {
                schema: "pg_db".into(),
                tb: "pg_tb".into(),
                cols: vec!["id".into(), "name".into()],
                nullable_cols: HashSet::new(),
                col_origin_type_map: HashMap::new(),
                key_map: HashMap::new(),
                order_cols: Vec::new(),
                order_cols_are_nullable: false,
                partition_col: "id".into(),
                id_cols: vec!["id".into()],
                foreign_keys: Vec::new(),
                ref_by_foreign_keys: Vec::new(),
            },
            oid: 0,
            col_type_map,
        }
    }

    #[test]
    fn mysql_builds_insert_sql() {
        let meta = build_tb_meta();
        let ctx = ReviseSqlContext::mysql(&meta, false);
        let row = build_row_data(1, "new_name");
        let sql = ctx.build_miss_sql(&row).unwrap().unwrap();
        assert!(sql.starts_with("INSERT INTO `test_db`.`test_tb`"));
        assert!(sql.contains("VALUES"));
        assert!(sql.contains("'new_name'"));
    }

    #[test]
    fn mysql_builds_update_sql_with_pk_match() {
        let meta = build_tb_meta();
        let ctx = ReviseSqlContext::mysql(&meta, false);

        let src_row = build_row_data(1, "fresh");
        let dst_row = build_row_data(1, "stale");

        let mut diff_cols = HashMap::new();
        diff_cols.insert(
            "name".into(),
            DiffColValue {
                src: Some("fresh".into()),
                dst: Some("stale".into()),
                src_type: None,
                dst_type: None,
            },
        );

        let sql = ctx
            .build_diff_sql(&src_row, &dst_row, &diff_cols)
            .unwrap()
            .unwrap();
        assert!(sql.starts_with("UPDATE `test_db`.`test_tb`"));
        assert!(sql.contains("SET `name`='fresh'"));
        assert!(sql.contains("WHERE `id` = 1"));
    }

    #[test]
    fn mysql_builds_update_sql_with_full_row_match() {
        let meta = build_tb_meta();
        let ctx = ReviseSqlContext::mysql(&meta, true);

        let src_row = build_row_data(1, "fixed");
        let dst_row = build_row_data(1, "broken");

        let mut diff_cols = HashMap::new();
        diff_cols.insert(
            "name".into(),
            DiffColValue {
                src: Some("fixed".into()),
                dst: Some("broken".into()),
                src_type: None,
                dst_type: None,
            },
        );

        let sql = ctx
            .build_diff_sql(&src_row, &dst_row, &diff_cols)
            .unwrap()
            .unwrap();
        assert!(sql.contains("SET `name`='fixed'"));
        assert!(sql.contains("WHERE `id` = 1 AND `name` = 'broken'"));
    }

    #[test]
    fn pg_builds_insert_sql() {
        let meta = build_pg_tb_meta();
        let ctx = ReviseSqlContext::pg(&meta, false);
        let row = build_row_data(10, "pg_name");
        let sql = ctx.build_miss_sql(&row).unwrap().unwrap();
        assert!(sql.starts_with("INSERT INTO \"pg_db\".\"pg_tb\""));
        assert!(sql.contains("VALUES"));
    }

    #[test]
    fn pg_full_row_match_includes_where_clause() {
        let meta = build_pg_tb_meta();
        let ctx = ReviseSqlContext::pg(&meta, true);
        let src_row = build_row_data(2, "fresh");
        let dst_row = build_row_data(2, "stale");
        let mut diff_cols = HashMap::new();
        diff_cols.insert(
            "name".into(),
            DiffColValue {
                src: Some("fresh".into()),
                dst: Some("stale".into()),
                src_type: None,
                dst_type: None,
            },
        );
        let sql = ctx
            .build_diff_sql(&src_row, &dst_row, &diff_cols)
            .unwrap()
            .unwrap();
        assert!(sql.contains("SET \"name\"='fresh'"));
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("\"id\" = 2"));
        assert!(sql.contains("\"name\" = 'stale'"));
    }

    #[test]
    fn mongo_builds_insert_cmd() {
        let ctx = ReviseSqlContext::mongo();
        let object_id = ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap();
        let doc = doc! { MongoConstants::ID: object_id, "name": "mongo_user" };

        let mut after = HashMap::new();
        after.insert(
            MongoConstants::DOC.to_string(),
            ColValue::MongoDoc(doc.clone()),
        );
        let row = RowData::new(
            "mongo_db".into(),
            "mongo_tb".into(),
            RowType::Insert,
            None,
            Some(after),
        );

        let sql = ctx.build_miss_sql(&row).unwrap().unwrap();
        assert!(sql.starts_with("db.mongo_tb.insertOne"));
        assert!(sql.contains("mongo_user"));
    }

    #[test]
    fn mongo_builds_update_cmd() {
        let ctx = ReviseSqlContext::mongo();
        let object_id = ObjectId::parse_str("507f1f77bcf86cd799439012").unwrap();
        let src_doc = doc! { MongoConstants::ID: object_id, "name": "new_name", "age": 18i32 };
        let dst_doc = doc! { MongoConstants::ID: object_id, "name": "old_name" };

        let mut src_after = HashMap::new();
        src_after.insert(
            MongoConstants::DOC.to_string(),
            ColValue::MongoDoc(src_doc.clone()),
        );
        let src_row = RowData::new(
            "mongo_db".into(),
            "mongo_tb".into(),
            RowType::Insert,
            None,
            Some(src_after),
        );

        let mut dst_after = HashMap::new();
        dst_after.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(dst_doc));
        let dst_row = RowData::new(
            "mongo_db".into(),
            "mongo_tb".into(),
            RowType::Insert,
            None,
            Some(dst_after),
        );

        let diff_col_values = BaseChecker::compare_row_data(&src_row, &dst_row).unwrap();

        let sql = ctx
            .build_diff_sql(&src_row, &dst_row, &diff_col_values)
            .unwrap()
            .unwrap();
        assert!(sql.starts_with("db.mongo_tb.updateOne"));
        assert!(sql.contains("'$set'"));
        assert!(sql.contains("'name': 'new_name'"));
    }

    #[test]
    fn verify_mongo_doc_strict_type_diff() {
        use crate::sinker::base_checker::BaseChecker;
        use dt_common::meta::mongo::mongo_constant::MongoConstants;
        use dt_common::meta::{col_value::ColValue, row_data::RowData, row_type::RowType};
        use mongodb::bson::doc;
        use std::collections::HashMap;

        // Create src with Int32(5) and dst with Int64(5)
        let src_doc = doc! { "val": 5i32 };
        let dst_doc = doc! { "val": 5i64 };

        let mut src_after = HashMap::new();
        src_after.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(src_doc));
        let src_row = RowData::new(
            "db".into(),
            "coll".into(),
            RowType::Insert,
            None,
            Some(src_after),
        );

        let mut dst_after = HashMap::new();
        dst_after.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(dst_doc));
        let dst_row = RowData::new(
            "db".into(),
            "coll".into(),
            RowType::Insert,
            None,
            Some(dst_after),
        );

        // Compare row data
        let diff_col_values = BaseChecker::compare_row_data(&src_row, &dst_row).unwrap();
        assert!(diff_col_values.contains_key("val"));
        assert!(!diff_col_values.contains_key(MongoConstants::DOC));

        // Build diff log
        let tb_meta = Default::default();
        let reverse_router = crate::rdb_router::RdbRouter {
            schema_map: HashMap::new(),
            tb_map: HashMap::new(),
            col_map: HashMap::new(),
            topic_map: HashMap::new(),
        };

        let check_log = BaseChecker::build_mongo_diff_log(
            src_row,
            dst_row,
            diff_col_values,
            &tb_meta,
            &reverse_router,
            false,
        )
        .unwrap();

        // Verify expansion and type diff
        assert!(check_log.diff_col_values.contains_key("val"));
        assert!(!check_log.diff_col_values.contains_key(MongoConstants::DOC));

        let diff_val = check_log.diff_col_values.get("val").unwrap();
        assert_eq!(diff_val.src_type, Some("Int32".to_string()));
        assert_eq!(diff_val.dst_type, Some("Int64".to_string()));
        // Values might look the same string-wise, but types differ
        assert_eq!(diff_val.src, Some("5".to_string()));
        assert_eq!(diff_val.dst, Some("NumberLong(5)".to_string()));
    }

    #[test]
    fn expand_mongo_doc_diff_strings_unquoted() {
        let object_id = ObjectId::parse_str("507f1f77bcf86cd799439012").unwrap();
        let src_doc = doc! { MongoConstants::ID: object_id, "email": "bob@example.com" };
        let dst_doc = doc! { MongoConstants::ID: object_id, "email": "bob_updated@example.com" };

        let mut src_after = HashMap::new();
        src_after.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(src_doc));
        let src_row = RowData::new(
            "db".into(),
            "coll".into(),
            RowType::Insert,
            None,
            Some(src_after),
        );

        let mut dst_after = HashMap::new();
        dst_after.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(dst_doc));
        let dst_row = RowData::new(
            "db".into(),
            "coll".into(),
            RowType::Insert,
            None,
            Some(dst_after),
        );

        let diff_col_values = BaseChecker::compare_row_data(&src_row, &dst_row).unwrap();
        let diff_val = diff_col_values.get("email").expect("email diff missing");

        assert_eq!(diff_val.src.as_deref(), Some("bob@example.com"));
        assert_eq!(diff_val.dst.as_deref(), Some("bob_updated@example.com"));
        assert!(diff_val.src_type.is_none());
        assert!(diff_val.dst_type.is_none());
    }
}
