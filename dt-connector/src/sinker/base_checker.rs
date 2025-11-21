use anyhow::Context;
use mongodb::bson::{Bson, Document};
use std::collections::{BTreeSet, HashMap};

use dt_common::meta::{
    col_value::ColValue, mongo::mongo_constant::MongoConstants, mysql::mysql_tb_meta::MysqlTbMeta,
    pg::pg_tb_meta::PgTbMeta, rdb_meta_manager::RdbMetaManager, rdb_tb_meta::RdbTbMeta,
    row_data::RowData, row_type::RowType,
    struct_meta::statement::struct_statement::StructStatement,
};
use dt_common::{log_diff, log_miss, rdb_filter::RdbFilter};

use crate::{
    check_log::{
        check_log::{CheckLog, DiffColValue},
        log_type::LogType,
    },
    rdb_query_builder::RdbQueryBuilder,
    rdb_router::RdbRouter,
};

pub enum ReviseSqlMeta<'a> {
    Mysql(&'a MysqlTbMeta),
    Pg(&'a PgTbMeta),
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

    pub fn build_miss_sql(&self, src_row_data: &RowData) -> anyhow::Result<Option<String>> {
        let after = match &src_row_data.after {
            Some(after) if !after.is_empty() => after.clone(),
            _ => return Ok(None),
        };

        let mut insert_row = RowData::new(
            src_row_data.schema.clone(),
            src_row_data.tb.clone(),
            RowType::Insert,
            None,
            Some(after),
        );
        insert_row.refresh_data_size();

        self.build_insert_query(&insert_row).map(Some)
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

        let src_after = match &src_row_data.after {
            Some(after) => after,
            None => return Ok(None),
        };

        let update_after: HashMap<_, _> = diff_col_values
            .keys()
            .filter_map(|col| src_after.get(col).map(|v| (col.clone(), v.clone())))
            .collect();

        if update_after.is_empty() {
            return Ok(None);
        }

        let update_before = dst_row_data
            .after
            .as_ref()
            .or(dst_row_data.before.as_ref())
            .filter(|before| !before.is_empty())
            .cloned();

        let Some(update_before) = update_before else {
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

        let sql = self.build_update_query(&update_row)?;
        Ok(Some(sql))
    }

    fn build_insert_query(&self, row_data: &RowData) -> anyhow::Result<String> {
        match self.meta {
            ReviseSqlMeta::Mysql(tb_meta) => {
                RdbQueryBuilder::new_for_mysql(tb_meta, None).get_query_sql(row_data, false)
            }
            ReviseSqlMeta::Pg(tb_meta) => {
                RdbQueryBuilder::new_for_pg(tb_meta, None).get_query_sql(row_data, false)
            }
        }
    }

    fn build_update_query(&self, row_data: &RowData) -> anyhow::Result<String> {
        match self.meta {
            ReviseSqlMeta::Mysql(tb_meta) => {
                if self.match_full_row {
                    let mut owned = tb_meta.clone();
                    owned.basic.id_cols = owned.basic.cols.clone();
                    RdbQueryBuilder::new_for_mysql(&owned, None).get_query_sql(row_data, false)
                } else {
                    RdbQueryBuilder::new_for_mysql(tb_meta, None).get_query_sql(row_data, false)
                }
            }
            ReviseSqlMeta::Pg(tb_meta) => {
                if self.match_full_row {
                    let mut owned = tb_meta.clone();
                    owned.basic.id_cols = owned.basic.cols.clone();
                    RdbQueryBuilder::new_for_pg(&owned, None).get_query_sql(row_data, false)
                } else {
                    RdbQueryBuilder::new_for_pg(tb_meta, None).get_query_sql(row_data, false)
                }
            }
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
    #[inline(always)]
    pub async fn batch_compare_row_data_items(
        src_data: &[RowData],
        dst_row_data_map: &HashMap<u128, RowData>,
        range: BatchCompareRange,
        ctx: BatchCompareContext<'_>,
    ) -> anyhow::Result<(Vec<CheckLog>, Vec<CheckLog>)> {
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
            return Ok((Vec::new(), Vec::new()));
        }

        let target_slice = &src_data[start..end];
        let mut miss = Vec::new();
        let mut diff = Vec::new();

        for src_row_data in target_slice {
            let hash_code = src_row_data.get_hash_code(dst_tb_meta)?;
            // src_row_data is already routed, so here we call get_hash_code by dst_tb_meta
            if let Some(dst_row_data) = dst_row_data_map.get(&hash_code) {
                let diff_col_values = Self::compare_row_data(src_row_data, dst_row_data)?;
                if !diff_col_values.is_empty() {
                    let diff_log = Self::build_diff_log(
                        src_row_data,
                        dst_row_data,
                        diff_col_values,
                        extractor_meta_manager,
                        reverse_router,
                        output_full_row,
                        revise_ctx,
                    )
                    .await?;
                    diff.push(diff_log);
                }
            } else {
                let miss_log = Self::build_miss_log(
                    src_row_data,
                    extractor_meta_manager,
                    reverse_router,
                    output_full_row,
                    revise_ctx,
                )
                .await?;
                miss.push(miss_log);
            }
        }
        Ok((miss, diff))
    }

    #[inline(always)]
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

        let diff_col_values = src
            .iter()
            .filter_map(|(col, src_val)| match dst.get(col) {
                Some(dst_val) if src_val == dst_val => None,
                Some(dst_val) => Some((
                    col.clone(),
                    DiffColValue {
                        src: src_val.to_option_string(),
                        dst: dst_val.to_option_string(),
                    },
                )),
                None => Some((
                    col.clone(),
                    DiffColValue {
                        src: src_val.to_option_string(),
                        dst: None,
                    },
                )),
            })
            .collect();

        Ok(diff_col_values)
    }

    pub fn log_dml(miss: Vec<CheckLog>, diff: Vec<CheckLog>) {
        for log in miss {
            log_miss!("{}", log.to_string());
        }
        for log in diff {
            log_diff!("{}", log.to_string());
        }
    }

    #[inline(always)]
    pub fn compare_struct(
        src_statement: &mut StructStatement,
        dst_statement: &mut StructStatement,
        filter: &RdbFilter,
    ) -> anyhow::Result<()> {
        if matches!(dst_statement, StructStatement::Unknown) {
            log_miss!("{:?}", src_statement.to_sqls(filter)?);
            return Ok(());
        }

        let src_sqls = src_statement.to_sqls(filter)?;
        let dst_sqls: HashMap<_, _> = dst_statement.to_sqls(filter)?.into_iter().collect();

        for (key, src_sql) in src_sqls {
            if let Some(dst_sql) = dst_sqls.get(&key) {
                if src_sql != *dst_sql {
                    log_diff!("key: {}, src_sql: {}", key, src_sql);
                    log_diff!("key: {}, dst_sql: {}", key, dst_sql);
                }
            } else {
                log_miss!("key: {}, src_sql: {}", key, src_sql);
            }
        }

        Ok(())
    }

    pub async fn build_miss_log(
        src_row_data: &RowData,
        extractor_meta_manager: &mut RdbMetaManager,
        reverse_router: &RdbRouter,
        output_full_row: bool,
        revise_ctx: Option<&ReviseSqlContext<'_>>,
    ) -> anyhow::Result<CheckLog> {
        let revise_sql = match revise_ctx {
            Some(ctx) => ctx.build_miss_sql(src_row_data)?,
            None => None,
        };

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

        let src_tb_meta = extractor_meta_manager
            .get_tb_meta(&reverse_src_row_data.schema, &reverse_src_row_data.tb)
            .await?;

        let id_col_values = Self::build_id_col_values(&reverse_src_row_data, src_tb_meta)
            .context("Failed to build ID col values")?;

        let src_row = if output_full_row {
            Self::clone_row_values(&reverse_src_row_data)
        } else {
            None
        };

        Ok(CheckLog {
            log_type: LogType::Miss,
            schema: reverse_src_row_data.schema,
            tb: reverse_src_row_data.tb,
            target_schema,
            target_tb,
            id_col_values,
            diff_col_values: HashMap::new(),
            src_row,
            dst_row: None,
            revise_sql,
        })
    }

    pub async fn build_diff_log(
        src_row_data: &RowData,
        dst_row_data: &RowData,
        diff_col_values: HashMap<String, DiffColValue>,
        extractor_meta_manager: &mut RdbMetaManager,
        reverse_router: &RdbRouter,
        output_full_row: bool,
        revise_ctx: Option<&ReviseSqlContext<'_>>,
    ) -> anyhow::Result<CheckLog> {
        let revise_sql = match revise_ctx {
            Some(ctx) => ctx.build_diff_sql(src_row_data, dst_row_data, &diff_col_values)?,
            None => None,
        };

        let mut log = Self::build_miss_log(
            src_row_data,
            extractor_meta_manager,
            reverse_router,
            output_full_row,
            revise_ctx,
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

        log.log_type = LogType::Diff;
        log.diff_col_values = mapped_diff_values;
        log.revise_sql = revise_sql;

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
        output_revise_sql: bool,
    ) -> CheckLog {
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

        let revise_sql = if output_revise_sql {
            Self::build_mongo_insert_cmd(&src_row_data)
        } else {
            None
        };

        CheckLog {
            log_type: LogType::Miss,
            schema: reverse_src_row_data.schema,
            tb: reverse_src_row_data.tb,
            target_schema,
            target_tb,
            id_col_values,
            diff_col_values: HashMap::new(),
            src_row,
            dst_row: None,
            revise_sql,
        }
    }

    pub fn build_mongo_diff_log(
        src_row_data: RowData,
        dst_row_data: RowData,
        diff_col_values: HashMap<String, DiffColValue>,
        tb_meta: &RdbTbMeta,
        reverse_router: &RdbRouter,
        output_full_row: bool,
        output_revise_sql: bool,
    ) -> CheckLog {
        let mut diff_cols_for_revise = diff_col_values;
        if output_revise_sql
            && diff_cols_for_revise.len() == 1
            && diff_cols_for_revise.contains_key(MongoConstants::DOC)
        {
            diff_cols_for_revise = Self::expand_mongo_doc_diff(&src_row_data, &dst_row_data);
        }

        let revise_sql = if output_revise_sql {
            Self::build_mongo_update_cmd(&src_row_data, &diff_cols_for_revise)
        } else {
            None
        };

        let mut diff_log = Self::build_mongo_miss_log(
            src_row_data.clone(),
            tb_meta,
            reverse_router,
            output_full_row,
            false,
        );

        diff_log.diff_col_values = diff_cols_for_revise;
        diff_log.log_type = LogType::Diff;
        diff_log.revise_sql = revise_sql;

        if output_full_row {
            let reverse_dst_row_data = reverse_router.route_row(dst_row_data);
            diff_log.dst_row = Self::clone_row_values(&reverse_dst_row_data);
        }

        diff_log
    }

    fn clone_row_values(row_data: &RowData) -> Option<HashMap<String, ColValue>> {
        match row_data.row_type {
            RowType::Insert | RowType::Update => row_data.after.clone(),
            RowType::Delete => row_data.before.clone(),
        }
    }

    fn build_id_col_values(
        row_data: &RowData,
        tb_meta: &RdbTbMeta,
    ) -> Option<HashMap<String, Option<String>>> {
        let mut id_col_values = HashMap::new();
        let after = row_data.after.as_ref()?;

        for col in tb_meta.id_cols.iter() {
            let val = after.get(col)?.to_option_string();
            id_col_values.insert(col.to_owned(), val);
        }
        Some(id_col_values)
    }

    fn build_mongo_insert_cmd(src_row_data: &RowData) -> Option<String> {
        let after = src_row_data.after.as_ref()?;
        let doc = after.get(MongoConstants::DOC)?;

        if let ColValue::MongoDoc(bson_doc) = doc {
            Some(format!("db.{}.insertOne({})", src_row_data.tb, bson_doc))
        } else {
            None
        }
    }

    fn build_mongo_update_cmd(
        src_row_data: &RowData,
        diff_col_values: &HashMap<String, DiffColValue>,
    ) -> Option<String> {
        let after = src_row_data.after.as_ref()?;
        let doc = after.get(MongoConstants::DOC)?;

        if let ColValue::MongoDoc(bson_doc) = doc {
            let id = bson_doc.get(MongoConstants::ID)?;

            let mut set_doc = Document::new();
            let mut unset_doc = Document::new();
            let mut sorted_keys: Vec<_> = diff_col_values.keys().collect();
            sorted_keys.sort();
            for col in sorted_keys {
                if col == MongoConstants::DOC || col == MongoConstants::ID {
                    continue;
                }
                if let Some(value) = bson_doc.get(col) {
                    set_doc.insert(col.clone(), value.clone());
                } else {
                    unset_doc.insert(col.clone(), Bson::Int32(1));
                }
            }

            if set_doc.is_empty() && unset_doc.is_empty() {
                return None;
            }

            let mut parts = Vec::new();
            if !set_doc.is_empty() {
                parts.push(format!("\"$set\": {}", set_doc));
            }
            if !unset_doc.is_empty() {
                parts.push(format!("\"$unset\": {}", unset_doc));
            }

            Some(format!(
                "db.{}.updateOne({{ \"_id\": {} }}, {{ {} }})",
                src_row_data.tb,
                id,
                parts.join(", ")
            ))
        } else {
            None
        }
    }

    fn expand_mongo_doc_diff(
        src_row_data: &RowData,
        dst_row_data: &RowData,
    ) -> HashMap<String, DiffColValue> {
        let get_doc = |row: &RowData| {
            row.after
                .as_ref()
                .and_then(|after| after.get(MongoConstants::DOC))
                .and_then(|val| {
                    if let ColValue::MongoDoc(doc) = val {
                        Some(doc.clone())
                    } else {
                        None
                    }
                })
        };

        let src_doc = get_doc(src_row_data);
        let dst_doc = get_doc(dst_row_data);

        let mut keys = BTreeSet::new();
        if let Some(doc) = src_doc.as_ref() {
            keys.extend(doc.keys().cloned());
        }
        if let Some(doc) = dst_doc.as_ref() {
            keys.extend(doc.keys().cloned());
        }

        let mut diff_cols = HashMap::new();
        for key in keys {
            if key == MongoConstants::ID {
                continue;
            }
            let src_value = src_doc.as_ref().and_then(|d| d.get(&key));
            let dst_value = dst_doc.as_ref().and_then(|d| d.get(&key));

            if src_value != dst_value {
                diff_cols.insert(
                    key,
                    DiffColValue {
                        src: src_value.map(|v| v.to_string()),
                        dst: dst_value.map(|v| v.to_string()),
                    },
                );
            }
        }
        diff_cols
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dt_common::meta::{
        col_value::ColValue,
        mysql::mysql_col_type::MysqlColType,
        pg::{pg_col_type::PgColType, pg_tb_meta::PgTbMeta, pg_value_type::PgValueType},
        rdb_tb_meta::RdbTbMeta,
    };
    use std::collections::HashMap;

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
                col_origin_type_map: HashMap::new(),
                key_map: HashMap::new(),
                order_col: None,
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
                col_origin_type_map: HashMap::new(),
                key_map: HashMap::new(),
                order_col: None,
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
}
