use mongodb::bson::Document;
use std::collections::{BTreeSet, HashMap};

use dt_common::meta::{
    col_value::ColValue, mongo::mongo_constant::MongoConstants, mysql::mysql_tb_meta::MysqlTbMeta,
    pg::pg_tb_meta::PgTbMeta, rdb_meta_manager::RdbMetaManager, rdb_tb_meta::RdbTbMeta,
    row_data::RowData, row_type::RowType,
    struct_meta::statement::struct_statement::StructStatement,
};
use dt_common::{log_diff, log_extra, log_miss, rdb_filter::RdbFilter};

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
        let sql = self.build_insert_query(&insert_row)?;
        Ok(Some(sql))
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
        let mut update_after = HashMap::new();
        for col in diff_col_values.keys() {
            if let Some(value) = src_after.get(col) {
                update_after.insert(col.clone(), value.clone());
            }
        }
        if update_after.is_empty() {
            return Ok(None);
        }

        let update_before = dst_row_data
            .after
            .as_ref()
            .cloned()
            .or_else(|| dst_row_data.before.clone());
        let update_before = match update_before {
            Some(before) if !before.is_empty() => before,
            _ => return Ok(None),
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
    pub async fn batch_compare_row_data_items<'ctx>(
        src_data: &[RowData],
        dst_row_data_map: &HashMap<u128, RowData>,
        range: BatchCompareRange,
        ctx: BatchCompareContext<'ctx>,
    ) -> anyhow::Result<(Vec<CheckLog>, Vec<CheckLog>)> {
        let BatchCompareContext {
            dst_tb_meta,
            extractor_meta_manager,
            reverse_router,
            output_full_row,
            revise_ctx,
        } = ctx;
        let mut miss = Vec::new();
        let mut diff = Vec::new();
        for src_row_data in src_data
            .iter()
            .skip(range.start_index)
            .take(range.batch_size)
        {
            // src_row_data is already routed, so here we call get_hash_code by dst_tb_meta
            let hash_code = src_row_data.get_hash_code(dst_tb_meta);
            if let Some(dst_row_data) = dst_row_data_map.get(&hash_code) {
                let diff_col_values = Self::compare_row_data(src_row_data, dst_row_data);
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
    ) -> HashMap<String, DiffColValue> {
        let mut diff_col_values = HashMap::new();
        let src = src_row_data.after.as_ref().unwrap();
        let dst = dst_row_data.after.as_ref().unwrap();
        for (col, src_col_value) in src.iter() {
            if let Some(dst_col_value) = dst.get(col) {
                if src_col_value != dst_col_value {
                    let diff_col_value = DiffColValue {
                        src: src_col_value.to_option_string(),
                        dst: dst_col_value.to_option_string(),
                    };
                    diff_col_values.insert(col.to_owned(), diff_col_value);
                }
            } else {
                let diff_col_value = DiffColValue {
                    src: src_col_value.to_option_string(),
                    dst: None,
                };
                diff_col_values.insert(col.to_owned(), diff_col_value);
            }
        }
        diff_col_values
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

        let mut src_sqls = HashMap::new();
        for (key, sql) in src_statement.to_sqls(filter)? {
            src_sqls.insert(key, sql);
        }

        let mut dst_sqls = HashMap::new();
        for (key, sql) in dst_statement.to_sqls(filter)? {
            dst_sqls.insert(key, sql);
        }

        for (key, src_sql) in src_sqls.iter() {
            if let Some(dst_sql) = dst_sqls.get(key) {
                if src_sql != dst_sql {
                    log_diff!("key: {}, src_sql: {}", key, src_sql);
                    log_diff!("key: {}, dst_sql: {}", key, dst_sql);
                }
            } else {
                log_miss!("key: {}, src_sql: {}", key, src_sql);
            }
        }

        for (key, dst_sql) in dst_sqls.iter() {
            if !src_sqls.contains_key(key) {
                log_extra!("key: {}, dst_sql: {}", key, dst_sql);
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
        let target_schema_value = src_row_data.schema.clone();
        let target_tb_value = src_row_data.tb.clone();
        // route src_row_data back since we need origin extracted row_data in check log
        let reverse_src_row_data = reverse_router.route_row(src_row_data.clone());
        let emit_target_fields = target_schema_value != reverse_src_row_data.schema
            || target_tb_value != reverse_src_row_data.tb;
        let target_schema = emit_target_fields.then_some(target_schema_value);
        let target_tb = emit_target_fields.then_some(target_tb_value);
        let src_tb_meta = extractor_meta_manager
            .get_tb_meta(&reverse_src_row_data.schema, &reverse_src_row_data.tb)
            .await?;

        let id_col_values = Self::build_id_col_values(&reverse_src_row_data, src_tb_meta);
        let src_row = if output_full_row {
            Self::clone_row_values(&reverse_src_row_data)
        } else {
            None
        };
        let miss_log = CheckLog {
            log_type: LogType::Miss,
            schema: reverse_src_row_data.schema.clone(),
            tb: reverse_src_row_data.tb.clone(),
            target_schema,
            target_tb,
            id_col_values,
            diff_col_values: HashMap::new(),
            src_row,
            dst_row: None,
            revise_sql,
        };
        Ok(miss_log)
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
        // share same logic to fill basic CheckLog fields as miss log
        let miss_log = Self::build_miss_log(
            src_row_data,
            extractor_meta_manager,
            reverse_router,
            output_full_row,
            revise_ctx,
        )
        .await?;
        let diff_col_values = if let Some(col_map) =
            reverse_router.get_col_map(&src_row_data.schema, &src_row_data.tb)
        {
            let mut reverse_diff_col_values = HashMap::new();
            for (col, diff_col_value) in diff_col_values {
                let reverse_col = col_map.get(&col).unwrap();
                reverse_diff_col_values.insert(reverse_col.to_owned(), diff_col_value);
            }
            reverse_diff_col_values
        } else {
            diff_col_values
        };

        let mut diff_log = CheckLog {
            log_type: LogType::Diff,
            schema: miss_log.schema,
            tb: miss_log.tb,
            target_schema: miss_log.target_schema.clone(),
            target_tb: miss_log.target_tb.clone(),
            id_col_values: miss_log.id_col_values,
            diff_col_values,
            src_row: miss_log.src_row,
            dst_row: None,
            revise_sql: None,
        };
        if output_full_row {
            let reverse_dst_row_data = reverse_router.route_row(dst_row_data.clone());
            diff_log.dst_row = Self::clone_row_values(&reverse_dst_row_data);
        }
        diff_log.revise_sql = revise_sql;
        Ok(diff_log)
    }

    pub fn build_mongo_miss_log(
        src_row_data: RowData,
        tb_meta: &RdbTbMeta,
        reverse_router: &RdbRouter,
        output_full_row: bool,
        output_revise_sql: bool,
    ) -> CheckLog {
        let target_schema_value = src_row_data.schema.clone();
        let target_tb_value = src_row_data.tb.clone();
        let reverse_src_row_data = reverse_router.route_row(src_row_data.clone());
        let emit_target_fields = target_schema_value != reverse_src_row_data.schema
            || target_tb_value != reverse_src_row_data.tb;
        let target_schema = emit_target_fields.then_some(target_schema_value);
        let target_tb = emit_target_fields.then_some(target_tb_value);
        let id_col_values = Self::build_id_col_values(&reverse_src_row_data, tb_meta);
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
        let revise_sql = if output_revise_sql {
            Self::build_mongo_update_cmd(&src_row_data, &diff_col_values)
        } else {
            None
        };
        let mut diff_log = Self::build_mongo_miss_log(
            src_row_data.clone(),
            tb_meta,
            reverse_router,
            output_full_row,
            false, // Don't generate insert cmd for diff log
        );
        diff_log.diff_col_values = diff_col_values;
        if output_revise_sql
            && diff_log.diff_col_values.len() == 1
            && diff_log.diff_col_values.contains_key(MongoConstants::DOC)
        {
            diff_log.diff_col_values = Self::expand_mongo_doc_diff(&src_row_data, &dst_row_data);
        }
        if output_full_row {
            let reverse_dst_row_data = reverse_router.route_row(dst_row_data);
            diff_log.dst_row = Self::clone_row_values(&reverse_dst_row_data);
        }
        diff_log.log_type = LogType::Diff;
        diff_log.revise_sql = revise_sql;
        // no col map in mongo
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
    ) -> HashMap<String, Option<String>> {
        let mut id_col_values = HashMap::new();
        let after = row_data.after.as_ref().unwrap();
        for col in tb_meta.id_cols.iter() {
            id_col_values.insert(col.to_owned(), after.get(col).unwrap().to_option_string());
        }
        id_col_values
    }

    /// Build MongoDB insertOne command for miss logs
    fn build_mongo_insert_cmd(src_row_data: &RowData) -> Option<String> {
        use dt_common::meta::mongo::mongo_constant::MongoConstants;

        let after = src_row_data.after.as_ref()?;
        let doc = after.get(MongoConstants::DOC)?;

        if let ColValue::MongoDoc(bson_doc) = doc {
            // Generate MongoDB insertOne command
            let cmd = format!("db.{}.insertOne({})", src_row_data.tb, bson_doc);
            Some(cmd)
        } else {
            None
        }
    }

    /// Build MongoDB updateOne command for diff logs
    fn build_mongo_update_cmd(
        src_row_data: &RowData,
        diff_col_values: &HashMap<String, DiffColValue>,
    ) -> Option<String> {
        use dt_common::meta::mongo::mongo_constant::MongoConstants;

        let after = src_row_data.after.as_ref()?;
        let doc = after.get(MongoConstants::DOC)?;

        if let ColValue::MongoDoc(bson_doc) = doc {
            // Extract _id for filter
            let id = bson_doc.get(MongoConstants::ID)?;

            let mut set_doc = Document::new();
            for (col, _) in diff_col_values {
                if col == MongoConstants::DOC || col == MongoConstants::ID {
                    continue;
                }
                if let Some(value) = bson_doc.get(col) {
                    set_doc.insert(col.clone(), value.clone());
                } else {
                    log_extra!("Mongo doc missing column {} for revise command", col);
                }
            }

            let update_doc = if set_doc.is_empty() {
                format!("$set: {}", bson_doc)
            } else {
                format!("$set: {}", set_doc)
            };

            let cmd = format!(
                "db.{}.updateOne({{ \"_id\": {} }}, {{ {} }})",
                src_row_data.tb, id, update_doc
            );
            Some(cmd)
        } else {
            None
        }
    }

    fn expand_mongo_doc_diff(
        src_row_data: &RowData,
        dst_row_data: &RowData,
    ) -> HashMap<String, DiffColValue> {
        let src_doc = src_row_data
            .after
            .as_ref()
            .and_then(|after| after.get(MongoConstants::DOC))
            .and_then(|col_value| {
                if let ColValue::MongoDoc(doc) = col_value {
                    Some(doc.clone())
                } else {
                    None
                }
            });
        let dst_doc = dst_row_data
            .after
            .as_ref()
            .and_then(|after| after.get(MongoConstants::DOC))
            .and_then(|col_value| {
                if let ColValue::MongoDoc(doc) = col_value {
                    Some(doc.clone())
                } else {
                    None
                }
            });

        let mut keys = BTreeSet::new();
        if let Some(doc) = &src_doc {
            keys.extend(doc.keys().cloned());
        }
        if let Some(doc) = &dst_doc {
            keys.extend(doc.keys().cloned());
        }

        let mut diff_cols = HashMap::new();
        for key in keys {
            if key == MongoConstants::ID {
                continue;
            }
            let src_value = src_doc.as_ref().and_then(|doc| doc.get(&key));
            let dst_value = dst_doc.as_ref().and_then(|doc| doc.get(&key));
            if src_value != dst_value {
                let diff_col_value = DiffColValue {
                    src: src_value.map(|v| v.to_string()),
                    dst: dst_value.map(|v| v.to_string()),
                };
                diff_cols.insert(key, diff_col_value);
            }
        }
        diff_cols
    }
}
