use std::collections::HashMap;

use dt_common::meta::{
    col_value::ColValue, rdb_meta_manager::RdbMetaManager, rdb_tb_meta::RdbTbMeta,
    row_data::RowData, row_type::RowType,
    struct_meta::statement::struct_statement::StructStatement,
};
use dt_common::{log_diff, log_extra, log_miss, rdb_filter::RdbFilter};

use crate::{
    check_log::{
        check_log::{CheckLog, DiffColValue},
        log_type::LogType,
    },
    rdb_router::RdbRouter,
};

pub struct BaseChecker {}

impl BaseChecker {
    #[inline(always)]
    pub async fn batch_compare_row_data_items(
        src_data: &[RowData],
        dst_row_data_map: &HashMap<u128, RowData>,
        start_index: usize,
        batch_size: usize,
        dst_tb_meta: &RdbTbMeta,
        extractor_meta_manager: &mut RdbMetaManager,
        reverse_router: &RdbRouter,
        output_full_row: bool,
    ) -> anyhow::Result<(Vec<CheckLog>, Vec<CheckLog>)> {
        let mut miss = Vec::new();
        let mut diff = Vec::new();
        for src_row_data in src_data.iter().skip(start_index).take(batch_size) {
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
    ) -> anyhow::Result<CheckLog> {
        // route src_row_data back since we need origin extracted row_data in check log
        let reverse_src_row_data = reverse_router.route_row(src_row_data.clone());
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
            id_col_values,
            diff_col_values: HashMap::new(),
            src_row,
            dst_row: None,
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
    ) -> anyhow::Result<CheckLog> {
        // share same logic to fill basic CheckLog fields as miss log
        let miss_log = Self::build_miss_log(
            src_row_data,
            extractor_meta_manager,
            reverse_router,
            output_full_row,
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
            id_col_values: miss_log.id_col_values,
            diff_col_values,
            src_row: miss_log.src_row,
            dst_row: None,
        };
        if output_full_row {
            let reverse_dst_row_data = reverse_router.route_row(dst_row_data.clone());
            diff_log.dst_row = Self::clone_row_values(&reverse_dst_row_data);
        }
        Ok(diff_log)
    }

    pub fn build_mongo_miss_log(
        src_row_data: RowData,
        tb_meta: &RdbTbMeta,
        reverse_router: &RdbRouter,
        output_full_row: bool,
    ) -> CheckLog {
        let reverse_src_row_data = reverse_router.route_row(src_row_data);
        let id_col_values = Self::build_id_col_values(&reverse_src_row_data, tb_meta);
        let src_row = if output_full_row {
            Self::clone_row_values(&reverse_src_row_data)
        } else {
            None
        };
        CheckLog {
            log_type: LogType::Miss,
            schema: reverse_src_row_data.schema,
            tb: reverse_src_row_data.tb,
            id_col_values,
            diff_col_values: HashMap::new(),
            src_row,
            dst_row: None,
        }
    }

    pub fn build_mongo_diff_log(
        src_row_data: RowData,
        dst_row_data: RowData,
        diff_col_values: HashMap<String, DiffColValue>,
        tb_meta: &RdbTbMeta,
        reverse_router: &RdbRouter,
        output_full_row: bool,
    ) -> CheckLog {
        let mut diff_log =
            Self::build_mongo_miss_log(src_row_data, tb_meta, reverse_router, output_full_row);
        diff_log.diff_col_values = diff_col_values;
        if output_full_row {
            let reverse_dst_row_data = reverse_router.route_row(dst_row_data);
            diff_log.dst_row = Self::clone_row_values(&reverse_dst_row_data);
        }
        diff_log.log_type = LogType::Diff;
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
}
