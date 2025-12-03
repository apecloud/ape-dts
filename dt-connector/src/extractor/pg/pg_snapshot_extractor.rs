use anyhow::bail;
use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{Pool, Postgres};

use crate::{
    extractor::{base_extractor::BaseExtractor, resumer::recovery::Recovery},
    rdb_query_builder::RdbQueryBuilder,
    Extractor,
};
use dt_common::{
    config::config_enums::DbType,
    log_info,
    meta::{
        adaptor::{pg_col_value_convertor::PgColValueConvertor, sqlx_ext::SqlxPgExt},
        col_value::ColValue,
        pg::{pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
        position::Position,
        rdb_tb_meta::RdbTbMeta,
        row_data::RowData,
    },
    rdb_filter::RdbFilter,
    utils::serialize_util::SerializeUtil,
};

pub struct PgSnapshotExtractor {
    pub base_extractor: BaseExtractor,
    pub conn_pool: Pool<Postgres>,
    pub meta_manager: PgMetaManager,
    pub filter: RdbFilter,
    pub batch_size: usize,
    pub sample_interval: usize,
    pub schema: String,
    pub tb: String,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

#[async_trait]
impl Extractor for PgSnapshotExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        log_info!(
            r#"PgSnapshotExtractor starts, schema: "{}", tb: "{}", batch_size: {}"#,
            self.schema,
            self.tb,
            self.batch_size
        );
        self.extract_internal().await?;
        self.base_extractor.wait_task_finish().await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl PgSnapshotExtractor {
    async fn extract_internal(&mut self) -> anyhow::Result<()> {
        let tb_meta = self
            .meta_manager
            .get_tb_meta(&self.schema, &self.tb)
            .await?
            .to_owned();

        if Self::can_extract_by_batch(&tb_meta) {
            let resume_values = self.get_resume_values(&tb_meta).await?;
            if resume_values.is_empty() {
                log_info!(
                    r#"start extracting data from "{}"."{}" by batch from beginning"#,
                    self.schema,
                    self.tb
                );
            } else {
                log_info!(
                    r#"start extracting data from "{}"."{}" by batch, start_values: {}"#,
                    self.schema,
                    self.tb,
                    SerializeUtil::serialize_hashmap_to_json(&resume_values)?
                );
            }
            self.extract_by_batch(&tb_meta, resume_values).await?;
        } else {
            self.extract_all(&tb_meta).await?;
        }
        Ok(())
    }

    async fn extract_all(&mut self, tb_meta: &PgTbMeta) -> anyhow::Result<()> {
        log_info!(
            r#"start extracting data from "{}"."{}" without batch"#,
            self.schema,
            self.tb
        );

        let sql = self.build_extract_sql(tb_meta, false)?;
        let ignore_cols = self.filter.get_ignore_cols(&self.schema, &self.tb);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await? {
            let row_data = RowData::from_pg_row(&row, tb_meta, &ignore_cols);
            self.base_extractor
                .push_row(row_data, Position::None)
                .await?;
        }

        log_info!(
            r#"end extracting data from "{}"."{}", all count: {}"#,
            self.schema,
            self.tb,
            self.base_extractor.monitor.counters.pushed_record_count
        );
        Ok(())
    }

    async fn extract_by_batch(
        &mut self,
        tb_meta: &PgTbMeta,
        mut resume_values: HashMap<String, ColValue>,
    ) -> anyhow::Result<()> {
        let mut start_from_beginning = false;
        if resume_values.is_empty() {
            resume_values = tb_meta.basic.get_default_order_col_values();
            start_from_beginning = true;
        }

        let mut start_values = resume_values;
        let mut extracted_count = 0;
        let sql_from_beginning = self.build_extract_sql(tb_meta, false)?;
        let sql_from_value = self.build_extract_sql(tb_meta, true)?;
        let sql_for_null = if tb_meta.basic.order_cols_are_nullable {
            self.build_extract_null_sql(tb_meta, true)?
        } else {
            String::new()
        };
        let ignore_cols = self.filter.get_ignore_cols(&self.schema, &self.tb);

        loop {
            let bind_values = start_values.clone();
            let query = if start_from_beginning {
                start_from_beginning = false;
                sqlx::query(&sql_from_beginning)
            } else {
                let mut query = sqlx::query(&sql_from_value);
                for order_col in tb_meta.basic.order_cols.iter() {
                    let order_col_type = tb_meta.get_col_type(order_col)?;
                    query = query.bind_col_value(bind_values.get(order_col), order_col_type)
                }
                query
            };

            let mut rows = query.fetch(&self.conn_pool);
            let mut slice_count = 0usize;
            while let Some(row) = rows.try_next().await? {
                for order_col in tb_meta.basic.order_cols.iter() {
                    let order_col_type = tb_meta.get_col_type(order_col)?;
                    if let Some(value) = start_values.get_mut(order_col) {
                        *value = PgColValueConvertor::from_query(&row, order_col, order_col_type)?;
                    } else {
                        bail!(
                            r#""{}"."{}" order col {} not found"#,
                            self.schema,
                            self.tb,
                            order_col
                        );
                    }
                }
                slice_count += 1;
                extracted_count += 1;
                // sampling may be used in check scenario
                if extracted_count % self.sample_interval != 0 {
                    continue;
                }

                let row_data = RowData::from_pg_row(&row, tb_meta, &ignore_cols);
                let position = tb_meta.basic.build_position(&DbType::Pg, &start_values);
                self.base_extractor.push_row(row_data, position).await?;
            }

            // all data extracted
            if slice_count < self.batch_size {
                break;
            }
        }

        // extract rows with NULL
        if tb_meta.basic.order_cols_are_nullable && !sql_for_null.is_empty() {
            let mut rows = sqlx::query(&sql_for_null).fetch(&self.conn_pool);
            while let Some(row) = rows.try_next().await? {
                extracted_count += 1;
                let row_data = RowData::from_pg_row(&row, tb_meta, &ignore_cols);
                self.base_extractor
                    .push_row(row_data, Position::None)
                    .await?;
            }
        }

        log_info!(
            r#"end extracting data from "{}"."{}"", all count: {}"#,
            self.schema,
            self.tb,
            extracted_count
        );
        Ok(())
    }

    #[inline(always)]
    pub fn can_extract_by_batch(tb_meta: &PgTbMeta) -> bool {
        !tb_meta.basic.order_cols.is_empty()
    }

    fn build_extract_cols_str(&mut self, tb_meta: &PgTbMeta) -> anyhow::Result<String> {
        let ignore_cols = self.filter.get_ignore_cols(&self.schema, &self.tb);
        let query_builder = RdbQueryBuilder::new_for_pg(tb_meta, ignore_cols);
        query_builder.build_extract_cols_str()
    }

    fn build_extract_sql(
        &mut self,
        tb_meta: &PgTbMeta,
        has_start_value: bool,
    ) -> anyhow::Result<String> {
        let cols_str = self.build_extract_cols_str(tb_meta)?;
        let where_sql = BaseExtractor::get_where_sql(&self.filter, &self.schema, &self.tb, "");

        // SELECT col_1, col_2::text FROM tb_1 WHERE col_1 > $1 ORDER BY col_1;
        if Self::can_extract_by_batch(tb_meta) {
            let order_by_clause = Self::build_order_by_clause(&tb_meta.basic)?;
            if has_start_value {
                let mut where_clause = Self::build_order_col_predicate(tb_meta)?;
                if tb_meta.basic.order_cols_are_nullable {
                    let not_null_predicate = Self::build_null_predicate(&tb_meta.basic, false)?;
                    if !not_null_predicate.is_empty() {
                        where_clause = format!("{} AND {}", where_clause, not_null_predicate);
                    }
                }
                let where_sql = BaseExtractor::get_where_sql(
                    &self.filter,
                    &self.schema,
                    &self.tb,
                    &where_clause,
                );
                Ok(format!(
                    r#"SELECT {} FROM "{}"."{}" {} ORDER BY {} LIMIT {}"#,
                    cols_str, self.schema, self.tb, where_sql, order_by_clause, self.batch_size
                ))
            } else {
                Ok(format!(
                    r#"SELECT {} FROM "{}"."{}" {} ORDER BY {} LIMIT {}"#,
                    cols_str, self.schema, self.tb, where_sql, order_by_clause, self.batch_size
                ))
            }
        } else {
            Ok(format!(
                r#"SELECT {} FROM "{}"."{}" {}"#,
                cols_str, self.schema, self.tb, where_sql
            ))
        }
    }

    fn build_extract_null_sql(
        &mut self,
        tb_meta: &PgTbMeta,
        is_null: bool,
    ) -> anyhow::Result<String> {
        let cols_str = self.build_extract_cols_str(tb_meta)?;
        let order_by_clause = Self::build_order_by_clause(&tb_meta.basic)?;
        let null_predicate = Self::build_null_predicate(&tb_meta.basic, is_null)?;
        let where_sql =
            BaseExtractor::get_where_sql(&self.filter, &self.schema, &self.tb, &null_predicate);

        Ok(format!(
            r#"SELECT {} FROM "{}"."{}" {} ORDER BY {}"#,
            cols_str, self.schema, self.tb, where_sql, order_by_clause
        ))
    }

    fn build_order_col_predicate(tb_meta: &PgTbMeta) -> anyhow::Result<String> {
        let order_cols = &tb_meta.basic.order_cols;
        if order_cols.is_empty() {
            bail!("order cols is empty");
        } else if order_cols.len() == 1 {
            // col_1 > $1::col_1_type
            Ok(format!(
                r#""{}" > {}"#,
                &order_cols[0],
                format_args!(
                    r#"$1::{}"#,
                    tb_meta.get_col_type(&order_cols[0]).unwrap().alias
                )
            ))
        } else {
            // (col_1, col_2, col_3) > ($1::col_1_type, $2::col_2_type, $3::col_3_type)
            Ok(format!(
                r#"({}) > ({})"#,
                order_cols
                    .iter()
                    .map(|col| format!(r#""{}""#, col))
                    .collect::<Vec<String>>()
                    .join(", "),
                order_cols
                    .iter()
                    .enumerate()
                    .map(|(i, col)| {
                        let col_type = tb_meta.get_col_type(col).unwrap();
                        format!(r#"${}::{}"#, i + 1, col_type.alias)
                    })
                    .collect::<Vec<String>>()
                    .join(", ")
            ))
        }
    }

    fn build_order_by_clause(tb_meta: &RdbTbMeta) -> anyhow::Result<String> {
        let order_cols = &tb_meta.order_cols;
        if order_cols.is_empty() {
            bail!("order cols is empty");
        } else if order_cols.len() == 1 {
            Ok(format!(r#""{}" ASC"#, &order_cols[0]))
        } else {
            // col_1 ASC, col_2 ASC, col_3 ASC
            // (col_1, col_2, col_3) ASC does not trigger index scan sometimes
            Ok(order_cols
                .iter()
                .map(|col| format!(r#""{}" ASC"#, col))
                .collect::<Vec<String>>()
                .join(", "))
        }
    }

    fn build_null_predicate(tb_meta: &RdbTbMeta, is_null: bool) -> anyhow::Result<String> {
        let order_cols = &tb_meta.order_cols;
        let null_check = if is_null { "IS NULL" } else { "IS NOT NULL" };
        let join_str = if is_null { "OR" } else { "AND" };
        if order_cols.is_empty() {
            bail!("order cols is empty");
        } else {
            // col_1 IS NOT NULL AND col_2 IS NOT NULL AND col_3 IS NOT NULL
            // col_1 IS NULL OR col_2 IS NULL OR col_3 IS NULL
            Ok(order_cols
                .iter()
                .filter(|&col| tb_meta.nullable_cols.contains(col))
                .map(|col| format!(r#""{}" {}"#, col, null_check))
                .collect::<Vec<String>>()
                .join(&format!(" {} ", join_str)))
        }
    }

    async fn get_resume_values(
        &mut self,
        tb_meta: &PgTbMeta,
    ) -> anyhow::Result<HashMap<String, ColValue>> {
        let mut resume_values: HashMap<String, ColValue> = HashMap::new();
        if let Some(handler) = &self.recovery {
            if let Some(Position::RdbSnapshot {
                schema,
                tb,
                order_col,
                value,
                order_col_values,
                ..
            }) = handler
                .get_snapshot_resume_position(&self.schema, &self.tb, false)
                .await
            {
                if schema != self.schema || tb != self.tb {
                    log_info!(
                        r#""{}"."{}" resume position schema/tb not match, ignore it"#,
                        self.schema,
                        self.tb
                    );
                    return Ok(HashMap::new());
                }

                if order_col_values.is_empty() {
                    resume_values.insert(
                        order_col.clone(),
                        PgColValueConvertor::from_str(
                            tb_meta.get_col_type(&order_col)?,
                            &value,
                            &mut self.meta_manager,
                        )?,
                    );
                }
                for (order_col, value) in order_col_values {
                    if let Some(value_str) = value {
                        resume_values.insert(
                            order_col.clone(),
                            PgColValueConvertor::from_str(
                                tb_meta.get_col_type(&order_col)?,
                                &value_str,
                                &mut self.meta_manager,
                            )?,
                        );
                    } else {
                        resume_values.insert(order_col, ColValue::None);
                    }
                }
                if resume_values.len() != tb_meta.basic.order_cols.len() {
                    log_info!(
                        r#""{}"."{}" resume values not match order cols in length"#,
                        self.schema,
                        self.tb
                    );
                    return Ok(HashMap::new());
                }

                for order_col in &tb_meta.basic.order_cols {
                    if !resume_values.contains_key(order_col) {
                        log_info!(
                            r#""{}"."{}" resume position missing order col {}"#,
                            self.schema,
                            self.tb,
                            order_col
                        );
                        return Ok(HashMap::new());
                    }
                }
            } else {
                log_info!(r#""{}"."{}" has no resume position"#, self.schema, self.tb);
                return Ok(HashMap::new());
            }
        }
        log_info!(
            r#"["{}"."{}"] recovery from [{}]"#,
            self.schema,
            self.tb,
            SerializeUtil::serialize_hashmap_to_json(&resume_values)?
        );
        Ok(resume_values)
    }
}
