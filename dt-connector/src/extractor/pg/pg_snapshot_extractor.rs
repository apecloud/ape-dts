use async_trait::async_trait;
use dt_common::rdb_filter::RdbFilter;
use futures::TryStreamExt;

use sqlx::{Pool, Postgres};

use dt_common::{config::config_enums::DbType, log_info};

use dt_common::meta::{
    adaptor::{pg_col_value_convertor::PgColValueConvertor, sqlx_ext::SqlxPgExt},
    col_value::ColValue,
    pg::{pg_col_type::PgColType, pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
    position::Position,
    row_data::RowData,
};

use crate::close_conn_pool;
use crate::{
    extractor::{base_extractor::BaseExtractor, resumer::snapshot_resumer::SnapshotResumer},
    rdb_query_builder::RdbQueryBuilder,
    Extractor,
};

pub struct PgSnapshotExtractor {
    pub base_extractor: BaseExtractor,
    pub conn_pool: Pool<Postgres>,
    pub meta_manager: PgMetaManager,
    pub filter: RdbFilter,
    pub resumer: SnapshotResumer,
    pub batch_size: usize,
    pub sample_interval: usize,
    pub schema: String,
    pub tb: String,
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
        close_conn_pool!(self)
    }
}

impl PgSnapshotExtractor {
    async fn extract_internal(&mut self) -> anyhow::Result<()> {
        if !self.check_if_has_data().await? {
            log_info!(r#"no data found in "{}"."{}""#, self.schema, self.tb);
            return Ok(());
        }

        let tb_meta = self
            .meta_manager
            .get_tb_meta(&self.schema, &self.tb)
            .await?
            .to_owned();

        if let Some(order_col) = &tb_meta.basic.order_col {
            let order_col_type = tb_meta.get_col_type(order_col)?;

            let resume_value = if let Some(value) =
                self.resumer
                    .get_resume_value(&self.schema, &self.tb, order_col, false)
            {
                PgColValueConvertor::from_str(order_col_type, &value, &mut self.meta_manager)?
            } else {
                ColValue::None
            };

            self.extract_by_batch(&tb_meta, order_col, order_col_type, resume_value)
                .await?;
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
            self.base_extractor.monitor.counters.record_count
        );
        Ok(())
    }

    async fn extract_by_batch(
        &mut self,
        tb_meta: &PgTbMeta,
        order_col: &str,
        order_col_type: &PgColType,
        resume_value: ColValue,
    ) -> anyhow::Result<()> {
        log_info!(
            r#"start extracting data from "{}"."{}" by batch, order_col: {}, start_value: {}"#,
            self.schema,
            self.tb,
            order_col,
            resume_value.to_string()
        );

        let mut extracted_count = 0;
        let mut start_value = resume_value;
        let sql_1 = self.build_extract_sql(tb_meta, false)?;
        let sql_2 = self.build_extract_sql(tb_meta, true)?;
        let ignore_cols = self.filter.get_ignore_cols(&self.schema, &self.tb);
        loop {
            let start_value_for_bind = start_value.clone();
            let query = if let ColValue::None = start_value {
                sqlx::query(&sql_1)
            } else {
                sqlx::query(&sql_2).bind_col_value(Some(&start_value_for_bind), order_col_type)
            };

            let mut rows = query.fetch(&self.conn_pool);
            let mut slice_count = 0usize;
            while let Some(row) = rows.try_next().await? {
                start_value = PgColValueConvertor::from_query(&row, order_col, order_col_type)?;
                slice_count += 1;
                extracted_count += 1;
                // sampling may be used in check scenario
                if extracted_count % self.sample_interval != 0 {
                    continue;
                }

                let row_data = RowData::from_pg_row(&row, tb_meta, &ignore_cols);
                let position = if let Some(value) = start_value.to_option_string() {
                    Position::RdbSnapshot {
                        db_type: DbType::Pg.to_string(),
                        schema: self.schema.clone(),
                        tb: self.tb.clone(),
                        order_col: order_col.into(),
                        value,
                    }
                } else {
                    Position::None
                };

                self.base_extractor.push_row(row_data, position).await?;
            }

            // all data extracted
            if slice_count < self.batch_size {
                break;
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

    async fn check_if_has_data(&mut self) -> anyhow::Result<bool> {
        let sql = format!(r#"SELECT 1 FROM "{}"."{}" LIMIT 1"#, self.schema, self.tb);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        if let Some(_row) = rows.try_next().await? {
            return Ok(true);
        }
        Ok(false)
    }

    fn build_extract_sql(
        &mut self,
        tb_meta: &PgTbMeta,
        has_start_value: bool,
    ) -> anyhow::Result<String> {
        let ignore_cols = self.filter.get_ignore_cols(&self.schema, &self.tb);
        let query_builder = RdbQueryBuilder::new_for_pg(tb_meta, ignore_cols);
        let cols_str = query_builder.build_extract_cols_str()?;
        let where_sql = BaseExtractor::get_where_sql(&self.filter, &self.schema, &self.tb, "");

        // SELECT col_1, col_2::text FROM tb_1 WHERE col_1 > $1 ORDER BY col_1;
        if let Some(order_col) = &tb_meta.basic.order_col {
            if has_start_value {
                let order_col_type = tb_meta.get_col_type(order_col)?;
                let condition = format!(r#""{}" > $1::{}"#, order_col, order_col_type.alias);
                let where_sql =
                    BaseExtractor::get_where_sql(&self.filter, &self.schema, &self.tb, &condition);
                Ok(format!(
                    r#"SELECT {} FROM "{}"."{}" {} ORDER BY "{}" ASC LIMIT {}"#,
                    cols_str, self.schema, self.tb, where_sql, order_col, self.batch_size
                ))
            } else {
                Ok(format!(
                    r#"SELECT {} FROM "{}"."{}" {} ORDER BY "{}" ASC LIMIT {}"#,
                    cols_str, self.schema, self.tb, where_sql, order_col, self.batch_size
                ))
            }
        } else {
            Ok(format!(
                r#"SELECT {} FROM "{}"."{}" {}"#,
                cols_str, self.schema, self.tb, where_sql
            ))
        }
    }
}
