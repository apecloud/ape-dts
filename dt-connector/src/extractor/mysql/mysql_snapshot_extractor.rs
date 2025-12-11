use std::{
    cmp,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use anyhow::bail;
use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{MySql, Pool};
use tokio::{sync::Mutex, task::JoinHandle};

use crate::{
    extractor::{base_extractor::BaseExtractor, resumer::recovery::Recovery},
    rdb_query_builder::RdbQueryBuilder,
    rdb_router::RdbRouter,
    Extractor,
};
use dt_common::{
    config::config_enums::DbType,
    log_debug, log_error, log_info,
    meta::{
        adaptor::{mysql_col_value_convertor::MysqlColValueConvertor, sqlx_ext::SqlxMysqlExt},
        col_value::ColValue,
        dt_data::{DtData, DtItem},
        dt_queue::DtQueue,
        mysql::{
            mysql_col_type::MysqlColType, mysql_meta_manager::MysqlMetaManager,
            mysql_tb_meta::MysqlTbMeta,
        },
        position::Position,
        row_data::RowData,
    },
    rdb_filter::RdbFilter,
};

pub struct MysqlSnapshotExtractor {
    pub base_extractor: BaseExtractor,
    pub conn_pool: Pool<MySql>,
    pub meta_manager: MysqlMetaManager,
    pub filter: RdbFilter,
    pub batch_size: usize,
    pub parallel_size: usize,
    pub sample_interval: usize,
    pub db: String,
    pub tb: String,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

struct ExtractColValue {
    value: ColValue,
}

#[async_trait]
impl Extractor for MysqlSnapshotExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        log_info!(
            "MysqlSnapshotExtractor starts, schema: `{}`, tb: `{}`, batch_size: {}, parallel_size: {}",
            self.db,
            self.tb,
            self.batch_size,
            self.parallel_size
        );
        self.extract_internal().await?;
        self.base_extractor.wait_task_finish().await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl MysqlSnapshotExtractor {
    async fn extract_internal(&mut self) -> anyhow::Result<()> {
        let extracted_count;
        let tb_meta = self
            .meta_manager
            .get_tb_meta(&self.db, &self.tb)
            .await?
            .to_owned();

        if let Some(order_col) = &tb_meta.basic.order_col {
            let order_col_type = tb_meta.get_col_type(order_col)?;
            let parallel_extract = self.parallel_size > 1
                && matches!(
                    order_col_type,
                    MysqlColType::Int { .. }
                        | MysqlColType::BigInt { .. }
                        | MysqlColType::MediumInt { .. }
                );

            let resume_value = if let Some(handler) = &self.recovery {
                if let Some(value) = handler
                    .get_snapshot_resume_position(&self.db, &self.tb, order_col, parallel_extract)
                    .await
                {
                    log_info!(
                        "[{}.{}] recovery from [{}]:[{}]",
                        self.db,
                        self.tb,
                        order_col,
                        value
                    );
                    MysqlColValueConvertor::from_str(order_col_type, &value)?
                } else {
                    ColValue::None
                }
            } else {
                ColValue::None
            };

            log_info!(
                "start extracting data from `{}`.`{}` by batch, order_col: {}, order_col_type: {}, start_value: {}",
                self.db,
                self.tb,
                order_col,
                order_col_type,
                resume_value.to_string()
            );

            extracted_count = if parallel_extract {
                log_info!("parallel extracting, parallel_size: {}", self.parallel_size);
                self.parallel_extract_by_batch(&tb_meta, order_col, order_col_type, resume_value)
                    .await?
            } else {
                self.extract_by_batch(&tb_meta, order_col, order_col_type, resume_value)
                    .await?
            };
        } else {
            extracted_count = self.extract_all(&tb_meta).await?;
        }

        log_info!(
            "end extracting data from `{}`.`{}`, all count: {}",
            self.db,
            self.tb,
            extracted_count
        );
        Ok(())
    }

    async fn extract_all(&mut self, tb_meta: &MysqlTbMeta) -> anyhow::Result<u64> {
        log_info!(
            "start extracting data from `{}`.`{}` without batch",
            self.db,
            self.tb
        );

        let ignore_cols = self.filter.get_ignore_cols(&self.db, &self.tb);
        let cols_str = self.build_extract_cols_str(tb_meta)?;
        let where_sql = BaseExtractor::get_where_sql(&self.filter, &self.db, &self.tb, "");
        let sql = format!(
            "SELECT {} FROM `{}`.`{}` {}",
            cols_str, self.db, self.tb, where_sql
        );

        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let row_data = RowData::from_mysql_row(&row, tb_meta, &ignore_cols);
            self.base_extractor
                .push_row(row_data, Position::None)
                .await?;
        }
        Ok(self.base_extractor.monitor.counters.pushed_record_count)
    }

    async fn extract_by_batch(
        &mut self,
        tb_meta: &MysqlTbMeta,
        order_col: &str,
        order_col_type: &MysqlColType,
        resume_value: ColValue,
    ) -> anyhow::Result<u64> {
        let mut extracted_count = 0;
        let mut start_value = resume_value;
        let ignore_cols = self.filter.get_ignore_cols(&self.db, &self.tb);
        let cols_str = self.build_extract_cols_str(tb_meta)?;

        let where_sql_1 = BaseExtractor::get_where_sql(&self.filter, &self.db, &self.tb, "");
        let sql_1 = format!(
            "SELECT {} FROM `{}`.`{}` {} ORDER BY `{}` ASC LIMIT {}",
            cols_str, self.db, self.tb, where_sql_1, order_col, self.batch_size
        );

        let condition_2 = format!("`{}` > ?", order_col);
        let where_sql_2 =
            BaseExtractor::get_where_sql(&self.filter, &self.db, &self.tb, &condition_2);
        let sql_2 = format!(
            "SELECT {} FROM `{}`.`{}` {} ORDER BY `{}` ASC LIMIT {}",
            cols_str, self.db, self.tb, where_sql_2, order_col, self.batch_size
        );

        loop {
            let start_value_for_bind = start_value.clone();
            let query = if let ColValue::None = start_value {
                sqlx::query(&sql_1)
            } else {
                sqlx::query(&sql_2).bind_col_value(Some(&start_value_for_bind), order_col_type)
            };

            let mut rows = query.fetch(&self.conn_pool);
            let mut slice_count = 0usize;

            while let Some(row) = rows.try_next().await.unwrap() {
                start_value = MysqlColValueConvertor::from_query(&row, order_col, order_col_type)?;
                extracted_count += 1;
                slice_count += 1;
                // sampling may be used in check scenario
                if extracted_count % self.sample_interval != 0 {
                    continue;
                }

                let row_data = RowData::from_mysql_row(&row, tb_meta, &ignore_cols);
                let position = if let Some(value) = start_value.to_option_string() {
                    Position::RdbSnapshot {
                        db_type: DbType::Mysql.to_string(),
                        schema: self.db.clone(),
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

        Ok(extracted_count as u64)
    }

    async fn parallel_extract_by_batch(
        &mut self,
        tb_meta: &MysqlTbMeta,
        order_col: &str,
        order_col_type: &MysqlColType,
        resume_value: ColValue,
    ) -> anyhow::Result<u64> {
        let all_extracted_count = Arc::new(AtomicU64::new(0));
        let parallel_size = self.parallel_size;
        let batch_size = cmp::max(self.batch_size / parallel_size, 1);
        let router = Arc::new(self.base_extractor.router.clone());
        let ignore_cols = self.filter.get_ignore_cols(&self.db, &self.tb).cloned();

        let mut start_value = resume_value;
        let cols_str = self.build_extract_cols_str(tb_meta)?;

        let where_sql_1 = BaseExtractor::get_where_sql(&self.filter, &self.db, &self.tb, "");
        let sql_1 = format!(
            "SELECT {} FROM `{}`.`{}` {} ORDER BY `{}` ASC LIMIT {}",
            cols_str, self.db, self.tb, where_sql_1, order_col, batch_size
        );

        let condition_2 = format!("`{}` > ? AND `{}` <= ?", order_col, order_col);
        let where_sql_2 =
            BaseExtractor::get_where_sql(&self.filter, &self.db, &self.tb, &condition_2);
        let sql_2 = format!(
            "SELECT {} FROM `{}`.`{}` {} ORDER BY `{}` ASC LIMIT {}",
            cols_str, self.db, self.tb, where_sql_2, order_col, batch_size
        );

        let condition_3 = format!("`{}` > ?", order_col);
        let where_sql_3 =
            BaseExtractor::get_where_sql(&self.filter, &self.db, &self.tb, &condition_3);
        let sql_3 = format!(
            "SELECT {} FROM `{}`.`{}` {} ORDER BY `{}` ASC LIMIT {}",
            cols_str, self.db, self.tb, where_sql_3, order_col, batch_size
        );

        loop {
            // send a checkpoint position before each loop
            self.send_checkpoint_position(order_col, &start_value)
                .await?;

            let all_finished = Arc::new(AtomicBool::new(false));
            let last_order_col_value = Arc::new(Mutex::new(ExtractColValue {
                value: start_value.clone(),
            }));

            if let ColValue::None = start_value {
                let mut slice_count = 0;
                let query = sqlx::query(&sql_1);
                let mut rows = query.fetch(&self.conn_pool);
                while let Some(row) = rows.try_next().await? {
                    start_value =
                        MysqlColValueConvertor::from_query(&row, order_col, order_col_type)?;
                    let row_data = RowData::from_mysql_row(&row, tb_meta, &ignore_cols.as_ref());
                    let position =
                        Self::build_position(&self.db, &self.tb, order_col, &start_value);
                    Self::push_row(&self.base_extractor.buffer, &router, row_data, position)
                        .await?;
                    slice_count += 1;
                }

                all_extracted_count.fetch_add(slice_count, Ordering::Release);
                all_finished.store(slice_count < batch_size as u64, Ordering::Release);
            } else {
                let mut futures = Vec::new();
                for i in 0..parallel_size {
                    let buffer = self.base_extractor.buffer.clone();
                    let router = router.clone();
                    let conn_pool = self.conn_pool.clone();
                    let db = self.db.clone();
                    let tb = self.tb.clone();
                    let tb_meta = tb_meta.clone();
                    let order_col = order_col.to_string();
                    let order_col_type = order_col_type.clone();
                    let ignore_cols = ignore_cols.clone();

                    let all_extracted_count = all_extracted_count.clone();
                    let all_finished = all_finished.clone();
                    let last_order_col_value = last_order_col_value.clone();

                    let (sub_start_value, sub_end_value) =
                        Self::get_sub_extractor_range(&start_value, i, batch_size);
                    let sql = if i == parallel_size - 1 {
                        // the last extractor
                        sql_3.clone()
                    } else {
                        sql_2.clone()
                    };

                    let future: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
                        let mut query = sqlx::query(&sql)
                            .bind_col_value(Some(&sub_start_value), &order_col_type);
                        if i < parallel_size - 1 {
                            query = query.bind_col_value(Some(&sub_end_value), &order_col_type);
                        }
                        let mut rows = query.fetch(&conn_pool);

                        let mut order_col_value = ColValue::None;
                        let mut slice_count = 0;
                        while let Some(row) = rows.try_next().await? {
                            order_col_value = MysqlColValueConvertor::from_query(
                                &row,
                                &order_col,
                                &order_col_type,
                            )?;

                            let row_data =
                                RowData::from_mysql_row(&row, &tb_meta, &ignore_cols.as_ref());
                            let position =
                                Self::build_position(&db, &tb, &order_col, &order_col_value);
                            Self::push_row(&buffer, &router, row_data, position).await?;
                            slice_count += 1;
                        }

                        all_extracted_count.fetch_add(slice_count, Ordering::Release);
                        if i == parallel_size - 1 {
                            last_order_col_value.lock().await.value = order_col_value;
                            all_finished.store(slice_count < batch_size as u64, Ordering::Release);
                        }
                        Ok(())
                    });
                    futures.push(future);
                }

                let mut execute_failed = false;
                for future in futures {
                    match future.await {
                        Ok(Ok(())) => (),
                        Ok(Err(error)) => {
                            log_error!(
                                "db: `{}`, tb: `{}`, parallel extract failed, error: {}",
                                self.db,
                                self.tb,
                                error
                            );
                            execute_failed = true;
                        }
                        Err(error) => {
                            log_error!(
                                "db: `{}`, tb: `{}`, parallel extract failed, error: {}",
                                self.db,
                                self.tb,
                                error
                            );
                            execute_failed = true;
                        }
                    }
                }

                if execute_failed {
                    bail!(
                        "db: `{}`, tb: `{}`, parallel extract failed",
                        self.db,
                        self.tb
                    );
                }

                start_value = last_order_col_value.lock().await.value.clone();
            }

            if all_finished.load(Ordering::Acquire) {
                break;
            }
        }

        Ok(all_extracted_count.load(Ordering::Acquire))
    }

    pub async fn push_row(
        buffer: &Arc<DtQueue>,
        router: &Arc<RdbRouter>,
        row_data: RowData,
        position: Position,
    ) -> anyhow::Result<()> {
        let row_data = router.route_row(row_data);
        let dt_data = DtData::Dml { row_data };
        let item = DtItem {
            dt_data,
            position,
            data_origin_node: String::new(),
        };
        log_debug!("extracted item: {:?}", item);
        buffer.push(item).await
    }

    fn get_sub_extractor_range(
        start_value: &ColValue,
        extractor_index: usize,
        batch_size: usize,
    ) -> (ColValue, ColValue) {
        let i = extractor_index;
        let v = match start_value {
            ColValue::Long(v) => *v as i128,
            ColValue::UnsignedLong(v) => *v as i128,
            ColValue::LongLong(v) => *v as i128,
            ColValue::UnsignedLongLong(v) => *v as i128,
            _ => 0,
        };

        let start = v + i as i128 * batch_size as i128;
        let end = start + batch_size as i128;
        match start_value {
            ColValue::Long(_) => (
                ColValue::Long(cmp::min(start, i32::MAX as i128) as i32),
                ColValue::Long(cmp::min(end, i32::MAX as i128) as i32),
            ),
            ColValue::UnsignedLong(_) => (
                ColValue::UnsignedLong(cmp::min(start, u32::MAX as i128) as u32),
                ColValue::UnsignedLong(cmp::min(end, u32::MAX as i128) as u32),
            ),
            ColValue::LongLong(_) => (
                ColValue::LongLong(cmp::min(start, i64::MAX as i128) as i64),
                ColValue::LongLong(cmp::min(end, i64::MAX as i128) as i64),
            ),
            ColValue::UnsignedLongLong(_) => (
                ColValue::UnsignedLongLong(cmp::min(start, u64::MAX as i128) as u64),
                ColValue::UnsignedLongLong(cmp::min(end, u64::MAX as i128) as u64),
            ),
            _ => (ColValue::None, ColValue::None),
        }
    }

    fn build_position(db: &str, tb: &str, order_col: &str, order_col_value: &ColValue) -> Position {
        if let Some(value) = order_col_value.to_option_string() {
            Position::RdbSnapshot {
                db_type: DbType::Mysql.to_string(),
                schema: db.into(),
                tb: tb.into(),
                order_col: order_col.into(),
                value,
            }
        } else {
            Position::None
        }
    }

    async fn send_checkpoint_position(
        &mut self,
        order_col: &str,
        order_col_value: &ColValue,
    ) -> anyhow::Result<()> {
        if *order_col_value == ColValue::None {
            return Ok(());
        }

        let position = Self::build_position(&self.db, &self.tb, order_col, order_col_value);
        let commit = DtData::Commit { xid: String::new() };
        self.base_extractor.push_dt_data(commit, position).await
    }

    fn build_extract_cols_str(&self, tb_meta: &MysqlTbMeta) -> anyhow::Result<String> {
        let ignore_cols = self.filter.get_ignore_cols(&self.db, &self.tb);
        let query_builder = RdbQueryBuilder::new_for_mysql(tb_meta, ignore_cols);
        query_builder.build_extract_cols_str()
    }
}
