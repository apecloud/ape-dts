use std::{
    cmp,
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use anyhow::bail;
use async_trait::async_trait;
use clickhouse::sql;
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
    log_debug, log_info,
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
        rdb_tb_meta::RdbTbMeta,
        row_data::RowData,
    },
    rdb_filter::RdbFilter,
    utils::serialize_util::SerializeUtil,
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

        if tb_meta.basic.can_extract_by_batch() {
            let parallel_extract = self.can_parallel_extract(&tb_meta)?;
            let resume_values = self.get_resume_values(&tb_meta, parallel_extract).await?;
            if resume_values.is_empty() {
                log_info!(
                    "start extracting data from `{}`.`{}` by batch from beginning",
                    self.db,
                    self.tb
                );
            } else {
                log_info!(
                    "start extracting data from `{}`.`{}` by batch, start values: {}",
                    self.db,
                    self.tb,
                    SerializeUtil::serialize_hashmap_to_json(&resume_values)?,
                );
            }
            extracted_count = if parallel_extract {
                log_info!("parallel extracting, parallel_size: {}", self.parallel_size);
                self.parallel_extract_by_batch(&tb_meta, resume_values)
                    .await?
            } else {
                self.extract_by_batch(&tb_meta, resume_values).await?
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
        mut resume_values: HashMap<String, ColValue>,
    ) -> anyhow::Result<u64> {
        let mut start_from_beginning = false;
        if resume_values.is_empty() {
            resume_values = tb_meta
                .basic
                .order_cols
                .iter()
                .map(|col| (col.clone(), ColValue::None))
                .collect();
            start_from_beginning = true;
        }
        let mut extracted_count = 0;
        let mut start_values = resume_values;
        let ignore_cols = self.filter.get_ignore_cols(&self.db, &self.tb);
        let sql_from_beginning = self.build_extract_sql(tb_meta, self.batch_size, 1)?;
        let sql_from_value = self.build_extract_sql(tb_meta, self.batch_size, 3)?;

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

            while let Some(row) = rows.try_next().await.unwrap() {
                for order_col in tb_meta.basic.order_cols.iter() {
                    let order_col_type = tb_meta.get_col_type(order_col)?;
                    if let Some(value) = start_values.get_mut(order_col) {
                        *value =
                            MysqlColValueConvertor::from_query(&row, order_col, order_col_type)?;
                    } else {
                        bail!(
                            "`{}`.`{}` order col {} not found",
                            self.db,
                            self.tb,
                            order_col
                        );
                    }
                }
                extracted_count += 1;
                slice_count += 1;
                // sampling may be used in check scenario
                if extracted_count % self.sample_interval != 0 {
                    continue;
                }

                let row_data = RowData::from_mysql_row(&row, tb_meta, &ignore_cols);
                let position = tb_meta.basic.build_position(&DbType::Mysql, &start_values);
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
        mut resume_values: HashMap<String, ColValue>,
    ) -> anyhow::Result<u64> {
        let mut start_from_beginning = false;
        if resume_values.is_empty() {
            resume_values = tb_meta
                .basic
                .order_cols
                .iter()
                .map(|col| (col.clone(), ColValue::None))
                .collect();
            start_from_beginning = true;
        }

        let all_extracted_count = Arc::new(AtomicU64::new(0));
        let parallel_size = self.parallel_size;
        let batch_size = cmp::max(self.batch_size / parallel_size, 1);
        let router = Arc::new(self.base_extractor.router.clone());
        let ignore_cols = self.filter.get_ignore_cols(&self.db, &self.tb).cloned();
        let sql_1 = self.build_extract_sql(tb_meta, batch_size, 1)?;
        let sql_2 = self.build_extract_sql(tb_meta, batch_size, 2)?;
        let sql_3 = self.build_extract_sql(tb_meta, batch_size, 3)?;

        let partition_col = &tb_meta.basic.order_cols[0];
        let partition_col_type = tb_meta.get_col_type(partition_col)?;
        let mut start_value = resume_values
            .get(partition_col)
            .cloned()
            .unwrap_or(ColValue::None);

        loop {
            // send a checkpoint position before each loop
            self.send_checkpoint_position(partition_col, &start_value)
                .await?;

            let all_finished = Arc::new(AtomicBool::new(false));
            let last_partition_col_value = Arc::new(Mutex::new(ExtractColValue {
                value: start_value.clone(),
            }));

            if start_from_beginning {
                start_from_beginning = false;
                let mut slice_count = 0;
                let query = sqlx::query(&sql_1);
                let mut rows = query.fetch(&self.conn_pool);
                while let Some(row) = rows.try_next().await.unwrap() {
                    start_value = MysqlColValueConvertor::from_query(
                        &row,
                        partition_col,
                        partition_col_type,
                    )?;
                    let row_data = RowData::from_mysql_row(&row, tb_meta, &ignore_cols.as_ref());
                    let position =
                        Self::build_position(&self.db, &self.tb, partition_col, &start_value);
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
                    let partition_col = partition_col.to_string();
                    let partition_col_type = partition_col_type.clone();
                    let ignore_cols = ignore_cols.clone();

                    let all_extracted_count = all_extracted_count.clone();
                    let all_finished = all_finished.clone();
                    let last_partition_col_value = last_partition_col_value.clone();

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
                            .bind_col_value(Some(&sub_start_value), &partition_col_type);
                        if i < parallel_size - 1 {
                            query = query.bind_col_value(Some(&sub_end_value), &partition_col_type);
                        }
                        let mut rows = query.fetch(&conn_pool);

                        let mut partition_col_value = ColValue::None;
                        let mut slice_count = 0;
                        while let Some(row) = rows.try_next().await.unwrap() {
                            partition_col_value = MysqlColValueConvertor::from_query(
                                &row,
                                &partition_col,
                                &partition_col_type,
                            )?;

                            let row_data =
                                RowData::from_mysql_row(&row, &tb_meta, &ignore_cols.as_ref());
                            let position = Self::build_position(
                                &db,
                                &tb,
                                &partition_col,
                                &partition_col_value,
                            );
                            Self::push_row(&buffer, &router, row_data, position).await?;
                            slice_count += 1;
                        }

                        all_extracted_count.fetch_add(slice_count, Ordering::Release);
                        if i == parallel_size - 1 {
                            last_partition_col_value.lock().await.value = partition_col_value;
                            all_finished.store(slice_count < batch_size as u64, Ordering::Release);
                        }
                        Ok(())
                    });
                    futures.push(future);
                }

                for future in futures {
                    let _ = future.await.unwrap();
                }

                start_value = last_partition_col_value.lock().await.value.clone();
                if all_finished.load(Ordering::Acquire) {
                    break;
                }
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
                order_col_values: HashMap::new(),
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

    fn can_parallel_extract(&self, tb_meta: &MysqlTbMeta) -> anyhow::Result<bool> {
        Ok(tb_meta.basic.order_cols.len() == 1
            && self.parallel_size > 1
            && matches!(
                tb_meta.get_col_type(&tb_meta.basic.order_cols[0])?,
                MysqlColType::Int { .. }
                    | MysqlColType::BigInt { .. }
                    | MysqlColType::MediumInt { .. }
            ))
    }

    async fn get_resume_values(
        &self,
        tb_meta: &MysqlTbMeta,
        check_point: bool,
    ) -> anyhow::Result<HashMap<String, ColValue>> {
        let mut resume_values: HashMap<String, ColValue> = HashMap::new();
        if let Some(handler) = &self.recovery {
            for order_col in &tb_meta.basic.order_cols {
                if let Some(value) = handler
                    .get_snapshot_resume_position(&self.db, &self.tb, order_col, check_point)
                    .await
                {
                    resume_values.insert(
                        order_col.to_string(),
                        MysqlColValueConvertor::from_str(tb_meta.get_col_type(order_col)?, &value)?,
                    );
                }
            }
        }
        if resume_values.is_empty() || resume_values.len() != tb_meta.basic.order_cols.len() {
            log_info!(
                r#"`{}`.`{}` resume values not match order cols"#,
                self.db,
                self.tb
            );
            return Ok(HashMap::new());
        }
        log_info!(
            r#"[`{}`.`{}`] recovery from [{}]"#,
            self.db,
            self.tb,
            SerializeUtil::serialize_hashmap_to_json(&resume_values)?
        );
        Ok(resume_values)
    }

    fn build_extract_sql(
        &self,
        tb_meta: &MysqlTbMeta,
        batch_size: usize,
        sql_type: u8,
    ) -> anyhow::Result<String> {
        let cols_str = self.build_extract_cols_str(tb_meta)?;
        let condition_str = match sql_type {
            1 => "".to_string(),
            2 => Self::build_key_predicate_range(tb_meta)?,
            3 => Self::build_key_predicate_upper(tb_meta)?,
            _ => bail!("unsupported sql_type: {}", sql_type),
        };
        let where_sql =
            BaseExtractor::get_where_sql(&self.filter, &self.db, &self.tb, &condition_str);
        let sql = format!(
            "SELECT {} FROM `{}`.`{}` {} ORDER BY {} LIMIT {}",
            cols_str,
            self.db,
            self.tb,
            where_sql,
            MysqlSnapshotExtractor::build_order_by_clause(&tb_meta.basic)?,
            batch_size,
        );
        Ok(sql)
    }

    fn build_order_col_str(order_cols: &Vec<String>) -> String {
        order_cols
            .iter()
            .map(|col| format!(r#"`{}`"#, col))
            .collect::<Vec<String>>()
            .join(", ")
    }

    fn build_place_holder_str(order_cols: &Vec<String>) -> String {
        order_cols
            .iter()
            .map(|_| "?".to_string())
            .collect::<Vec<String>>()
            .join(", ")
    }

    fn build_key_predicate_range(tb_meta: &MysqlTbMeta) -> anyhow::Result<String> {
        let order_cols = &tb_meta.basic.order_cols;
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

    fn build_key_predicate_upper(tb_meta: &MysqlTbMeta) -> anyhow::Result<String> {
        let order_cols = &tb_meta.basic.order_cols;
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

    fn build_order_by_clause(tb_meta: &RdbTbMeta) -> anyhow::Result<String> {
        let order_cols = &tb_meta.order_cols;
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
}
