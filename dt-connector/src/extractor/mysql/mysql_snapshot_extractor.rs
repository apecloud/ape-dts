use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use anyhow::bail;
use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{MySql, Pool};
use tokio::task::JoinSet;

use crate::{
    extractor::{
        base_extractor::BaseExtractor,
        mysql::mysql_snapshot_splitter::{MySqlSnapshotSplitter, SnapshotChunk},
        rdb_snapshot_extract_statement::{OrderKeyPredicateType, RdbSnapshotExtractStatement},
        resumer::recovery::Recovery,
    },
    rdb_router::RdbRouter,
    Extractor,
};
use dt_common::utils::sql_util::MYSQL_ESCAPE;
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
        order_key::OrderKey,
        position::Position,
        row_data::RowData,
    },
    quote_mysql,
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
    pub sample_interval: u64,
    pub db: String,
    pub tb: String,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

struct ParallelExtractCtx<'a> {
    pub conn_pool: &'a Pool<MySql>,
    pub tb_meta: &'a MysqlTbMeta,
    pub partition_col: &'a String,
    pub partition_col_type: &'a MysqlColType,
    pub sql_le: &'a String,
    pub sql_range: &'a String,
    pub chunk: SnapshotChunk,
    pub ignore_cols: &'a Option<HashSet<String>>,
    pub buffer: &'a Arc<DtQueue>,
    pub router: &'a Arc<RdbRouter>,
}

#[async_trait]
impl Extractor for MysqlSnapshotExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        log_info!(
            "MysqlSnapshotExtractor starts, schema: {}, tb: {}, batch_size: {}, parallel_size: {}",
            quote_mysql!(&self.db),
            quote_mysql!(&self.tb),
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
        let mut tb_meta = self
            .meta_manager
            .get_tb_meta(&self.db, &self.tb)
            .await?
            .to_owned();
        // TODO(wl): get user defined partition col from config
        let user_defined_partition_col = "".to_string();
        self.validate_user_defined(&mut tb_meta, &user_defined_partition_col)?;
        let mut splitter = MySqlSnapshotSplitter::new(
            &tb_meta,
            self.conn_pool.clone(),
            self.batch_size,
            if !user_defined_partition_col.is_empty() {
                user_defined_partition_col.clone()
            } else {
                tb_meta.basic.partition_col.clone()
            },
        );
        let extracted_count = if user_defined_partition_col.is_empty()
            && self.parallel_size <= 1
            && !tb_meta.basic.order_cols.is_empty()
        {
            self.serial_extract(&tb_meta).await?
        } else {
            self.parallel_extract_by_batch(&tb_meta, &mut splitter)
                .await?
        };

        log_info!(
            "end extracting data from {}.{}, all count: {}",
            quote_mysql!(&self.db),
            quote_mysql!(&self.tb),
            extracted_count
        );
        Ok(())
    }

    async fn serial_extract(&mut self, tb_meta: &MysqlTbMeta) -> anyhow::Result<u64> {
        if !tb_meta.basic.order_cols.is_empty() {
            let resume_values = self
                .get_resume_values(tb_meta, &tb_meta.basic.order_cols, false)
                .await?;
            self.extract_by_batch(tb_meta, resume_values).await
        } else {
            self.extract_all(tb_meta).await
        }
    }

    async fn extract_all(&mut self, tb_meta: &MysqlTbMeta) -> anyhow::Result<u64> {
        log_info!(
            "start extracting data from {}.{} without batch",
            quote_mysql!(&self.db),
            quote_mysql!(&self.tb)
        );

        let ignore_cols = self.filter.get_ignore_cols(&self.db, &self.tb);
        let where_condition = self
            .filter
            .get_where_condition(&self.db, &self.tb)
            .cloned()
            .unwrap_or_default();
        let sql = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.unwrap_or(&HashSet::new()))
            .with_where_condition(&where_condition)
            .build()?;

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
            resume_values = tb_meta.basic.get_default_order_col_values();
            start_from_beginning = true;
        }
        let mut extracted_count = 0u64;
        let mut start_values = resume_values;
        let ignore_cols = self.filter.get_ignore_cols(&self.db, &self.tb);
        let where_condition = self
            .filter
            .get_where_condition(&self.db, &self.tb)
            .cloned()
            .unwrap_or_default();
        let sql_from_beginning = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.unwrap_or(&HashSet::new()))
            .with_order_cols(&tb_meta.basic.order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::None)
            .with_limit(self.batch_size)
            .build()?;
        let sql_from_value = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.unwrap_or(&HashSet::new()))
            .with_order_cols(&tb_meta.basic.order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::GreaterThan)
            .with_limit(self.batch_size)
            .build()?;
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
                            "{}.{} order col {} not found",
                            quote_mysql!(&self.db),
                            quote_mysql!(&self.tb),
                            quote_mysql!(order_col),
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

        // extract rows with NULL
        if tb_meta
            .basic
            .order_cols
            .iter()
            .any(|col| tb_meta.basic.is_col_nullable(col))
        {
            extracted_count += self
                .extract_nulls(tb_meta, &tb_meta.basic.order_cols)
                .await?;
        }

        Ok(extracted_count)
    }

    async fn extract_nulls(
        &mut self,
        tb_meta: &MysqlTbMeta,
        order_cols: &[String],
    ) -> anyhow::Result<u64> {
        let mut extracted_count = 0u64;
        let ignore_cols = self.filter.get_ignore_cols(&self.db, &self.tb);
        let where_condition = self
            .filter
            .get_where_condition(&self.db, &self.tb)
            .cloned()
            .unwrap_or_default();
        let sql_for_null = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.unwrap_or(&HashSet::new()))
            .with_order_cols(order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::IsNull)
            .build()?;

        let mut rows = sqlx::query(&sql_for_null).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await? {
            extracted_count += 1;
            let row_data = RowData::from_mysql_row(&row, tb_meta, &ignore_cols);
            self.base_extractor
                .push_row(row_data, Position::None)
                .await?;
        }
        Ok(extracted_count)
    }

    async fn parallel_extract_by_batch(
        &mut self,
        tb_meta: &MysqlTbMeta,
        splitter: &mut MySqlSnapshotSplitter<'_>,
    ) -> anyhow::Result<u64> {
        log_info!("parallel extracting, parallel_size: {}", self.parallel_size);
        let order_cols = vec![splitter.get_partition_col()];
        let partition_col = &order_cols[0];
        let partition_col_type = tb_meta.get_col_type(partition_col)?;
        let resume_values = self.get_resume_values(tb_meta, &order_cols, true).await?;
        splitter.init(&resume_values)?;

        let mut extract_cnt = 0u64;
        let parallel_size = self.parallel_size;
        let router = Arc::new(self.base_extractor.router.clone());
        let ignore_cols = self.filter.get_ignore_cols(&self.db, &self.tb).cloned();
        let where_condition = self
            .filter
            .get_where_condition(&self.db, &self.tb)
            .cloned()
            .unwrap_or_default();
        let sql_le = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.as_ref().unwrap_or(&HashSet::new()))
            .with_order_cols(&order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::LessThanOrEqual)
            .build()?;
        let sql_range = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.as_ref().unwrap_or(&HashSet::new()))
            .with_order_cols(&order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::Range)
            .build()?;
        let mut parallel_cnt = 0;
        let mut join_set: JoinSet<anyhow::Result<(u64, u64, ColValue)>> = JoinSet::new();
        let mut pending_chunks = VecDeque::new();
        while parallel_cnt < parallel_size {
            pending_chunks.extend(splitter.get_next_chunks().await?.into_iter());
            if let Some(chunk) = pending_chunks.pop_front() {
                if let (ColValue::None, ColValue::None) = &chunk.chunk_range {
                    if !join_set.is_empty() {
                        bail!(
                            "table {}.{} has no split chunk, but some parallel extractors are running",
                            quote_mysql!(&self.db),
                            quote_mysql!(&self.tb)
                        );
                    }
                    // no split
                    log_info!(
                        "table {}.{} has no split chunk, extracting by single batch extractor",
                        quote_mysql!(&self.db),
                        quote_mysql!(&self.tb)
                    );
                    return self.serial_extract(tb_meta).await;
                }

                Self::spawn_sub_parallel_extract(
                    ParallelExtractCtx {
                        conn_pool: &self.conn_pool,
                        tb_meta,
                        partition_col,
                        partition_col_type,
                        sql_le: &sql_le,
                        sql_range: &sql_range,
                        chunk,
                        ignore_cols: &ignore_cols,
                        buffer: &self.base_extractor.buffer,
                        router: &router,
                    },
                    &mut join_set,
                )
                .await?;
                parallel_cnt += 1;
            } else {
                break;
            }
        }

        while let Some(res) = join_set.join_next().await {
            let (chunk_id, cnt, partition_col_value) = res??;
            if let Some(position) =
                splitter.get_next_checkpoint_position(chunk_id, partition_col_value)
            {
                self.send_checkpoint_position(position).await?;
            }
            extract_cnt += cnt;
            // spawn new extract task
            if let Some(chunk) = if !pending_chunks.is_empty() {
                pending_chunks.pop_front()
            } else {
                pending_chunks.extend(splitter.get_next_chunks().await?.into_iter());
                pending_chunks.pop_front()
            } {
                Self::spawn_sub_parallel_extract(
                    ParallelExtractCtx {
                        conn_pool: &self.conn_pool,
                        tb_meta,
                        partition_col,
                        partition_col_type,
                        sql_le: &sql_le,
                        sql_range: &sql_range,
                        chunk,
                        ignore_cols: &ignore_cols,
                        buffer: &self.base_extractor.buffer,
                        router: &router,
                    },
                    &mut join_set,
                )
                .await?;
            }
        }

        if tb_meta.basic.is_col_nullable(partition_col) {
            extract_cnt += self.extract_nulls(tb_meta, &order_cols).await?;
        }
        Ok(extract_cnt)
    }

    async fn spawn_sub_parallel_extract(
        extract_ctx: ParallelExtractCtx<'_>,
        join_set: &mut JoinSet<anyhow::Result<(u64, u64, ColValue)>>,
    ) -> anyhow::Result<()> {
        let conn_pool = extract_ctx.conn_pool.clone();
        let tb_meta = extract_ctx.tb_meta.clone();
        let partition_col = extract_ctx.partition_col.clone();
        let partition_col_type = extract_ctx.partition_col_type.clone();
        let sql_le = extract_ctx.sql_le.clone();
        let sql_range = extract_ctx.sql_range.clone();
        let chunk = extract_ctx.chunk;
        let ignore_cols = extract_ctx.ignore_cols.clone();
        let router = extract_ctx.router.clone();
        let buffer = extract_ctx.buffer.clone();

        join_set.spawn(async move {
            let chunk_id = chunk.chunk_id;
            let (start_value, end_value) = chunk.chunk_range;
            let query = match (&start_value, &end_value) {
                (ColValue::None, ColValue::None) | (_, ColValue::None) => {
                    bail!(
                        "chunk {} has bad chunk range from {}.{}",
                        chunk_id,
                        quote_mysql!(&tb_meta.basic.schema),
                        quote_mysql!(&tb_meta.basic.tb)
                    );
                }
                (ColValue::None, _) => {
                    sqlx::query(&sql_le).bind_col_value(Some(&end_value), &partition_col_type)
                }
                _ => sqlx::query(&sql_range)
                    .bind_col_value(Some(&start_value), &partition_col_type)
                    .bind_col_value(Some(&end_value), &partition_col_type),
            };

            let mut extracted_cnt = 0u64;
            let mut partition_col_value = ColValue::None;
            let mut rows = query.fetch(&conn_pool);
            while let Some(row) = rows.try_next().await.unwrap() {
                partition_col_value =
                    MysqlColValueConvertor::from_query(&row, &partition_col, &partition_col_type)?;
                let row_data = RowData::from_mysql_row(&row, &tb_meta, &ignore_cols.as_ref());
                let position = tb_meta.basic.build_position_for_partition(
                    &DbType::Mysql,
                    &partition_col,
                    &partition_col_value,
                );
                Self::push_row(&buffer, &router, row_data, position).await?;
                extracted_cnt += 1;
            }
            Ok((chunk_id, extracted_cnt, partition_col_value))
        });
        Ok(())
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

    async fn send_checkpoint_position(&mut self, position: Position) -> anyhow::Result<()> {
        let commit = DtData::Commit { xid: String::new() };
        self.base_extractor.push_dt_data(commit, position).await?;
        Ok(())
    }

    pub fn validate_user_defined(
        &self,
        tb_meta: &mut MysqlTbMeta,
        user_defined_partition_col: &String,
    ) -> anyhow::Result<()> {
        if user_defined_partition_col.is_empty() {
            return Ok(());
        }
        // if user defined partition col is set, use it
        if tb_meta.basic.has_col(user_defined_partition_col) {
            return Ok(());
        }
        bail!(
            "user defined partition col {} not in cols of {}.{}",
            quote_mysql!(user_defined_partition_col),
            quote_mysql!(&tb_meta.basic.schema),
            quote_mysql!(&tb_meta.basic.tb),
        );
    }

    async fn get_resume_values(
        &self,
        tb_meta: &MysqlTbMeta,
        order_cols: &Vec<String>,
        check_point: bool,
    ) -> anyhow::Result<HashMap<String, ColValue>> {
        let mut resume_values: HashMap<String, ColValue> = HashMap::new();
        if let Some(handler) = &self.recovery {
            if let Some(Position::RdbSnapshot {
                schema,
                tb,
                order_key: Some(order_key),
                ..
            }) = handler
                .get_snapshot_resume_position(&self.db, &self.tb, check_point)
                .await
            {
                if schema != self.db || tb != self.tb {
                    log_info!(
                        r#"{}.{} resume position db/tb not match, ignore it"#,
                        quote_mysql!(&self.db),
                        quote_mysql!(&self.tb)
                    );
                    return Ok(HashMap::new());
                }
                let order_col_values = match order_key {
                    OrderKey::Single((order_col, value)) => vec![(order_col, value)],
                    OrderKey::Composite(values) => values,
                };
                if order_col_values.len() != order_cols.len() {
                    log_info!(
                        r#"{}.{} resume values not match order cols in length"#,
                        quote_mysql!(&self.db),
                        quote_mysql!(&self.tb)
                    );
                    return Ok(HashMap::new());
                }
                for ((position_order_col, value), order_col) in
                    order_col_values.into_iter().zip(order_cols.iter())
                {
                    if position_order_col != *order_col {
                        log_info!(
                            r#"{}.{} resume position order col {} not match {}"#,
                            quote_mysql!(&self.db),
                            quote_mysql!(&self.tb),
                            position_order_col,
                            order_col
                        );
                        return Ok(HashMap::new());
                    }
                    let col_value = match value {
                        Some(v) => {
                            MysqlColValueConvertor::from_str(tb_meta.get_col_type(order_col)?, &v)?
                        }
                        None => ColValue::None,
                    };
                    resume_values.insert(position_order_col, col_value);
                }
            } else {
                log_info!(r#"`{}`.`{}` has no resume position"#, self.db, self.tb);
                return Ok(HashMap::new());
            }
        }
        log_info!(
            r#"[{}.{}] recovery from [{}]"#,
            quote_mysql!(&self.db),
            quote_mysql!(&self.tb),
            SerializeUtil::serialize_hashmap_to_json(&resume_values)?
        );
        Ok(resume_values)
    }
}
