use std::{
    cmp,
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use anyhow::bail;
use async_trait::async_trait;
use futures::{future::Join, TryStreamExt};
use sqlx::{MySql, Pool};
use tokio::{
    sync::Mutex,
    task::{JoinHandle, JoinSet},
};

use crate::{
    extractor::{
        base_extractor::BaseExtractor,
        mysql::mysql_snapshot_splitter::{MySqlSnapshotSplitter, SnapshotChunk},
        resumer::recovery::Recovery,
    },
    rdb_query_builder::RdbQueryBuilder,
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
        rdb_tb_meta::RdbTbMeta,
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
            quote_mysql!(self.db),
            quote_mysql!(self.tb),
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

        if Self::can_extract_by_batch(&tb_meta) {
            let mut splitter =
                MySqlSnapshotSplitter::new(&tb_meta, self.conn_pool.clone(), self.batch_size);
            let parallel_extract = self.parallel_size > 1 && splitter.can_be_splitted().await?;
            let resume_values = self.get_resume_values(&tb_meta, parallel_extract).await?;
            extracted_count = if parallel_extract {
                log_info!("parallel extracting, parallel_size: {}", self.parallel_size);
                splitter.init(&resume_values)?;
                self.parallel_extract_by_batch(&tb_meta, &mut splitter)
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
            resume_values = tb_meta.basic.get_default_order_col_values();
            start_from_beginning = true;
        }
        let mut extracted_count = 0u64;
        let mut start_values = resume_values;
        let sql_from_beginning = Self::with_limit(
            self.build_extract_sql(tb_meta, &tb_meta.basic.order_cols, 1)?,
            self.batch_size,
        );
        let sql_from_value = Self::with_limit(
            self.build_extract_sql(tb_meta, &tb_meta.basic.order_cols, 3)?,
            self.batch_size,
        );
        let ignore_cols = self.filter.get_ignore_cols(&self.db, &self.tb);

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
                            quote_mysql!(self.db),
                            quote_mysql!(self.tb),
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
        if tb_meta.basic.order_cols_are_nullable {
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
        let sql_for_null = self.build_extract_null_sql(tb_meta, order_cols, true)?;
        let ignore_cols = self.filter.get_ignore_cols(&self.db, &self.tb);

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
        let mut extract_cnt = 0u64;
        let parallel_size = self.parallel_size;
        let router = Arc::new(self.base_extractor.router.clone());
        let ignore_cols = self.filter.get_ignore_cols(&self.db, &self.tb).cloned();
        let sql_le = self.build_extract_sql(tb_meta, &tb_meta.basic.order_cols, 4)?;
        let sql_range = self.build_extract_sql(tb_meta, &tb_meta.basic.order_cols, 2)?;
        let partition_col = &tb_meta.basic.partition_col;
        let partition_col_type = tb_meta.get_col_type(partition_col)?;

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
                            quote_mysql!(self.db),
                            quote_mysql!(self.tb)
                        );
                    }
                    // no split
                    log_info!(
                        "table {}.{} has no split chunk",
                        quote_mysql!(self.db),
                        quote_mysql!(self.tb)
                    );
                    let resume_values = self.get_resume_values(tb_meta, false).await?;
                    return self.extract_by_batch(tb_meta, resume_values).await;
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
            extract_cnt += self
                .extract_nulls(tb_meta, &tb_meta.basic.order_cols)
                .await?;
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
                        quote_mysql!(tb_meta.basic.schema),
                        quote_mysql!(tb_meta.basic.tb)
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

    fn build_extract_cols_str(&self, tb_meta: &MysqlTbMeta) -> anyhow::Result<String> {
        let ignore_cols = self.filter.get_ignore_cols(&self.db, &self.tb);
        let query_builder = RdbQueryBuilder::new_for_mysql(tb_meta, ignore_cols);
        query_builder.build_extract_cols_str()
    }

    #[inline(always)]
    pub fn can_extract_by_batch(tb_meta: &MysqlTbMeta) -> bool {
        !tb_meta.basic.order_cols.is_empty()
    }

    fn build_null_predicate(
        tb_meta: &RdbTbMeta,
        order_cols: &[String],
        is_null: bool,
    ) -> anyhow::Result<String> {
        let null_check = if is_null { "IS NULL" } else { "IS NOT NULL" };
        let join_str = if is_null { "OR" } else { "AND" };
        if order_cols.is_empty() {
            bail!("order cols is empty");
        } else {
            // col_1 IS NOT NULL AND col_2 IS NOT NULL AND col_3 IS NOT NULL
            // col_1 IS NULL OR col_2 IS NULL OR col_3 IS NULL
            Ok(order_cols
                .iter()
                .filter(|&col| tb_meta.is_col_nullable(col))
                .map(|col| format!(r#"`{}` {}"#, col, null_check))
                .collect::<Vec<String>>()
                .join(&format!(" {} ", join_str)))
        }
    }

    async fn get_resume_values(
        &self,
        tb_meta: &MysqlTbMeta,
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
                        r#"`{}`.`{}` resume position db/tb not match, ignore it"#,
                        self.db,
                        self.tb
                    );
                    return Ok(HashMap::new());
                }
                let order_col_values = match order_key {
                    OrderKey::Single((order_col, value)) => vec![(order_col, value)],
                    OrderKey::Composite(values) => values,
                };
                if order_col_values.len() != tb_meta.basic.order_cols.len() {
                    log_info!(
                        r#"`{}`.`{}` resume values not match order cols in length"#,
                        self.db,
                        self.tb
                    );
                    return Ok(HashMap::new());
                }
                for ((position_order_col, value), order_col) in order_col_values
                    .into_iter()
                    .zip(tb_meta.basic.order_cols.iter())
                {
                    if position_order_col != *order_col {
                        log_info!(
                            r#"`{}`.`{}` resume position order col {} not match {}"#,
                            self.db,
                            self.tb,
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
        order_cols: &[String],
        sql_type: u8,
    ) -> anyhow::Result<String> {
        let cols_str = self.build_extract_cols_str(tb_meta)?;
        let mut condition_str = match sql_type {
            1 => "".to_string(),
            2 => Self::build_order_col_predicate_range(order_cols)?,
            3 => Self::build_order_col_predicate_gt(order_cols)?,
            4 => Self::build_order_col_predicate_le(order_cols)?,
            _ => bail!("unsupported sql_type: {}", sql_type),
        };

        if order_cols
            .iter()
            .any(|col| tb_meta.basic.is_col_nullable(col))
        {
            let not_null_predicate = Self::build_null_predicate(&tb_meta.basic, order_cols, false)?;
            if !not_null_predicate.is_empty() {
                condition_str = if condition_str.is_empty() {
                    not_null_predicate
                } else {
                    format!("{} AND {}", condition_str, not_null_predicate)
                };
            }
        }
        let where_sql =
            BaseExtractor::get_where_sql(&self.filter, &self.db, &self.tb, &condition_str);
        let sql = format!(
            "SELECT {} FROM `{}`.`{}` {} ORDER BY {}",
            cols_str,
            self.db,
            self.tb,
            where_sql,
            Self::build_order_by_clause(&tb_meta.basic.order_cols)?,
        );
        Ok(sql)
    }

    fn with_limit(mut sql: String, limit_size: usize) -> String {
        sql.push_str(&format!(" LIMIT {}", limit_size));
        sql
    }

    fn build_extract_null_sql(
        &self,
        tb_meta: &MysqlTbMeta,
        order_cols: &[String],
        is_null: bool,
    ) -> anyhow::Result<String> {
        let cols_str = self.build_extract_cols_str(tb_meta)?;
        let order_by_clause = Self::build_order_by_clause(&tb_meta.basic.order_cols)?;
        let null_predicate = Self::build_null_predicate(&tb_meta.basic, order_cols, is_null)?;
        let where_sql =
            BaseExtractor::get_where_sql(&self.filter, &self.db, &self.tb, &null_predicate);

        Ok(format!(
            r#"SELECT {} FROM `{}`.`{}` {} ORDER BY {}"#,
            cols_str, self.db, self.tb, where_sql, order_by_clause
        ))
    }

    #[inline(always)]
    fn build_order_col_str(order_cols: &[String]) -> String {
        order_cols
            .iter()
            .map(|col| format!(r#"`{}`"#, col))
            .collect::<Vec<String>>()
            .join(", ")
    }

    #[inline(always)]
    fn build_place_holder_str(order_cols: &[String]) -> String {
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
}
