use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use anyhow::bail;
use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{Pool, Postgres};
use tokio::task::JoinSet;

use crate::{
    extractor::{
        base_extractor::BaseExtractor,
        base_splitter::SnapshotChunk,
        pg::pg_snapshot_splitter::PgSnapshotSplitter,
        rdb_snapshot_extract_statement::{OrderKeyPredicateType, RdbSnapshotExtractStatement},
        resumer::recovery::Recovery,
    },
    rdb_router::RdbRouter,
    Extractor,
};
use dt_common::utils::sql_util::PG_ESCAPE;
use dt_common::{
    config::config_enums::DbType,
    log_debug, log_info,
    meta::{
        adaptor::{pg_col_value_convertor::PgColValueConvertor, sqlx_ext::SqlxPgExt},
        col_value::ColValue,
        dt_data::{DtData, DtItem},
        dt_queue::DtQueue,
        order_key::OrderKey,
        pg::{pg_col_type::PgColType, pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
        position::Position,
        row_data::RowData,
    },
    quote_pg,
    rdb_filter::RdbFilter,
    utils::serialize_util::SerializeUtil,
};

use quote_pg as quote;

pub struct PgSnapshotExtractor {
    pub base_extractor: BaseExtractor,
    pub conn_pool: Pool<Postgres>,
    pub meta_manager: PgMetaManager,
    pub filter: RdbFilter,
    pub batch_size: usize,
    pub parallel_size: usize,
    pub sample_interval: u64,
    pub schema: String,
    pub tb: String,
    pub user_defined_partition_col: String,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

struct ParallelExtractCtx<'a> {
    pub conn_pool: &'a Pool<Postgres>,
    pub tb_meta: &'a PgTbMeta,
    pub partition_col: &'a String,
    pub partition_col_type: &'a PgColType,
    pub sql_le: &'a String,
    pub sql_range: &'a String,
    pub chunk: SnapshotChunk,
    pub ignore_cols: &'a Option<HashSet<String>>,
    pub buffer: &'a Arc<DtQueue>,
    pub router: &'a Arc<RdbRouter>,
}

#[async_trait]
impl Extractor for PgSnapshotExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        log_info!(
            "PgSnapshotExtractor starts, schema: {}, tb: {}, batch_size: {}, parallel_size: {}",
            quote!(&self.schema),
            quote!(&self.tb),
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

impl PgSnapshotExtractor {
    async fn extract_internal(&mut self) -> anyhow::Result<()> {
        let mut tb_meta = self
            .meta_manager
            .get_tb_meta(&self.schema, &self.tb)
            .await?
            .to_owned();
        let user_defined_partition_col = &self.user_defined_partition_col;
        self.validate_user_defined(&mut tb_meta, user_defined_partition_col)?;
        let mut splitter = PgSnapshotSplitter::new(
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
            quote!(&self.schema),
            quote!(&self.tb),
            extracted_count
        );
        Ok(())
    }

    async fn serial_extract(&mut self, tb_meta: &PgTbMeta) -> anyhow::Result<u64> {
        if !tb_meta.basic.order_cols.is_empty() {
            let resume_values = self
                .get_resume_values(tb_meta, &tb_meta.basic.order_cols, false)
                .await?;
            self.extract_by_batch(tb_meta, resume_values).await
        } else {
            self.extract_all(tb_meta).await
        }
    }

    async fn extract_all(&mut self, tb_meta: &PgTbMeta) -> anyhow::Result<u64> {
        log_info!(
            "start extracting data from {}.{} without batch",
            quote!(&self.schema),
            quote!(&self.tb)
        );

        let ignore_cols = self.filter.get_ignore_cols(&self.schema, &self.tb);
        let where_condition = self
            .filter
            .get_where_condition(&self.schema, &self.tb)
            .cloned()
            .unwrap_or_default();
        let sql = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.unwrap_or(&HashSet::new()))
            .with_where_condition(&where_condition)
            .build()?;

        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await? {
            let row_data = RowData::from_pg_row(&row, tb_meta, &ignore_cols);
            self.base_extractor
                .push_row(row_data, Position::None)
                .await?;
        }
        Ok(self.base_extractor.monitor.counters.pushed_record_count)
    }

    async fn extract_by_batch(
        &mut self,
        tb_meta: &PgTbMeta,
        mut resume_values: HashMap<String, ColValue>,
    ) -> anyhow::Result<u64> {
        let mut start_from_beginning = false;
        if resume_values.is_empty() {
            resume_values = tb_meta.basic.get_default_order_col_values();
            start_from_beginning = true;
        }
        let mut extracted_count = 0u64;
        let mut start_values = resume_values;
        let ignore_cols = self.filter.get_ignore_cols(&self.schema, &self.tb);
        let where_condition = self
            .filter
            .get_where_condition(&self.schema, &self.tb)
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

            while let Some(row) = rows.try_next().await? {
                for order_col in tb_meta.basic.order_cols.iter() {
                    let order_col_type = tb_meta.get_col_type(order_col)?;
                    if let Some(value) = start_values.get_mut(order_col) {
                        *value = PgColValueConvertor::from_query(&row, order_col, order_col_type)?;
                    } else {
                        bail!(
                            "{}.{} order col {} not found",
                            quote!(&self.schema),
                            quote!(&self.tb),
                            quote!(order_col),
                        );
                    }
                }
                extracted_count += 1;
                slice_count += 1;
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
        tb_meta: &PgTbMeta,
        order_cols: &Vec<String>,
    ) -> anyhow::Result<u64> {
        let mut extracted_count = 0u64;
        let ignore_cols = self.filter.get_ignore_cols(&self.schema, &self.tb);
        let where_condition = self
            .filter
            .get_where_condition(&self.schema, &self.tb)
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
            let row_data = RowData::from_pg_row(&row, tb_meta, &ignore_cols);
            self.base_extractor
                .push_row(row_data, Position::None)
                .await?;
        }
        Ok(extracted_count)
    }

    async fn parallel_extract_by_batch(
        &mut self,
        tb_meta: &PgTbMeta,
        splitter: &mut PgSnapshotSplitter<'_>,
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
        let ignore_cols = self.filter.get_ignore_cols(&self.schema, &self.tb).cloned();
        let where_condition = self
            .filter
            .get_where_condition(&self.schema, &self.tb)
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
                            quote!(&self.schema),
                            quote!(&self.tb)
                        );
                    }
                    // no split
                    log_info!(
                        "table {}.{} has no split chunk, extracting by single batch extractor",
                        quote!(&self.schema),
                        quote!(&self.tb)
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

        log_debug!(
            "extract by partition_col: {}, chunk range: {:?}",
            quote!(partition_col),
            chunk
        );
        join_set.spawn(async move {
            let chunk_id = chunk.chunk_id;
            let (start_value, end_value) = chunk.chunk_range;
            let query = match (&start_value, &end_value) {
                (ColValue::None, ColValue::None) | (_, ColValue::None) => {
                    bail!(
                        "chunk {} has bad chunk range from {}.{}",
                        chunk_id,
                        quote!(&tb_meta.basic.schema),
                        quote!(&tb_meta.basic.tb)
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
            while let Some(row) = rows.try_next().await? {
                partition_col_value =
                    PgColValueConvertor::from_query(&row, &partition_col, &partition_col_type)?;
                let row_data = RowData::from_pg_row(&row, &tb_meta, &ignore_cols.as_ref());
                Self::push_row(&buffer, &router, row_data, Position::None).await?;
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
        tb_meta: &mut PgTbMeta,
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
            quote!(user_defined_partition_col),
            quote!(&tb_meta.basic.schema),
            quote!(&tb_meta.basic.tb),
        );
    }

    async fn get_resume_values(
        &mut self,
        tb_meta: &PgTbMeta,
        order_cols: &[String],
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
                .get_snapshot_resume_position(&self.schema, &self.tb, check_point)
                .await
            {
                if schema != self.schema || tb != self.tb {
                    log_info!(
                        r#"{}.{} resume position schema/tb not match, ignore it"#,
                        quote!(&self.schema),
                        quote!(&self.tb)
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
                        quote!(&self.schema),
                        quote!(&self.tb)
                    );
                    return Ok(HashMap::new());
                }
                for ((position_order_col, value), order_col) in
                    order_col_values.into_iter().zip(order_cols.iter())
                {
                    if position_order_col != *order_col {
                        log_info!(
                            r#"{}.{} resume position order col {} not match {}"#,
                            quote!(&self.schema),
                            quote!(&self.tb),
                            position_order_col,
                            order_col
                        );
                        return Ok(HashMap::new());
                    }
                    let col_value = match value {
                        Some(v) => PgColValueConvertor::from_str(
                            tb_meta.get_col_type(order_col)?,
                            &v,
                            &mut self.meta_manager,
                        )?,
                        None => ColValue::None,
                    };
                    resume_values.insert(position_order_col, col_value);
                }
            } else {
                log_info!(
                    r#"{}.{} has no resume position"#,
                    quote!(&self.schema),
                    quote!(&self.tb)
                );
                return Ok(HashMap::new());
            }
        }
        log_info!(
            r#"[{}.{}] recovery from [{}]"#,
            quote!(&self.schema),
            quote!(&self.tb),
            SerializeUtil::serialize_hashmap_to_json(&resume_values)?
        );
        Ok(resume_values)
    }
}
