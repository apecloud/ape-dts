use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{MySql, Pool};
use tokio::task::JoinSet;

use crate::{
    extractor::{
        base_extractor::{BaseExtractor, ExtractState},
        base_splitter::SnapshotChunk,
        extractor_monitor::ExtractorMonitor,
        mysql::mysql_snapshot_splitter::MySqlSnapshotSplitter,
        rdb_snapshot_extract_statement::{OrderKeyPredicateType, RdbSnapshotExtractStatement},
        resumer::recovery::Recovery,
    },
    Extractor,
};
use dt_common::monitor::{monitor_task_id, task_monitor::TaskMonitorHandle};
use dt_common::utils::sql_util::MYSQL_ESCAPE;
use dt_common::{
    config::config_enums::{DbType, RdbParallelType},
    log_debug, log_info,
    meta::{
        adaptor::{mysql_col_value_convertor::MysqlColValueConvertor, sqlx_ext::SqlxMysqlExt},
        col_value::ColValue,
        dt_data::DtData,
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

use quote_mysql as quote;

pub struct MysqlSnapshotExtractor {
    pub base_extractor: BaseExtractor,
    pub extract_state: ExtractState,
    pub conn_pool: Pool<MySql>,
    pub meta_manager: MysqlMetaManager,
    pub filter: RdbFilter,
    pub batch_size: usize,
    pub parallel_size: usize,
    pub parallel_type: RdbParallelType,
    pub sample_interval: u64,
    pub db: String,
    pub tb: String,
    pub db_tbs: HashMap<String, Vec<String>>,
    pub user_defined_partition_col: String,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

#[derive(Clone)]
struct MysqlSnapshotShared {
    conn_pool: Pool<MySql>,
    meta_manager: MysqlMetaManager,
    filter: RdbFilter,
    batch_size: usize,
    parallel_size: usize,
    sample_interval: u64,
    recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

struct MysqlTableWorker {
    shared: MysqlSnapshotShared,
    base_extractor: BaseExtractor,
    extract_state: ExtractState,
    db: String,
    tb: String,
    user_defined_partition_col: String,
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
    pub base_extractor: BaseExtractor,
    pub extract_state: ExtractState,
}

struct TableMonitorGuard {
    handle: TaskMonitorHandle,
    task_id: String,
}

impl Drop for TableMonitorGuard {
    fn drop(&mut self) {
        self.handle.unregister_monitor(&self.task_id);
    }
}

#[async_trait]
impl Extractor for MysqlSnapshotExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        if self.parallel_size < 1 {
            bail!("parallel_size must be greater than 0");
        }

        let tables = self.collect_tables();
        log_info!(
            "MysqlSnapshotExtractor starts, tables: {}, parallel_type: {:?}, parallel_size: {}",
            tables.len(),
            self.parallel_type,
            self.parallel_size
        );

        match self.parallel_type {
            RdbParallelType::Table => self.extract_by_table_parallel(tables).await?,
            RdbParallelType::Chunk => self.extract_by_chunk_tables(tables).await?,
        }

        self.base_extractor
            .wait_task_finish(&mut self.extract_state)
            .await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl MysqlSnapshotExtractor {
    fn shared(&self) -> MysqlSnapshotShared {
        MysqlSnapshotShared {
            conn_pool: self.conn_pool.clone(),
            meta_manager: self.meta_manager.clone(),
            filter: self.filter.clone(),
            batch_size: self.batch_size,
            parallel_size: self.parallel_size,
            sample_interval: self.sample_interval,
            recovery: self.recovery.clone(),
        }
    }

    fn collect_tables(&self) -> Vec<(String, String)> {
        if !self.db.is_empty() && !self.tb.is_empty() {
            return vec![(self.db.clone(), self.tb.clone())];
        }

        let mut tables = Vec::new();
        for (db, tbs) in &self.db_tbs {
            for tb in tbs {
                tables.push((db.clone(), tb.clone()));
            }
        }
        tables
    }

    async fn run_table_worker(
        &self,
        db: String,
        tb: String,
        user_defined_partition_col: String,
    ) -> anyhow::Result<()> {
        let task_id = monitor_task_id::from_schema_tb(&db, &tb);
        let monitor_handle = self.extract_state.monitor.monitor.clone();
        let monitor = monitor_handle.build_monitor("extractor", &task_id);
        monitor_handle.register_monitor(&task_id, monitor.clone());
        let _guard = TableMonitorGuard {
            handle: monitor_handle.clone(),
            task_id: task_id.clone(),
        };

        let extractor_monitor = ExtractorMonitor::new(monitor_handle.clone(), task_id).await;
        let data_marker = self.extract_state.data_marker.clone();
        let extract_state = self
            .extract_state
            .derive_for_table(extractor_monitor, data_marker)
            .await;

        let mut worker = MysqlTableWorker {
            shared: self.shared(),
            base_extractor: self.base_extractor.clone(),
            extract_state,
            db,
            tb,
            user_defined_partition_col,
        };
        let res = worker
            .extract_single_table(matches!(self.parallel_type, RdbParallelType::Chunk))
            .await;
        worker.extract_state.monitor.try_flush(true).await;
        res
    }

    async fn extract_by_table_parallel(&self, tables: Vec<(String, String)>) -> anyhow::Result<()> {
        let mut join_set = JoinSet::new();
        let mut iter = tables.into_iter();

        while join_set.len() < self.parallel_size {
            let Some((db, tb)) = iter.next() else {
                break;
            };
            let partition_col = if self.db == db && self.tb == tb {
                self.user_defined_partition_col.clone()
            } else {
                String::new()
            };
            let this = self.clone_for_dispatch();
            join_set.spawn(async move { this.run_table_worker(db, tb, partition_col).await });
        }

        while let Some(result) = join_set.join_next().await {
            result.map_err(|e| anyhow!("mysql table worker join error: {}", e))??;

            if let Some((db, tb)) = iter.next() {
                let partition_col = if self.db == db && self.tb == tb {
                    self.user_defined_partition_col.clone()
                } else {
                    String::new()
                };
                let this = self.clone_for_dispatch();
                join_set.spawn(async move { this.run_table_worker(db, tb, partition_col).await });
            }
        }

        Ok(())
    }

    async fn extract_by_chunk_tables(&self, tables: Vec<(String, String)>) -> anyhow::Result<()> {
        for (db, tb) in tables {
            let partition_col = if self.db == db && self.tb == tb {
                self.user_defined_partition_col.clone()
            } else {
                String::new()
            };
            self.run_table_worker(db, tb, partition_col).await?;
        }
        Ok(())
    }

    fn clone_for_dispatch(&self) -> Self {
        Self {
            base_extractor: self.base_extractor.clone(),
            extract_state: ExtractState {
                monitor: ExtractorMonitor {
                    monitor: self.extract_state.monitor.monitor.clone(),
                    default_task_id: self.extract_state.monitor.default_task_id.clone(),
                    count_window: self.extract_state.monitor.count_window,
                    time_window_secs: self.extract_state.monitor.time_window_secs,
                    last_flush_time: tokio::time::Instant::now(),
                    flushed_counters: Default::default(),
                    counters: Default::default(),
                },
                data_marker: self.extract_state.data_marker.clone(),
                time_filter: self.extract_state.time_filter.clone(),
            },
            conn_pool: self.conn_pool.clone(),
            meta_manager: self.meta_manager.clone(),
            filter: self.filter.clone(),
            batch_size: self.batch_size,
            parallel_size: self.parallel_size,
            parallel_type: self.parallel_type.clone(),
            sample_interval: self.sample_interval,
            db: self.db.clone(),
            tb: self.tb.clone(),
            db_tbs: self.db_tbs.clone(),
            user_defined_partition_col: self.user_defined_partition_col.clone(),
            recovery: self.recovery.clone(),
        }
    }
}

impl MysqlTableWorker {
    async fn extract_single_table(&mut self, enable_chunk_parallel: bool) -> anyhow::Result<()> {
        log_info!(
            "MysqlSnapshotExtractor starts, schema: {}, tb: {}, batch_size: {}, parallel_size: {}",
            quote!(&self.db),
            quote!(&self.tb),
            self.shared.batch_size,
            self.shared.parallel_size
        );

        let mut tb_meta = self
            .shared
            .meta_manager
            .get_tb_meta(&self.db, &self.tb)
            .await?
            .to_owned();
        let user_defined_partition_col = &self.user_defined_partition_col;
        self.validate_user_defined(&mut tb_meta, user_defined_partition_col)?;
        let mut splitter = MySqlSnapshotSplitter::new(
            &tb_meta,
            self.shared.conn_pool.clone(),
            self.shared.batch_size,
            if !user_defined_partition_col.is_empty() {
                user_defined_partition_col.clone()
            } else {
                tb_meta.basic.partition_col.clone()
            },
        );

        let extracted_count = if enable_chunk_parallel {
            if user_defined_partition_col.is_empty()
                && self.shared.parallel_size <= 1
                && !tb_meta.basic.order_cols.is_empty()
            {
                self.serial_extract(&tb_meta).await?
            } else {
                self.parallel_extract_by_batch(&tb_meta, &mut splitter)
                    .await?
            }
        } else {
            self.serial_extract(&tb_meta).await?
        };

        log_info!(
            "end extracting data from {}.{}, all count: {}",
            quote!(&self.db),
            quote!(&self.tb),
            extracted_count
        );

        self.base_extractor.push_snapshot_finished(
            &self.db,
            &self.tb,
            Position::RdbSnapshotFinished {
                db_type: DbType::Mysql.to_string(),
                schema: self.db.clone(),
                tb: self.tb.clone(),
            },
        )?;
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
            quote!(&self.db),
            quote!(&self.tb)
        );

        let base_count = self.extract_state.monitor.counters.pushed_record_count;
        let ignore_cols = self.shared.filter.get_ignore_cols(&self.db, &self.tb);
        let where_condition = self
            .shared
            .filter
            .get_where_condition(&self.db, &self.tb)
            .cloned()
            .unwrap_or_default();
        let sql = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.unwrap_or(&HashSet::new()))
            .with_where_condition(&where_condition)
            .build()?;

        let mut rows = sqlx::query(&sql).fetch(&self.shared.conn_pool);
        while let Some(row) = rows.try_next().await? {
            let row_data = RowData::from_mysql_row(&row, tb_meta, &ignore_cols);
            self.base_extractor
                .push_row(&mut self.extract_state, row_data, Position::None)
                .await?;
        }
        Ok(self.extract_state.monitor.counters.pushed_record_count - base_count)
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
        let ignore_cols = self.shared.filter.get_ignore_cols(&self.db, &self.tb);
        let where_condition = self
            .shared
            .filter
            .get_where_condition(&self.db, &self.tb)
            .cloned()
            .unwrap_or_default();
        let sql_from_beginning = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.unwrap_or(&HashSet::new()))
            .with_order_cols(&tb_meta.basic.order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::None)
            .with_limit(self.shared.batch_size)
            .build()?;
        let sql_from_value = RdbSnapshotExtractStatement::from(tb_meta)
            .with_ignore_cols(ignore_cols.unwrap_or(&HashSet::new()))
            .with_order_cols(&tb_meta.basic.order_cols)
            .with_where_condition(&where_condition)
            .with_predicate_type(OrderKeyPredicateType::GreaterThan)
            .with_limit(self.shared.batch_size)
            .build()?;

        loop {
            let bind_values = start_values.clone();
            let query = if start_from_beginning {
                start_from_beginning = false;
                sqlx::query(&sql_from_beginning)
            } else {
                let mut query = sqlx::query(&sql_from_value);
                for order_col in &tb_meta.basic.order_cols {
                    let order_col_type = tb_meta.get_col_type(order_col)?;
                    query = query.bind_col_value(bind_values.get(order_col), order_col_type)
                }
                query
            };

            let mut rows = query.fetch(&self.shared.conn_pool);
            let mut slice_count = 0usize;

            while let Some(row) = rows.try_next().await? {
                for order_col in &tb_meta.basic.order_cols {
                    let order_col_type = tb_meta.get_col_type(order_col)?;
                    if let Some(value) = start_values.get_mut(order_col) {
                        *value =
                            MysqlColValueConvertor::from_query(&row, order_col, order_col_type)?;
                    } else {
                        bail!(
                            "{}.{} order col {} not found",
                            quote!(&self.db),
                            quote!(&self.tb),
                            quote!(order_col),
                        );
                    }
                }
                extracted_count += 1;
                slice_count += 1;
                if extracted_count % self.shared.sample_interval != 0 {
                    continue;
                }

                let row_data = RowData::from_mysql_row(&row, tb_meta, &ignore_cols);
                let position = tb_meta.basic.build_position(&DbType::Mysql, &start_values);
                self.base_extractor
                    .push_row(&mut self.extract_state, row_data, position)
                    .await?;
            }

            if slice_count < self.shared.batch_size {
                break;
            }
        }

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
        order_cols: &Vec<String>,
    ) -> anyhow::Result<u64> {
        let mut extracted_count = 0u64;
        let ignore_cols = self.shared.filter.get_ignore_cols(&self.db, &self.tb);
        let where_condition = self
            .shared
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

        let mut rows = sqlx::query(&sql_for_null).fetch(&self.shared.conn_pool);
        while let Some(row) = rows.try_next().await? {
            extracted_count += 1;
            let row_data = RowData::from_mysql_row(&row, tb_meta, &ignore_cols);
            self.base_extractor
                .push_row(&mut self.extract_state, row_data, Position::None)
                .await?;
        }
        Ok(extracted_count)
    }

    async fn parallel_extract_by_batch(
        &mut self,
        tb_meta: &MysqlTbMeta,
        splitter: &mut MySqlSnapshotSplitter<'_>,
    ) -> anyhow::Result<u64> {
        log_info!(
            "parallel extracting, parallel_size: {}",
            self.shared.parallel_size
        );
        let order_cols = vec![splitter.get_partition_col()];
        let partition_col = &order_cols[0];
        let partition_col_type = tb_meta.get_col_type(partition_col)?;
        let resume_values = self.get_resume_values(tb_meta, &order_cols, true).await?;
        splitter.init(&resume_values)?;

        let mut extract_cnt = 0u64;
        let monitor_handle = self.extract_state.monitor.monitor.clone();
        let task_id = self.extract_state.monitor.default_task_id.clone();
        let ignore_cols = self
            .shared
            .filter
            .get_ignore_cols(&self.db, &self.tb)
            .cloned();
        let where_condition = self
            .shared
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
        let mut parallel_cnt = 0usize;
        let mut join_set: JoinSet<anyhow::Result<(u64, u64, ColValue)>> = JoinSet::new();
        let mut pending_chunks = VecDeque::new();
        while parallel_cnt < self.shared.parallel_size {
            pending_chunks.extend(splitter.get_next_chunks().await?.into_iter());
            if let Some(chunk) = pending_chunks.pop_front() {
                if let (ColValue::None, ColValue::None) = &chunk.chunk_range {
                    if !join_set.is_empty() {
                        bail!(
                            "table {}.{} has no split chunk, but some parallel extractors are running",
                            quote!(&self.db),
                            quote!(&self.tb)
                        );
                    }
                    log_info!(
                        "table {}.{} has no split chunk, extracting by single batch extractor",
                        quote!(&self.db),
                        quote!(&self.tb)
                    );
                    return self.serial_extract(tb_meta).await;
                }

                Self::spawn_sub_parallel_extract(
                    ParallelExtractCtx {
                        conn_pool: &self.shared.conn_pool,
                        tb_meta,
                        partition_col,
                        partition_col_type,
                        sql_le: &sql_le,
                        sql_range: &sql_range,
                        chunk,
                        ignore_cols: &ignore_cols,
                        base_extractor: self.base_extractor.clone(),
                        extract_state: self
                            .extract_state
                            .derive_for_table(
                                ExtractorMonitor::new(monitor_handle.clone(), task_id.clone())
                                    .await,
                                self.extract_state.data_marker.clone(),
                            )
                            .await,
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
            if let Some(chunk) = if !pending_chunks.is_empty() {
                pending_chunks.pop_front()
            } else {
                pending_chunks.extend(splitter.get_next_chunks().await?.into_iter());
                pending_chunks.pop_front()
            } {
                Self::spawn_sub_parallel_extract(
                    ParallelExtractCtx {
                        conn_pool: &self.shared.conn_pool,
                        tb_meta,
                        partition_col,
                        partition_col_type,
                        sql_le: &sql_le,
                        sql_range: &sql_range,
                        chunk,
                        ignore_cols: &ignore_cols,
                        base_extractor: self.base_extractor.clone(),
                        extract_state: self
                            .extract_state
                            .derive_for_table(
                                ExtractorMonitor::new(monitor_handle.clone(), task_id.clone())
                                    .await,
                                self.extract_state.data_marker.clone(),
                            )
                            .await,
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
        let base_extractor = extract_ctx.base_extractor.clone();
        let mut extract_state = extract_ctx.extract_state;

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
                    MysqlColValueConvertor::from_query(&row, &partition_col, &partition_col_type)?;
                let row_data = RowData::from_mysql_row(&row, &tb_meta, &ignore_cols.as_ref());
                base_extractor
                    .push_row(&mut extract_state, row_data, Position::None)
                    .await?;
                extracted_cnt += 1;
            }
            extract_state.monitor.try_flush(true).await;
            Ok((chunk_id, extracted_cnt, partition_col_value))
        });
        Ok(())
    }

    async fn send_checkpoint_position(&mut self, position: Position) -> anyhow::Result<()> {
        let commit = DtData::Commit { xid: String::new() };
        self.base_extractor
            .push_dt_data(&mut self.extract_state, commit, position)
            .await?;
        Ok(())
    }

    fn validate_user_defined(
        &self,
        tb_meta: &mut MysqlTbMeta,
        user_defined_partition_col: &String,
    ) -> anyhow::Result<()> {
        if user_defined_partition_col.is_empty() {
            return Ok(());
        }
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
        &self,
        tb_meta: &MysqlTbMeta,
        order_cols: &[String],
        check_point: bool,
    ) -> anyhow::Result<HashMap<String, ColValue>> {
        let mut resume_values: HashMap<String, ColValue> = HashMap::new();
        if let Some(handler) = &self.shared.recovery {
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
                        quote!(&self.db),
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
                        quote!(&self.db),
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
                            quote!(&self.db),
                            quote!(&self.tb),
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
            quote!(&self.db),
            quote!(&self.tb),
            SerializeUtil::serialize_hashmap_to_json(&resume_values)?
        );
        Ok(resume_values)
    }
}
