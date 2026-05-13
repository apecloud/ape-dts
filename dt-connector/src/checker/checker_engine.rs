use anyhow::Context;
use mongodb::bson::Document;
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use tokio::time::{sleep, Duration, Instant};

use super::cdc_state::build_identity_key;
use super::{
    CheckContext, CheckEntry, CheckInconsistency, Checker, CheckerStoreKey, CheckerTbMeta,
    DataChecker, RecheckKey, RetryItem,
};
use crate::checker::check_log::{CheckLog, CheckLogJsonExt, DiffColValue};
use crate::sinker::base_sinker::BaseSinker;
use crate::sinker::mongo::mongo_cmd;
use dt_common::meta::{
    col_value::ColValue, mongo::mongo_constant::MongoConstants, pg::pg_value_type::PgValueType,
    rdb_tb_meta::RdbTbMeta, row_data::RowData, row_type::RowType,
};
use dt_common::{
    log_diff, log_miss, log_sql, log_warn,
    monitor::{counter_type::CounterType, task_metrics::TaskMetricsType},
    utils::limit_queue::LimitedQueue,
};

struct PreparedCheckRow<'a> {
    row: &'a RowData,
    key: u128,
}

impl<C: Checker> DataChecker<C> {
    const MAX_DIFF_COLS: usize = 8;

    fn build_revise_sql(
        output_revise_sql: bool,
        revise_match_full_row: bool,
        tb_meta: &CheckerTbMeta,
        src_row_data: &RowData,
        dst_row_data: Option<&RowData>,
        diff_col_values: Option<&HashMap<String, DiffColValue>>,
    ) -> anyhow::Result<Option<String>> {
        if !output_revise_sql {
            return Ok(None);
        };

        if src_row_data.row_type == RowType::Delete {
            let dst_row = dst_row_data.context("missing dst row for delete revise")?;
            return tb_meta.build_delete_sql(dst_row);
        }

        match diff_col_values {
            None => tb_meta.build_miss_sql(src_row_data),
            Some(diff_col_values) => {
                let dst_row = dst_row_data.context("missing dst row in diff")?;
                tb_meta.build_diff_sql(
                    src_row_data,
                    dst_row,
                    diff_col_values,
                    revise_match_full_row,
                )
            }
        }
    }

    pub async fn build_check_entry(
        check_result: CheckInconsistency,
        src_row_data: &RowData,
        dst_row_data: Option<&RowData>,
        ctx: &mut CheckContext,
        tb_meta: &CheckerTbMeta,
    ) -> anyhow::Result<CheckEntry> {
        let key = RecheckKey::from_row_data(src_row_data, &tb_meta.basic().id_cols)?;
        let (log, revise_sql, diff_cols) = match check_result {
            CheckInconsistency::Miss => {
                let log = Self::build_miss_log(src_row_data, ctx, tb_meta).await?;
                let revise_sql = Self::build_revise_sql(
                    ctx.output_revise_sql,
                    ctx.revise_match_full_row,
                    tb_meta,
                    src_row_data,
                    None,
                    None,
                )?;
                (log, revise_sql, None)
            }
            CheckInconsistency::Diff(diff_col_values) => {
                let dst_row = dst_row_data.context("missing dst row in diff")?;
                let revise_sql = Self::build_revise_sql(
                    ctx.output_revise_sql,
                    ctx.revise_match_full_row,
                    tb_meta,
                    src_row_data,
                    Some(dst_row),
                    Some(&diff_col_values),
                )?;
                let mut log = Self::build_miss_log(src_row_data, ctx, tb_meta).await?;
                let routed_diffs = if let Some(col_map) = ctx
                    .reverse_router
                    .get_col_map(&src_row_data.schema, &src_row_data.tb)
                {
                    diff_col_values
                        .into_iter()
                        .map(|(col, val)| {
                            let key = col_map
                                .get(&col)
                                .filter(|c| *c != &col)
                                .cloned()
                                .unwrap_or(col);
                            (key, val)
                        })
                        .collect()
                } else {
                    diff_col_values
                };
                let diff_cols = Self::summarize_diff_cols(&routed_diffs);
                log.diff_col_values = routed_diffs
                    .into_iter()
                    .filter(|(col, _)| diff_cols.contains(col))
                    .collect();
                log.dst_row = if ctx.output_full_row {
                    if ctx
                        .reverse_router
                        .get_col_map(&dst_row.schema, &dst_row.tb)
                        .is_some()
                    {
                        let routed = ctx.reverse_router.route_row(dst_row.clone());
                        Self::clone_row_values(&routed)
                    } else {
                        Self::clone_row_values(dst_row)
                    }
                } else {
                    None
                };
                (log, revise_sql, Some(diff_cols))
            }
        };

        Ok(CheckEntry {
            key,
            log,
            revise_sql,
            diff_cols,
        })
    }

    fn summarize_diff_cols(diff_col_values: &HashMap<String, DiffColValue>) -> Vec<String> {
        let mut cols = diff_col_values.keys().cloned().collect::<Vec<_>>();
        cols.sort();
        cols.truncate(Self::MAX_DIFF_COLS);
        cols
    }

    async fn check_rows(
        &mut self,
        src_data: &[PreparedCheckRow<'_>],
        mut dst_row_data_map: HashMap<u128, RowData>,
        tb_meta: &CheckerTbMeta,
    ) -> anyhow::Result<(usize, Vec<RowData>)> {
        let mut checked_count = 0;
        let mut retry_rows = Vec::new();

        for item in src_data {
            let src_row_data = item.row;
            let key = item.key;
            checked_count += 1;
            let dst_row_data = dst_row_data_map.remove(&key);

            if self.ctx.is_cdc || self.ctx.max_retries == 0 {
                self.reconcile_row_inconsistency(key, src_row_data, dst_row_data.as_ref(), tb_meta)
                    .await?;
            } else if Self::compare_src_dst(src_row_data, dst_row_data.as_ref(), tb_meta)?.is_some()
            {
                retry_rows.push((*src_row_data).clone());
            }
        }

        Ok((checked_count, retry_rows))
    }

    pub fn compare_src_dst(
        src_row: &RowData,
        dst_row: Option<&RowData>,
        tb_meta: &CheckerTbMeta,
    ) -> anyhow::Result<Option<CheckInconsistency>> {
        if src_row.row_type == RowType::Delete {
            return Ok(dst_row
                .is_some()
                .then_some(CheckInconsistency::Diff(HashMap::new())));
        }
        match dst_row {
            Some(dst_row) => {
                let diffs = Self::compare_row_data(src_row, dst_row, tb_meta)?;
                Ok((!diffs.is_empty()).then_some(CheckInconsistency::Diff(diffs)))
            }
            None => Ok(Some(CheckInconsistency::Miss)),
        }
    }

    fn compare_row_data(
        src_row_data: &RowData,
        dst_row_data: &RowData,
        tb_meta: &CheckerTbMeta,
    ) -> anyhow::Result<HashMap<String, DiffColValue>> {
        let src = src_row_data
            .after
            .as_ref()
            .context("src after is missing")?;
        let dst = dst_row_data
            .after
            .as_ref()
            .context("dst after is missing")?;

        let mut diff_col_values = HashMap::new();
        for (col, src_val) in src {
            if src_val.is_unchanged_toast() {
                continue;
            }
            let maybe_diff = match dst.get(col) {
                Some(dst_val) if Self::is_same_col_value(col, src_val, dst_val, tb_meta)? => None,
                Some(dst_val) => {
                    let src_type = src_val.type_name();
                    let dst_type = dst_val.type_name();
                    let type_diff = src_type != dst_type;
                    Some(DiffColValue {
                        src: src_val.to_option_string(),
                        dst: dst_val.to_option_string(),
                        src_type: type_diff.then(|| src_type.to_string()),
                        dst_type: type_diff.then(|| dst_type.to_string()),
                    })
                }
                None => Some(DiffColValue {
                    src: src_val.to_option_string(),
                    dst: None,
                    src_type: Some(src_val.type_name().to_string()),
                    dst_type: None,
                }),
            };

            if let Some(entry) = maybe_diff {
                diff_col_values.insert(col.to_owned(), entry);
            }
        }

        if diff_col_values.contains_key(MongoConstants::DOC)
            && [src_row_data, dst_row_data].iter().any(|row| {
                matches!(
                    row.after.as_ref().and_then(|m| m.get(MongoConstants::DOC)),
                    Some(ColValue::MongoDoc(_))
                )
            })
        {
            diff_col_values =
                Self::expand_mongo_doc_diff(src_row_data, dst_row_data, diff_col_values);
        }

        Ok(diff_col_values)
    }

    fn is_same_col_value(
        col: &str,
        src_val: &ColValue,
        dst_val: &ColValue,
        tb_meta: &CheckerTbMeta,
    ) -> anyhow::Result<bool> {
        if src_val.is_same_value(dst_val) {
            return Ok(true);
        }

        let is_pg_network_col = matches!(
            tb_meta,
            CheckerTbMeta::Pg(meta)
                if matches!(
                    meta.get_col_type(col)?.value_type,
                    PgValueType::INET | PgValueType::CIDR
                )
        );
        if !is_pg_network_col {
            return Ok(false);
        }

        Ok(match (src_val, dst_val) {
            (ColValue::String(v1), ColValue::String(v2)) => {
                Self::normalize_pg_network_text(v1) == Self::normalize_pg_network_text(v2)
            }
            _ => false,
        })
    }

    fn normalize_pg_network_text(value: &str) -> &str {
        value
            .strip_suffix("/32")
            .or_else(|| value.strip_suffix("/128"))
            .unwrap_or(value)
    }

    pub fn lookup_match_key(
        row_data: &RowData,
        tb_meta: &RdbTbMeta,
    ) -> anyhow::Result<Option<u128>> {
        let id_values = match row_data.row_type {
            RowType::Delete => row_data.require_before()?,
            _ => row_data.require_after()?,
        };
        let key = Self::hash_from_id_values(id_values, tb_meta)?;
        Ok((key != 0).then_some(key))
    }

    fn match_key_from_values(
        id_values: &HashMap<String, ColValue>,
        tb_meta: &RdbTbMeta,
    ) -> anyhow::Result<Option<u128>> {
        let key = Self::hash_from_id_values(id_values, tb_meta)?;
        Ok((key != 0).then_some(key))
    }

    fn should_sample_check_row(sample_rate: Option<u8>, row_key: u128) -> bool {
        let Some(sample_rate) = sample_rate.filter(|rate| *rate < 100) else {
            return true;
        };
        row_key % 100 < u128::from(sample_rate)
    }

    fn prepare_rows_for_fetch<'a>(
        &mut self,
        rows: &[&'a RowData],
        tb_meta: &CheckerTbMeta,
    ) -> Vec<PreparedCheckRow<'a>> {
        let mut prepared_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let row = *row;
            match Self::lookup_match_key(row, tb_meta.basic()) {
                Ok(Some(key)) => {
                    if self.ctx.is_cdc {
                        self.cleanup_stale_update_key(
                            row,
                            tb_meta.basic(),
                            Some(key),
                            self.ctx.sample_rate,
                        );
                    }
                    if Self::should_sample_check_row(self.ctx.sample_rate, key) {
                        prepared_rows.push(PreparedCheckRow { row, key });
                    }
                }
                Ok(None) => {
                    if self.ctx.is_cdc {
                        self.cleanup_stale_update_key(
                            row,
                            tb_meta.basic(),
                            None,
                            self.ctx.sample_rate,
                        );
                    }
                    log_warn!(
                        "Skipping row with NULL key component in {}.{}.",
                        row.schema,
                        row.tb
                    );
                    self.ctx.record_table_skipped(row, 1);
                    self.ctx.summary.skip_count += 1;
                    self.snapshot_dirty = true;
                }
                Err(e) => {
                    if self.ctx.is_cdc {
                        self.cleanup_stale_update_key(
                            row,
                            tb_meta.basic(),
                            None,
                            self.ctx.sample_rate,
                        );
                    }
                    log_warn!(
                        "Skipping unhashable row in {}.{}: {}",
                        row.schema,
                        row.tb,
                        e
                    );
                    self.ctx.record_table_skipped(row, 1);
                    self.ctx.summary.skip_count += 1;
                    self.snapshot_dirty = true;
                }
            }
        }
        prepared_rows
    }

    /// Computes a PK composite hash with `31 * h + col_hash` and returns 0 when any PK column is NULL.
    fn hash_from_id_values(
        id_values: &HashMap<String, ColValue>,
        tb_meta: &RdbTbMeta,
    ) -> anyhow::Result<u128> {
        let mut hash_code = 1u128;
        for col in &tb_meta.id_cols {
            let col_hash_code = id_values
                .get(col)
                .with_context(|| format!("missing id col value: {col}"))?
                .hash_code()
                .with_context(|| {
                    format!(
                        "unhashable _id value in schema: {}, tb: {}, col: {col}",
                        tb_meta.schema, tb_meta.tb
                    )
                })?;
            if col_hash_code == 0 {
                return Ok(0);
            }
            hash_code = 31 * hash_code + u128::from(col_hash_code);
        }
        Ok(hash_code)
    }

    fn select_dst_row(
        src_row: &RowData,
        tb_meta: &CheckerTbMeta,
        dst_rows: Vec<RowData>,
    ) -> anyhow::Result<Option<RowData>> {
        let Some(src_key) = Self::lookup_match_key(src_row, tb_meta.basic())? else {
            return Ok(None);
        };
        for row in dst_rows {
            if Self::lookup_match_key(&row, tb_meta.basic())? == Some(src_key) {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn log_entry(entry: &CheckEntry) {
        if let Some(log) = entry.log.to_json_line() {
            if entry.is_miss() {
                log_miss!("{}", log);
            } else {
                log_diff!("{}", log);
            }
        }
        if let Some(sql) = &entry.revise_sql {
            log_sql!("{}", sql);
        }
    }

    fn checker_metric_types(entry: &CheckEntry) -> (TaskMetricsType, CounterType) {
        if entry.is_miss() {
            (
                TaskMetricsType::CheckerMissCount,
                CounterType::CheckerMissCount,
            )
        } else {
            (
                TaskMetricsType::CheckerDiffCount,
                CounterType::CheckerDiffCount,
            )
        }
    }

    async fn add_entry_metrics(&self, entry: &CheckEntry) {
        let (task_metric, counter_type) = Self::checker_metric_types(entry);
        // Update both cumulative task metrics and checker window counters.
        if let Some(task_monitor) = &self.ctx.task_monitor {
            task_monitor.add_no_window_metrics(task_metric, 1);
        }
        self.ctx.monitor.add_counter(counter_type, 1).await;
    }

    /// Updates cumulative Prometheus summary counters, which differ from point-in-time unresolved snapshot counts.
    async fn update_summary_for_entry(&mut self, entry: &CheckEntry) {
        {
            let summary = &mut self.ctx.summary;
            if entry.is_miss() {
                summary.miss_count += 1;
            } else if entry.counts_as_diff() {
                summary.diff_count += 1;
            }
            if entry.revise_sql.is_some() {
                summary.sql_count = Some(summary.sql_count.unwrap_or_default() + 1);
            }
        }
        self.ctx.record_table_entry(entry);

        self.add_entry_metrics(entry).await;
    }

    pub fn update_pending_counter(&self) {
        self.ctx
            .monitor
            .set_counter(CounterType::CheckerPending, self.store.len() as u64);
    }

    pub fn remove_store_entry(&mut self, row_data: &RowData, row_key: u128) {
        let store_key = CheckerStoreKey::new(&row_data.schema, &row_data.tb, row_key);
        if let Some(entry) = self.store.shift_remove(&store_key) {
            self.dirty_upserts.shift_remove(&store_key);
            let identity_key = build_identity_key(&entry);
            if self
                .persisted_identity_keys
                .as_ref()
                .is_some_and(|keys| keys.contains(&identity_key))
            {
                self.dirty_deletes.insert(store_key, identity_key);
            }
            self.store_dirty = !self.dirty_upserts.is_empty() || !self.dirty_deletes.is_empty();
            self.snapshot_dirty = true;
            self.update_pending_counter();
        }
    }

    fn cleanup_stale_update_key(
        &mut self,
        row_data: &RowData,
        tb_meta: &RdbTbMeta,
        new_key: Option<u128>,
        sample_rate: Option<u8>,
    ) {
        if row_data.row_type != RowType::Update {
            return;
        }

        let Some(before_values) = row_data.before.as_ref() else {
            return;
        };

        match Self::match_key_from_values(before_values, tb_meta) {
            Ok(Some(old_key))
                if Some(old_key) != new_key
                    && Self::should_sample_check_row(sample_rate, old_key) =>
            {
                self.remove_store_entry(row_data, old_key);
            }
            Ok(_) => {}
            Err(err) => {
                log_warn!(
                    "Failed to compute pre-update key for {}.{}: {}",
                    row_data.schema,
                    row_data.tb,
                    err
                );
            }
        }
    }

    async fn reconcile_row_inconsistency(
        &mut self,
        row_key: u128,
        src_row_data: &RowData,
        dst_row_data: Option<&RowData>,
        tb_meta: &CheckerTbMeta,
    ) -> anyhow::Result<()> {
        if let Some(check_result) = Self::compare_src_dst(src_row_data, dst_row_data, tb_meta)? {
            let entry = Self::build_check_entry(
                check_result,
                src_row_data,
                dst_row_data,
                &mut self.ctx,
                tb_meta,
            )
            .await?;
            self.store_entry(src_row_data, row_key, entry).await;
        } else {
            self.remove_store_entry(src_row_data, row_key);
        }
        Ok(())
    }

    pub async fn store_entry(&mut self, row_data: &RowData, row_key: u128, entry: CheckEntry) {
        if !self.ctx.is_cdc {
            Self::log_entry(&entry);
            self.update_summary_for_entry(&entry).await;
            return;
        }

        self.store_dirty = true;
        self.snapshot_dirty = true;
        self.add_entry_metrics(&entry).await;
        let store_key = CheckerStoreKey::new(&row_data.schema, &row_data.tb, row_key);
        self.dirty_deletes.shift_remove(&store_key);
        self.dirty_upserts.insert(store_key.clone());
        self.store.insert(store_key, entry);
        self.update_pending_counter();
    }

    pub async fn flush_store(&mut self) {
        let entries = std::mem::take(&mut self.store);
        for (_key, entry) in entries {
            Self::log_entry(&entry);
            self.update_summary_for_entry(&entry).await;
        }
    }

    async fn build_miss_log(
        src_row_data: &RowData,
        ctx: &mut CheckContext,
        tb_meta: &CheckerTbMeta,
    ) -> anyhow::Result<CheckLog> {
        let (mapped_schema, mapped_tb) = ctx
            .reverse_router
            .get_tb_map(&src_row_data.schema, &src_row_data.tb);
        let has_col_map = ctx
            .reverse_router
            .get_col_map(&src_row_data.schema, &src_row_data.tb)
            .is_some();
        let schema_changed = src_row_data.schema != mapped_schema || src_row_data.tb != mapped_tb;

        let routed_row = if has_col_map {
            Cow::Owned(ctx.reverse_router.route_row(src_row_data.clone()))
        } else {
            Cow::Borrowed(src_row_data)
        };
        let (schema, tb) = if has_col_map {
            (routed_row.schema.clone(), routed_row.tb.clone())
        } else {
            (mapped_schema.to_string(), mapped_tb.to_string())
        };

        let id_col_values = if let Some(meta_manager) = ctx.extractor_meta_manager.as_mut() {
            let src_tb_meta = meta_manager.get_tb_meta(&schema, &tb).await?;
            Self::build_id_col_values(&routed_row, src_tb_meta)
                .context("Failed to build ID col values")?
        } else {
            Self::build_id_col_values(&routed_row, tb_meta.basic()).unwrap_or_default()
        };

        let src_row = if ctx.output_full_row {
            Self::clone_row_values(&routed_row)
        } else {
            None
        };

        Ok(CheckLog {
            schema,
            tb,
            target_schema: schema_changed.then(|| src_row_data.schema.clone()),
            target_tb: schema_changed.then(|| src_row_data.tb.clone()),
            id_col_values,
            diff_col_values: HashMap::new(),
            src_row,
            dst_row: None,
        })
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
    ) -> Option<HashMap<String, Option<String>>> {
        let col_values = match row_data.row_type {
            RowType::Delete => row_data.require_before().ok()?,
            _ => row_data.require_after().ok()?,
        };

        tb_meta
            .id_cols
            .iter()
            .map(|col| Some((col.to_owned(), col_values.get(col)?.to_option_string())))
            .collect()
    }

    fn expand_mongo_doc_diff(
        src_row_data: &RowData,
        dst_row_data: &RowData,
        mut diff_col_values: HashMap<String, DiffColValue>,
    ) -> HashMap<String, DiffColValue> {
        diff_col_values.remove(MongoConstants::DOC);
        let src_doc = match src_row_data
            .after
            .as_ref()
            .and_then(|after| after.get(MongoConstants::DOC))
        {
            Some(ColValue::MongoDoc(doc)) => Some(doc),
            _ => None,
        };
        let dst_doc = match dst_row_data
            .after
            .as_ref()
            .and_then(|after| after.get(MongoConstants::DOC))
        {
            Some(ColValue::MongoDoc(doc)) => Some(doc),
            _ => None,
        };

        let keys: BTreeSet<_> = src_doc
            .into_iter()
            .flat_map(Document::keys)
            .chain(dst_doc.into_iter().flat_map(Document::keys))
            .cloned()
            .collect();

        for key in keys {
            let src_value = src_doc.and_then(|d| d.get(&key));
            let dst_value = dst_doc.and_then(|d| d.get(&key));
            if src_value == dst_value {
                continue;
            }
            let src_type_name = src_value.map_or("None", mongo_cmd::bson_type_name);
            let dst_type_name = dst_value.map_or("None", mongo_cmd::bson_type_name);
            let type_diff = src_type_name != dst_type_name;
            diff_col_values.insert(
                key,
                DiffColValue {
                    src: src_value.map(mongo_cmd::bson_to_log_literal),
                    dst: dst_value.map(mongo_cmd::bson_to_log_literal),
                    src_type: type_diff.then(|| src_type_name.to_string()),
                    dst_type: type_diff.then(|| dst_type_name.to_string()),
                },
            );
        }

        diff_col_values
    }

    fn enqueue_retry_rows(&mut self, rows: Vec<RowData>) {
        if rows.is_empty() {
            return;
        }

        let retry_at = Instant::now() + Duration::from_secs(self.ctx.retry_interval_secs);
        if self.retry_next_at.is_none_or(|current| retry_at < current) {
            self.retry_next_at = Some(retry_at);
        }
        self.retry_queue
            .extend(rows.into_iter().map(|row| RetryItem {
                row,
                retries_left: self.ctx.max_retries,
                next_retry_at: retry_at,
            }));
    }

    pub async fn process_due_retries(&mut self) -> anyhow::Result<()> {
        if self.retry_queue.is_empty() {
            return Ok(());
        }
        let now = Instant::now();
        if self.retry_next_at.is_some_and(|t| t > now) {
            return Ok(());
        }

        let mut next_retry_at: Option<Instant> = None;
        let pending_len = self.retry_queue.len();
        for _ in 0..pending_len {
            let Some(item) = self.retry_queue.pop_front() else {
                break;
            };

            if item.next_retry_at > now {
                next_retry_at = Some(
                    next_retry_at
                        .map_or(item.next_retry_at, |c: Instant| c.min(item.next_retry_at)),
                );
                self.retry_queue.push_back(item);
                continue;
            }

            if let Some(rescheduled) = self.retry_check_item(item).await? {
                next_retry_at = Some(
                    next_retry_at.map_or(rescheduled.next_retry_at, |c: Instant| {
                        c.min(rescheduled.next_retry_at)
                    }),
                );
                self.retry_queue.push_back(rescheduled);
            }
        }
        self.retry_next_at = next_retry_at;
        Ok(())
    }

    async fn retry_check_item(&mut self, mut item: RetryItem) -> anyhow::Result<Option<RetryItem>> {
        let row_ref = &item.row;
        let tb_meta = self.checker.fetch_meta(row_ref).await?;
        let Some(key) = Self::lookup_match_key(&item.row, tb_meta.basic())? else {
            log_warn!(
                "Skipping retry row with NULL key component in {}.{}.",
                item.row.schema,
                item.row.tb
            );
            self.ctx.summary.skip_count += 1;
            self.ctx.record_table_skipped(&item.row, 1);
            self.snapshot_dirty = true;
            return Ok(None);
        };
        let dst_rows = self
            .checker
            .fetch(tb_meta.clone(), std::slice::from_ref(&row_ref))
            .await?;
        let dst_row = Self::select_dst_row(&item.row, tb_meta.as_ref(), dst_rows)?;

        if item.retries_left > 1 {
            if Self::compare_src_dst(&item.row, dst_row.as_ref(), tb_meta.as_ref())?.is_none() {
                return Ok(None);
            }
            item.retries_left -= 1;
            item.next_retry_at = Instant::now() + Duration::from_secs(self.ctx.retry_interval_secs);
            return Ok(Some(item));
        }

        self.cleanup_stale_update_key(&item.row, tb_meta.basic(), Some(key), None);
        self.reconcile_row_inconsistency(key, &item.row, dst_row.as_ref(), tb_meta.as_ref())
            .await?;
        Ok(None)
    }

    pub async fn drain_retries(&mut self) -> anyhow::Result<()> {
        while !self.retry_queue.is_empty() {
            let next_retry_at = self
                .retry_queue
                .iter()
                .map(|item| item.next_retry_at)
                .min()
                .expect("retry queue should not be empty");
            let now = Instant::now();
            if next_retry_at > now {
                sleep(next_retry_at.duration_since(now)).await;
            }
            self.process_due_retries().await?;
        }
        self.retry_next_at = None;
        Ok(())
    }

    pub async fn check_batch(&mut self, data: &[RowData], batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        if !batch {
            return self.process_batch(data, true).await;
        }
        let batch_size = self.ctx.batch_size.max(1);
        for chunk in data.chunks(batch_size) {
            self.process_batch(chunk, false).await?;
        }
        Ok(())
    }

    pub async fn process_batch(
        &mut self,
        data: &[RowData],
        is_serial_mode: bool,
    ) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let start_time = tokio::time::Instant::now();
        let mut grouped: HashMap<(&str, &str), Vec<&RowData>> = HashMap::new();
        for row in data {
            grouped
                .entry((row.schema.as_str(), row.tb.as_str()))
                .or_default()
                .push(row);
        }

        let mut total_checked = 0usize;
        let mut retry_rows = Vec::new();
        for rows in grouped.into_values() {
            let first_row = rows.first().context("checker group is empty")?;
            let tb_meta = self.checker.fetch_meta(first_row).await?;
            let prepared_rows = self.prepare_rows_for_fetch(&rows, tb_meta.as_ref());
            if prepared_rows.is_empty() {
                continue;
            }
            let rows_to_fetch = prepared_rows
                .iter()
                .map(|item| item.row)
                .collect::<Vec<_>>();
            let dst_rows = self.checker.fetch(tb_meta.clone(), &rows_to_fetch).await?;

            let mut dst_row_data_map = HashMap::with_capacity(dst_rows.len());
            for row in dst_rows {
                if let Some(key) = Self::lookup_match_key(&row, tb_meta.basic())? {
                    dst_row_data_map.insert(key, row);
                }
            }

            let (checked_count, table_retry_rows) = self
                .check_rows(&prepared_rows, dst_row_data_map, tb_meta.as_ref())
                .await?;
            self.ctx.record_table_checked(first_row, checked_count);
            total_checked += checked_count;
            retry_rows.extend(table_retry_rows);
        }
        if total_checked == 0 {
            return Ok(());
        }

        let mut rts = LimitedQueue::new(1);
        let elapsed_millis = u64::try_from(start_time.elapsed().as_millis()).unwrap_or(u64::MAX);
        rts.push((elapsed_millis, 1));
        self.enqueue_retry_rows(retry_rows);

        let monitor = self.ctx.monitor.clone();
        if is_serial_mode {
            BaseSinker::update_serial_monitor(&monitor, total_checked as u64, 0).await?;
        } else {
            BaseSinker::update_batch_monitor(&monitor, total_checked as u64, 0).await?;
        }
        BaseSinker::update_monitor_rt(&monitor, &rts).await
    }
}

#[cfg(test)]
mod tests {
    use super::super::{CheckContext, CheckerIo};
    use super::*;
    use crate::{
        checker::check_log::{CheckLog, CheckSummaryLog},
        rdb_router::RdbRouter,
    };
    use async_trait::async_trait;
    use dt_common::monitor::monitor::Monitor;
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex as StdMutex,
    };

    struct DummyChecker;

    #[async_trait]
    impl Checker for DummyChecker {
        async fn fetch_meta(&mut self, _src_row: &RowData) -> anyhow::Result<Arc<CheckerTbMeta>> {
            unreachable!("ut should not call fetch_meta")
        }

        async fn fetch(
            &mut self,
            _tb_meta: Arc<CheckerTbMeta>,
            _src_rows: &[&RowData],
        ) -> anyhow::Result<Vec<RowData>> {
            unreachable!("ut should not call fetch")
        }
    }

    struct MetaOnlyChecker {
        tb_meta: Arc<CheckerTbMeta>,
        fetch_count: Arc<AtomicU64>,
    }

    #[async_trait]
    impl Checker for MetaOnlyChecker {
        async fn fetch_meta(&mut self, _src_row: &RowData) -> anyhow::Result<Arc<CheckerTbMeta>> {
            Ok(self.tb_meta.clone())
        }

        async fn fetch(
            &mut self,
            _tb_meta: Arc<CheckerTbMeta>,
            _src_rows: &[&RowData],
        ) -> anyhow::Result<Vec<RowData>> {
            self.fetch_count.fetch_add(1, Ordering::Relaxed);
            Err(anyhow::anyhow!(
                "fetch should not be called for empty sample"
            ))
        }
    }

    fn build_ctx(is_cdc: bool) -> CheckContext {
        CheckContext {
            monitor: Arc::new(Monitor::new("checker", "unit-test", 1, 1, 1)),
            task_monitor: None,
            summary: CheckSummaryLog::default(),
            output_revise_sql: false,
            extractor_meta_manager: None,
            reverse_router: RdbRouter {
                schema_map: HashMap::new(),
                tb_map: HashMap::new(),
                col_map: HashMap::new(),
                topic_map: HashMap::new(),
            },
            output_full_row: false,
            revise_match_full_row: false,
            global_summary: None,
            batch_size: 1,
            sample_rate: None,
            retry_interval_secs: 0,
            max_retries: 0,
            is_cdc,
            check_log_dir: String::new(),
            cdc_check_log_max_file_size: 1,
            cdc_check_log_max_rows: 1,
            s3_output: None,
            cdc_check_log_interval_secs: 1,
            state_store: None,
            source_checker: None,
            expected_resume_position: None,
        }
    }

    fn build_mysql_tb_meta() -> CheckerTbMeta {
        CheckerTbMeta::Mysql(dt_common::meta::mysql::mysql_tb_meta::MysqlTbMeta {
            basic: RdbTbMeta {
                schema: "s1".to_string(),
                tb: "t1".to_string(),
                cols: vec!["id".to_string(), "name".to_string()],
                id_cols: vec!["id".to_string()],
                ..Default::default()
            },
            col_type_map: HashMap::from([
                (
                    "id".to_string(),
                    dt_common::meta::mysql::mysql_col_type::MysqlColType::BigInt {
                        unsigned: false,
                    },
                ),
                (
                    "name".to_string(),
                    dt_common::meta::mysql::mysql_col_type::MysqlColType::Varchar {
                        length: 32,
                        charset: "utf8mb4".to_string(),
                    },
                ),
            ]),
        })
    }

    fn build_insert_row(id: i32, name: &str) -> RowData {
        RowData::new(
            "s1".to_string(),
            "t1".to_string(),
            RowType::Insert,
            None,
            Some(HashMap::from([
                ("id".to_string(), ColValue::Long(id)),
                ("name".to_string(), ColValue::String(name.to_string())),
            ])),
        )
    }

    fn build_null_key_row() -> RowData {
        RowData::new(
            "s1".to_string(),
            "t1".to_string(),
            RowType::Insert,
            None,
            Some(HashMap::from([
                ("id".to_string(), ColValue::None),
                ("name".to_string(), ColValue::String("null-key".to_string())),
            ])),
        )
    }

    fn build_update_row(old_id: i32, new_id: i32) -> RowData {
        RowData::new(
            "s1".to_string(),
            "t1".to_string(),
            RowType::Update,
            Some(HashMap::from([
                ("id".to_string(), ColValue::Long(old_id)),
                ("name".to_string(), ColValue::String("old".to_string())),
            ])),
            Some(HashMap::from([
                ("id".to_string(), ColValue::Long(new_id)),
                ("name".to_string(), ColValue::String("new".to_string())),
            ])),
        )
    }

    fn build_checker() -> DataChecker<DummyChecker> {
        let (_control_tx, control_rx) = tokio::sync::mpsc::unbounded_channel();
        DataChecker::new(
            DummyChecker,
            "unit-test".to_string(),
            build_ctx(true),
            CheckerIo {
                batch_queue: Arc::new(StdMutex::new(LimitedQueue::new(1))),
                batch_notify: Arc::new(tokio::sync::Notify::new()),
                dropped_items: Arc::new(AtomicU64::new(0)),
                control_rx,
            },
            "unit-test",
        )
    }

    fn build_checker_with<C: Checker>(checker: C, ctx: CheckContext) -> DataChecker<C> {
        let (_control_tx, control_rx) = tokio::sync::mpsc::unbounded_channel();
        DataChecker::new(
            checker,
            "unit-test".to_string(),
            ctx,
            CheckerIo {
                batch_queue: Arc::new(StdMutex::new(LimitedQueue::new(1))),
                batch_notify: Arc::new(tokio::sync::Notify::new()),
                dropped_items: Arc::new(AtomicU64::new(0)),
                control_rx,
            },
            "unit-test",
        )
    }

    fn build_entry(row: &RowData, tb_meta: &RdbTbMeta) -> CheckEntry {
        CheckEntry {
            key: RecheckKey::from_row_data(row, &tb_meta.id_cols).unwrap(),
            log: CheckLog {
                schema: row.schema.clone(),
                tb: row.tb.clone(),
                target_schema: None,
                target_tb: None,
                id_col_values: HashMap::new(),
                diff_col_values: HashMap::new(),
                src_row: None,
                dst_row: None,
            },
            revise_sql: None,
            diff_cols: None,
        }
    }

    #[tokio::test]
    async fn build_check_entry_keeps_diff_values_full_rows_and_revise_sql_for_cdc_diff() {
        let src = RowData::new(
            "s1".to_string(),
            "t1".to_string(),
            RowType::Insert,
            None,
            Some(HashMap::from([
                ("id".to_string(), ColValue::Long(1)),
                ("name".to_string(), ColValue::String("src".to_string())),
            ])),
        );
        let dst = RowData::new(
            "s1".to_string(),
            "t1".to_string(),
            RowType::Insert,
            None,
            Some(HashMap::from([
                ("id".to_string(), ColValue::Long(1)),
                ("name".to_string(), ColValue::String("dst".to_string())),
            ])),
        );
        let tb_meta = build_mysql_tb_meta();
        let mut ctx = build_ctx(true);
        ctx.output_full_row = true;
        ctx.output_revise_sql = true;

        let entry = DataChecker::<DummyChecker>::build_check_entry(
            CheckInconsistency::Diff(HashMap::from([(
                "name".to_string(),
                DiffColValue {
                    src: Some("src".to_string()),
                    dst: Some("dst".to_string()),
                    src_type: None,
                    dst_type: None,
                },
            )])),
            &src,
            Some(&dst),
            &mut ctx,
            &tb_meta,
        )
        .await
        .unwrap();

        assert_eq!(
            entry.log.diff_col_values["name"].src.as_deref(),
            Some("src")
        );
        assert_eq!(
            entry.log.diff_col_values["name"].dst.as_deref(),
            Some("dst")
        );
        assert!(entry.log.src_row.is_some());
        assert!(entry.log.dst_row.is_some());
        assert!(entry.revise_sql.is_some());
    }

    #[test]
    fn sampling_uses_pk_hash_and_filters_before_fetch() {
        let tb_meta = build_mysql_tb_meta();
        let row_a = build_insert_row(7, "alice");
        let row_b = build_insert_row(7, "bob");
        let key_a = DataChecker::<DummyChecker>::lookup_match_key(&row_a, tb_meta.basic())
            .unwrap()
            .unwrap();
        let key_b = DataChecker::<DummyChecker>::lookup_match_key(&row_b, tb_meta.basic())
            .unwrap()
            .unwrap();

        assert_eq!(key_a, key_b);
        let bucket = (key_a % 100) as u8;
        let passing_rate = (bucket + 1).min(100);
        assert!(DataChecker::<DummyChecker>::should_sample_check_row(
            Some(passing_rate),
            key_a,
        ));
        if bucket > 0 {
            assert!(!DataChecker::<DummyChecker>::should_sample_check_row(
                Some(bucket),
                key_a,
            ));
        }

        let row = build_insert_row(1, "not-sampled");
        let mut checker = build_checker();
        checker.ctx.sample_rate = Some(0);
        let rows = checker.prepare_rows_for_fetch(&[&row], &tb_meta);
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn pre_fetch_sampling_skips_group_when_sample_is_empty() {
        let tb_meta = Arc::new(build_mysql_tb_meta());
        let fetch_count = Arc::new(AtomicU64::new(0));
        let mut ctx = build_ctx(false);
        ctx.sample_rate = Some(0);
        let mut checker = build_checker_with(
            MetaOnlyChecker {
                tb_meta,
                fetch_count: fetch_count.clone(),
            },
            ctx,
        );

        checker
            .process_batch(&[build_insert_row(1, "not-sampled")], false)
            .await
            .unwrap();

        assert_eq!(fetch_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn pre_fetch_filter_skips_null_key_without_target_fetch() {
        let tb_meta = Arc::new(build_mysql_tb_meta());
        let fetch_count = Arc::new(AtomicU64::new(0));
        let ctx = build_ctx(false);
        let mut checker = build_checker_with(
            MetaOnlyChecker {
                tb_meta,
                fetch_count: fetch_count.clone(),
            },
            ctx,
        );

        checker
            .process_batch(&[build_null_key_row()], false)
            .await
            .unwrap();

        assert_eq!(fetch_count.load(Ordering::Relaxed), 0);
        assert_eq!(checker.ctx.summary.skip_count, 1);
        assert_eq!(checker.ctx.summary.tables[0].skip_count, 1);
        assert_eq!(checker.ctx.summary.tables[0].checked_count, 0);
    }

    #[test]
    fn sampled_cdc_pk_update_cleans_only_sampled_old_key() {
        let tb_meta = build_mysql_tb_meta();
        let mut selected = None;
        for old_id in 1..500 {
            let old_row = build_insert_row(old_id, "old");
            let old_key = DataChecker::<DummyChecker>::lookup_match_key(&old_row, tb_meta.basic())
                .unwrap()
                .unwrap();
            let old_bucket = (old_key % 100) as u8;
            if old_bucket == 0 {
                continue;
            }
            let new_row = build_insert_row(old_id + 1000, "new");
            let new_key = DataChecker::<DummyChecker>::lookup_match_key(&new_row, tb_meta.basic())
                .unwrap()
                .unwrap();
            if old_key != new_key {
                selected = Some((old_id, old_key, new_key, old_bucket));
                break;
            }
        }
        let (old_id, old_key, new_key, old_bucket) =
            selected.expect("test should find an old key with bucket > 0");
        let old_store_key = CheckerStoreKey::new("s1", "t1", old_key);
        let old_row = build_insert_row(old_id, "old");
        let update_row = build_update_row(old_id, old_id + 1000);

        let mut checker = build_checker();
        checker.store.insert(
            old_store_key.clone(),
            build_entry(&old_row, tb_meta.basic()),
        );
        checker.cleanup_stale_update_key(
            &update_row,
            tb_meta.basic(),
            Some(new_key),
            Some(old_bucket),
        );
        assert!(checker.store.contains_key(&old_store_key));

        checker.cleanup_stale_update_key(
            &update_row,
            tb_meta.basic(),
            Some(new_key),
            Some(old_bucket + 1),
        );
        assert!(!checker.store.contains_key(&old_store_key));
    }
}
