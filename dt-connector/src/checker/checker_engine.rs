use super::*;
use dt_common::monitor::counter_type::CounterType;

impl<C: Checker> DataChecker<C> {
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

    async fn build_check_entry(
        check_result: CheckInconsistency,
        src_row_data: &RowData,
        dst_row_data: Option<&RowData>,
        ctx: &mut CheckContext,
        tb_meta: &CheckerTbMeta,
    ) -> anyhow::Result<CheckEntry> {
        let (log, revise_sql, is_miss) = match check_result {
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
                (log, revise_sql, true)
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
                log.diff_col_values = if let Some(col_map) = ctx
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
                (log, revise_sql, false)
            }
        };

        Ok(CheckEntry {
            log,
            revise_sql,
            is_miss,
            src_row_data: src_row_data.clone(),
        })
    }

    async fn check_rows(
        &mut self,
        src_data: &[&RowData],
        mut dst_row_data_map: HashMap<u128, RowData>,
        tb_meta: &CheckerTbMeta,
    ) -> anyhow::Result<(usize, Vec<RowData>)> {
        let mut skip_count = 0;
        let mut retry_rows = Vec::new();

        for src_row_data in src_data {
            let key = match Self::lookup_hash_code(src_row_data, tb_meta.basic()) {
                Ok(k) => k,
                Err(e) => {
                    log_warn!(
                        "Skipping unhashable row in {}.{}: {}",
                        src_row_data.schema,
                        src_row_data.tb,
                        e
                    );
                    skip_count += 1;
                    continue;
                }
            };

            self.cleanup_stale_update_key(src_row_data, tb_meta.basic(), key);
            let dst_row_data = dst_row_data_map.remove(&key);

            if self.ctx.is_cdc || self.ctx.max_retries == 0 {
                self.reconcile_row_inconsistency(key, src_row_data, dst_row_data.as_ref(), tb_meta)
                    .await?;
            } else if Self::compare_src_dst(src_row_data, dst_row_data.as_ref())?.is_some() {
                retry_rows.push((*src_row_data).clone());
            }
        }

        Ok((skip_count, retry_rows))
    }

    fn compare_src_dst(
        src_row: &RowData,
        dst_row: Option<&RowData>,
    ) -> anyhow::Result<Option<CheckInconsistency>> {
        if src_row.row_type == RowType::Delete {
            return Ok(dst_row
                .is_some()
                .then_some(CheckInconsistency::Diff(HashMap::new())));
        }
        match dst_row {
            Some(dst_row) => {
                let diffs = Self::compare_row_data(src_row, dst_row)?;
                Ok((!diffs.is_empty()).then_some(CheckInconsistency::Diff(diffs)))
            }
            None => Ok(Some(CheckInconsistency::Miss)),
        }
    }

    fn compare_row_data(
        src_row_data: &RowData,
        dst_row_data: &RowData,
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
                Some(dst_val) if src_val.is_same_value(dst_val) => None,
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

    fn lookup_hash_code(row_data: &RowData, tb_meta: &RdbTbMeta) -> anyhow::Result<u128> {
        let id_values = match row_data.row_type {
            RowType::Delete => row_data.require_before()?,
            _ => row_data.require_after()?,
        };
        Self::hash_from_id_values(id_values, tb_meta)
    }

    fn hash_from_id_values(
        id_values: &HashMap<String, ColValue>,
        tb_meta: &RdbTbMeta,
    ) -> anyhow::Result<u128> {
        let mut hash_code = 1u128;
        for col in &tb_meta.id_cols {
            let col_hash_code = id_values
                .get(col)
                .with_context(|| format!("missing id col value: {}", col))?
                .hash_code()
                .with_context(|| {
                    format!(
                        "unhashable _id value in schema: {}, tb: {}, col: {}",
                        tb_meta.schema, tb_meta.tb, col
                    )
                })?;
            hash_code = 31 * hash_code + col_hash_code as u128;
        }
        Ok(hash_code)
    }

    fn select_dst_row(
        src_row: &RowData,
        tb_meta: &CheckerTbMeta,
        dst_rows: Vec<RowData>,
    ) -> anyhow::Result<Option<RowData>> {
        let src_key = Self::lookup_hash_code(src_row, tb_meta.basic())?;
        for row in dst_rows {
            if Self::lookup_hash_code(&row, tb_meta.basic())? == src_key {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn log_entry(&self, entry: &CheckEntry) {
        if entry.is_miss {
            log_miss!("{}", entry.log);
        } else {
            log_diff!("{}", entry.log);
        }
        if let Some(sql) = &entry.revise_sql {
            log_sql!("{}", sql);
        }
    }

    async fn update_summary_for_entry(&mut self, entry: &CheckEntry) {
        let summary = &mut self.ctx.summary;
        summary.end_time = chrono::Local::now().to_rfc3339();
        if entry.is_miss {
            summary.miss_count += 1;
            self.ctx
                .monitor
                .add_counter(CounterType::CheckerMissCount, 1)
                .await;
        } else {
            summary.diff_count += 1;
            self.ctx
                .monitor
                .add_counter(CounterType::CheckerDiffCount, 1)
                .await;
        }
        if entry.revise_sql.is_some() {
            summary.sql_count = Some(summary.sql_count.unwrap_or(0) + 1);
        }
    }

    pub(super) fn update_pending_counter(&self) {
        self.ctx
            .monitor
            .set_counter(CounterType::CheckerPending, self.store.len() as u64);
    }

    pub(super) fn remove_store_entry(&mut self, key: u128) {
        if self.store.shift_remove(&key).is_some() {
            self.store_dirty = true;
            self.update_pending_counter();
        }
    }

    fn cleanup_stale_update_key(&mut self, row_data: &RowData, tb_meta: &RdbTbMeta, key: u128) {
        if row_data.row_type != RowType::Update {
            return;
        }

        let Some(before_values) = row_data.before.as_ref() else {
            return;
        };

        match Self::hash_from_id_values(before_values, tb_meta) {
            Ok(old_key) if old_key != key => self.remove_store_entry(old_key),
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
        key: u128,
        src_row_data: &RowData,
        dst_row_data: Option<&RowData>,
        tb_meta: &CheckerTbMeta,
    ) -> anyhow::Result<()> {
        if let Some(check_result) = Self::compare_src_dst(src_row_data, dst_row_data)? {
            let entry = Self::build_check_entry(
                check_result,
                src_row_data,
                dst_row_data,
                &mut self.ctx,
                tb_meta,
            )
            .await?;
            self.store_entry(key, entry).await;
        } else {
            self.remove_store_entry(key);
        }
        Ok(())
    }

    pub(super) async fn store_entry(&mut self, key: u128, entry: CheckEntry) {
        if !self.ctx.is_cdc {
            self.log_entry(&entry);
            self.update_summary_for_entry(&entry).await;
            return;
        }

        self.store_dirty = true;
        if entry.is_miss {
            self.ctx
                .monitor
                .add_counter(CounterType::CheckerMissCount, 1)
                .await;
        } else {
            self.ctx
                .monitor
                .add_counter(CounterType::CheckerDiffCount, 1)
                .await;
        }
        self.store.insert(key, entry);
        self.update_pending_counter();
    }

    pub(super) async fn flush_store(&mut self) {
        let entries = std::mem::take(&mut self.store);
        for (_key, entry) in entries {
            self.log_entry(&entry);
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
            let src_type_name = src_value.map(mongo_cmd::bson_type_name).unwrap_or("None");
            let dst_type_name = dst_value.map(mongo_cmd::bson_type_name).unwrap_or("None");
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

    pub(super) async fn process_due_retries(&mut self) -> anyhow::Result<()> {
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
        let fetch_result = self.checker.fetch(std::slice::from_ref(&row_ref)).await?;
        let tb_meta = fetch_result.tb_meta;
        let dst_row = Self::select_dst_row(&item.row, tb_meta.as_ref(), fetch_result.dst_rows)?;

        if item.retries_left > 1 {
            if Self::compare_src_dst(&item.row, dst_row.as_ref())?.is_none() {
                return Ok(None);
            }
            item.retries_left -= 1;
            item.next_retry_at = Instant::now() + Duration::from_secs(self.ctx.retry_interval_secs);
            return Ok(Some(item));
        }

        let key = Self::lookup_hash_code(&item.row, tb_meta.basic())?;
        self.cleanup_stale_update_key(&item.row, tb_meta.basic(), key);
        self.reconcile_row_inconsistency(key, &item.row, dst_row.as_ref(), tb_meta.as_ref())
            .await?;
        Ok(None)
    }

    pub(super) async fn drain_retries(&mut self) -> anyhow::Result<()> {
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

    pub(super) async fn check_batch(
        &mut self,
        data: &[RowData],
        batch: bool,
    ) -> anyhow::Result<()> {
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

    pub(super) async fn process_batch(
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
        let mut total_skip_count = 0usize;
        let mut retry_rows = Vec::new();
        for rows in grouped.into_values() {
            let fetch_result = self.checker.fetch(&rows).await?;
            let tb_meta = fetch_result.tb_meta;
            total_checked += rows.len();

            let mut dst_row_data_map = HashMap::with_capacity(fetch_result.dst_rows.len());
            for row in fetch_result.dst_rows {
                dst_row_data_map.insert(Self::lookup_hash_code(&row, tb_meta.basic())?, row);
            }

            let (skip_count, table_retry_rows) = self
                .check_rows(&rows, dst_row_data_map, tb_meta.as_ref())
                .await?;
            total_skip_count += skip_count;
            retry_rows.extend(table_retry_rows);
        }
        if total_checked == 0 {
            return Ok(());
        }

        let mut rts = LimitedQueue::new(1);
        rts.push((start_time.elapsed().as_millis() as u64, 1));
        self.ctx.summary.skip_count += total_skip_count;
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
    use super::*;
    use async_trait::async_trait;

    struct DummyChecker;

    #[async_trait]
    impl Checker for DummyChecker {
        async fn fetch(&mut self, _src_rows: &[&RowData]) -> anyhow::Result<FetchResult> {
            unreachable!("hash test should not call fetch")
        }
    }

    #[test]
    fn nullable_composite_key_hash_keeps_non_null_components() {
        let tb_meta = RdbTbMeta {
            schema: "s1".to_string(),
            tb: "t1".to_string(),
            id_cols: vec!["id1".to_string(), "id2".to_string()],
            ..Default::default()
        };

        let values1 = HashMap::from([
            ("id1".to_string(), ColValue::None),
            ("id2".to_string(), ColValue::Long(1)),
        ]);
        let values2 = HashMap::from([
            ("id1".to_string(), ColValue::None),
            ("id2".to_string(), ColValue::Long(2)),
        ]);

        let hash1 = DataChecker::<DummyChecker>::hash_from_id_values(&values1, &tb_meta)
            .expect("nullable composite key should still hash");
        let hash2 = DataChecker::<DummyChecker>::hash_from_id_values(&values2, &tb_meta)
            .expect("nullable composite key should still hash");

        assert_ne!(hash1, hash2);
    }
}
