use anyhow::Context;
use std::collections::{BTreeMap, BTreeSet};

use super::*;

#[derive(Serialize)]
struct IdentityJsonRef<'a> {
    schema: &'a str,
    tb: &'a str,
    id_col_values: BTreeMap<&'a str, &'a Option<String>>,
}

fn build_identity_json_ref(entry: &CheckEntry) -> IdentityJsonRef<'_> {
    let id_col_values = entry
        .log
        .id_col_values
        .iter()
        .map(|(key, value)| (key.as_str(), value))
        .collect::<BTreeMap<_, _>>();
    IdentityJsonRef {
        schema: &entry.log.schema,
        tb: &entry.log.tb,
        id_col_values,
    }
}

fn build_identity_json(entry: &CheckEntry) -> anyhow::Result<String> {
    serde_json::to_string(&build_identity_json_ref(entry)).map_err(anyhow::Error::from)
}

#[derive(Clone, Serialize, Deserialize)]
enum PersistedKeyValue {
    None,
    Bool(bool),
    Tiny(i8),
    UnsignedTiny(u8),
    Short(i16),
    UnsignedShort(u16),
    Long(i32),
    UnsignedLong(u32),
    LongLong(i64),
    UnsignedLongLong(u64),
    Decimal(String),
    Time(String),
    Date(String),
    DateTime(String),
    Timestamp(String),
    Year(u16),
    String(String),
}

impl PersistedKeyValue {
    fn try_from_col_value(value: &ColValue) -> anyhow::Result<Self> {
        Ok(match value {
            ColValue::None => Self::None,
            ColValue::Bool(v) => Self::Bool(*v),
            ColValue::Tiny(v) => Self::Tiny(*v),
            ColValue::UnsignedTiny(v) => Self::UnsignedTiny(*v),
            ColValue::Short(v) => Self::Short(*v),
            ColValue::UnsignedShort(v) => Self::UnsignedShort(*v),
            ColValue::Long(v) => Self::Long(*v),
            ColValue::UnsignedLong(v) => Self::UnsignedLong(*v),
            ColValue::LongLong(v) => Self::LongLong(*v),
            ColValue::UnsignedLongLong(v) => Self::UnsignedLongLong(*v),
            ColValue::Decimal(v) => Self::Decimal(v.clone()),
            ColValue::Time(v) => Self::Time(v.clone()),
            ColValue::Date(v) => Self::Date(v.clone()),
            ColValue::DateTime(v) => Self::DateTime(v.clone()),
            ColValue::Timestamp(v) => Self::Timestamp(v.clone()),
            ColValue::Year(v) => Self::Year(*v),
            ColValue::String(v) => Self::String(v.clone()),
            other => anyhow::bail!(
                "unsupported pk col value for checkpoint: {}",
                other.type_name()
            ),
        })
    }

    fn into_col_value(self) -> ColValue {
        match self {
            PersistedKeyValue::None => ColValue::None,
            PersistedKeyValue::Bool(v) => ColValue::Bool(v),
            PersistedKeyValue::Tiny(v) => ColValue::Tiny(v),
            PersistedKeyValue::UnsignedTiny(v) => ColValue::UnsignedTiny(v),
            PersistedKeyValue::Short(v) => ColValue::Short(v),
            PersistedKeyValue::UnsignedShort(v) => ColValue::UnsignedShort(v),
            PersistedKeyValue::Long(v) => ColValue::Long(v),
            PersistedKeyValue::UnsignedLong(v) => ColValue::UnsignedLong(v),
            PersistedKeyValue::LongLong(v) => ColValue::LongLong(v),
            PersistedKeyValue::UnsignedLongLong(v) => ColValue::UnsignedLongLong(v),
            PersistedKeyValue::Decimal(v) => ColValue::Decimal(v),
            PersistedKeyValue::Time(v) => ColValue::Time(v),
            PersistedKeyValue::Date(v) => ColValue::Date(v),
            PersistedKeyValue::DateTime(v) => ColValue::DateTime(v),
            PersistedKeyValue::Timestamp(v) => ColValue::Timestamp(v),
            PersistedKeyValue::Year(v) => ColValue::Year(v),
            PersistedKeyValue::String(v) => ColValue::String(v),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct PersistedRecheckKey {
    schema: String,
    tb: String,
    is_delete: bool,
    pk: BTreeMap<String, PersistedKeyValue>,
}

impl PersistedRecheckKey {
    fn try_from_key(key: &RecheckKey) -> anyhow::Result<Self> {
        let pk = key
            .pk
            .iter()
            .map(|(col, value)| {
                PersistedKeyValue::try_from_col_value(value).map(|value| (col.clone(), value))
            })
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        Ok(Self {
            schema: key.schema.clone(),
            tb: key.tb.clone(),
            is_delete: key.is_delete,
            pk,
        })
    }

    fn into_key(self) -> RecheckKey {
        RecheckKey {
            schema: self.schema,
            tb: self.tb,
            is_delete: self.is_delete,
            pk: self
                .pk
                .into_iter()
                .map(|(col, value)| (col, value.into_col_value()))
                .collect(),
        }
    }
}

pub(super) enum PreparedCheckpointWrite {
    PositionOnly {
        task_id: String,
        position: Position,
    },
    Full {
        commit: CheckerCheckpointCommit,
        persisted_identity_keys: BTreeSet<String>,
    },
}

impl<C: Checker> DataChecker<C> {
    const DEFAULT_CDC_LOG_MAX_FILE_SIZE: usize = 100 * 1024 * 1024;
    const DEFAULT_CDC_LOG_MAX_ROWS: usize = 1000;

    pub async fn snapshot_and_output(&mut self) -> anyhow::Result<()> {
        let max_file_size = usize::try_from(self.ctx.cdc_check_log_max_file_size)
            .ok()
            .filter(|v| *v > 0)
            .unwrap_or(Self::DEFAULT_CDC_LOG_MAX_FILE_SIZE);
        let max_rows = if self.ctx.cdc_check_log_max_rows == 0 {
            Self::DEFAULT_CDC_LOG_MAX_ROWS
        } else {
            self.ctx.cdc_check_log_max_rows
        };
        let mut miss_buf_builder = BoundedLineBuffer::new(max_file_size, Some(max_rows));
        let mut diff_buf_builder = BoundedLineBuffer::new(max_file_size, Some(max_rows));
        let mut sql_buf_builder = BoundedLineBuffer::new(max_file_size, None);
        let mut total_sql_count = 0usize;
        let mut total_miss = 0usize;
        let mut total_diff = 0usize;

        for entry in self.store.values() {
            if entry.is_miss() {
                total_miss += 1;
                miss_buf_builder.push_json(&entry.log);
            } else {
                total_diff += 1;
                diff_buf_builder.push_json(&entry.log);
            }
            if let Some(sql) = &entry.revise_sql {
                total_sql_count += 1;
                sql_buf_builder.push_str(sql);
            }
        }
        let miss_buf = miss_buf_builder.into_bytes();
        let diff_buf = diff_buf_builder.into_bytes();
        let sql_buf = sql_buf_builder.into_bytes();

        let summary = CheckSummaryLog {
            start_time: self.ctx.summary.start_time.clone(),
            end_time: chrono::Local::now().to_rfc3339(),
            is_consistent: total_miss == 0 && total_diff == 0,
            miss_count: total_miss,
            diff_count: total_diff,
            skip_count: self.ctx.summary.skip_count,
            sql_count: (total_sql_count > 0).then_some(total_sql_count),
        };
        self.ctx.summary = summary.clone();
        let summary_buf = serde_json::to_vec(&summary)?;

        Self::write_to_disk(
            &self.ctx.check_log_dir,
            &miss_buf,
            &diff_buf,
            &sql_buf,
            &summary_buf,
        )?;
        if self.ctx.s3_output.is_some() {
            self.upload_to_s3(&miss_buf, &diff_buf, &sql_buf, &summary_buf)
                .await?;
        }

        Ok(())
    }

    fn write_to_disk(
        dir: &str,
        miss_buf: &[u8],
        diff_buf: &[u8],
        sql_buf: &[u8],
        summary_buf: &[u8],
    ) -> anyhow::Result<()> {
        let path = std::path::Path::new(dir);
        std::fs::create_dir_all(path)?;
        std::fs::write(path.join("miss.log"), miss_buf)?;
        std::fs::write(path.join("diff.log"), diff_buf)?;
        let mut summary_with_newline = summary_buf.to_vec();
        summary_with_newline.push(b'\n');
        std::fs::write(path.join("summary.log"), &summary_with_newline)?;
        if !sql_buf.is_empty() {
            std::fs::write(path.join("sql.log"), sql_buf)?;
        }
        Ok(())
    }

    fn build_state_rows(&self) -> anyhow::Result<Vec<CheckerStateRow>> {
        let mut rows = Vec::with_capacity(self.store.len());
        for (store_key, entry) in &self.store {
            let identity_json = build_identity_json(entry)?;
            rows.push(CheckerStateRow {
                row_key: store_key.row_key,
                identity_key: hex::encode(openssl::sha::sha256(identity_json.as_bytes())),
                identity_json,
                payload: serde_json::to_string(&PersistedRecheckKey::try_from_key(&entry.key)?)?,
            });
        }
        Ok(rows)
    }

    pub fn restore_store_from_rows(&mut self, rows: Vec<CheckerStateRow>) -> anyhow::Result<()> {
        self.store.clear();
        let mut persisted_identity_keys = BTreeSet::new();
        for row in rows {
            persisted_identity_keys.insert(row.identity_key.clone());
            let key =
                serde_json::from_str::<PersistedRecheckKey>(&row.payload).with_context(|| {
                    format!(
                        "Checker [{}] failed to parse state row key [{}]",
                        self.name, row.row_key
                    )
                })?;
            let key = key.into_key();
            let entry = self.build_restored_entry(key.clone());
            let store_key = CheckerStoreKey::new(&key.schema, &key.tb, row.row_key);
            self.store.insert(store_key, entry);
        }
        self.persisted_identity_keys = Some(persisted_identity_keys);
        self.update_pending_counter();
        Ok(())
    }

    fn build_restored_entry(&self, key: RecheckKey) -> CheckEntry {
        let lookup_row = key.to_lookup_row();
        let source_row = self.ctx.reverse_router.route_row(lookup_row.clone());
        let id_col_values = match source_row.row_type {
            RowType::Delete => source_row.before.as_ref(),
            _ => source_row.after.as_ref(),
        }
        .map(|values| {
            values
                .iter()
                .map(|(col, value)| (col.clone(), value.to_option_string()))
                .collect()
        })
        .unwrap_or_default();
        let schema_changed =
            source_row.schema != lookup_row.schema || source_row.tb != lookup_row.tb;

        CheckEntry {
            key,
            log: CheckLog {
                schema: source_row.schema,
                tb: source_row.tb,
                target_schema: schema_changed.then(|| lookup_row.schema),
                target_tb: schema_changed.then(|| lookup_row.tb),
                id_col_values,
                diff_col_values: HashMap::new(),
                src_row: None,
                dst_row: None,
            },
            revise_sql: None,
            diff_cols: Some(Vec::new()),
        }
    }

    async fn refetch_source_rows(&self, keys: &[RecheckKey]) -> anyhow::Result<Vec<RowData>> {
        let source_checker = self
            .ctx
            .source_checker
            .clone()
            .context("missing source_checker for cdc recheck")?;
        let forward_router = self.ctx.reverse_router.reverse();
        let lookup_rows = keys
            .iter()
            .map(RecheckKey::to_lookup_row)
            .map(|row| self.ctx.reverse_router.route_row(row))
            .collect::<Vec<_>>();
        let mut grouped = HashMap::<(&str, &str), Vec<&RowData>>::new();
        for row in &lookup_rows {
            grouped
                .entry((row.schema.as_str(), row.tb.as_str()))
                .or_default()
                .push(row);
        }

        let mut checker = source_checker.lock().await;
        let mut rows = Vec::new();
        for group in grouped.into_values() {
            rows.extend(
                checker
                    .fetch(&group)
                    .await?
                    .dst_rows
                    .into_iter()
                    .map(|row| forward_router.route_row(row)),
            );
        }
        Ok(rows)
    }

    pub async fn load_initial_state(&mut self) -> anyhow::Result<bool> {
        if let Some(state_store) = &self.ctx.state_store {
            let task_id = &self.task_id;
            let rows = state_store.load_rows(task_id).await.with_context(|| {
                format!("failed to load checker state rows for task_id {}", task_id)
            })?;
            if !rows.is_empty() {
                if let Some(expected) = &self.ctx.expected_resume_position {
                    self.last_checkpoint_position = Some(expected.clone());
                }
                self.restore_store_from_rows(rows)?;
                log_info!(
                    "Checker [{}] restored {} store entries from state store",
                    self.name,
                    self.store.len()
                );
                return Ok(!self.store.is_empty());
            }
            self.persisted_identity_keys = Some(BTreeSet::new());
        }
        Ok(false)
    }

    pub async fn run_recheck(&mut self) -> anyhow::Result<()> {
        let keys_for_recheck: Vec<RecheckKey> =
            self.store.values().map(|entry| entry.key.clone()).collect();
        if keys_for_recheck.is_empty() {
            return Ok(());
        }

        log_info!(
            "Checker [{}] enters RECHECKING, replay {} unresolved keys",
            self.name,
            keys_for_recheck.len()
        );
        let batch_size = self.ctx.batch_size.max(1);
        for chunk in keys_for_recheck.chunks(batch_size) {
            let mut grouped = HashMap::<(&str, &str), Vec<RecheckKey>>::new();
            for key in chunk {
                grouped
                    .entry((key.schema.as_str(), key.tb.as_str()))
                    .or_default()
                    .push(key.clone());
            }
            for keys in grouped.into_values() {
                let lookup_rows = keys
                    .iter()
                    .map(RecheckKey::to_lookup_row)
                    .collect::<Vec<_>>();
                let lookup_refs = lookup_rows.iter().collect::<Vec<_>>();
                let fetch_result = self.checker.fetch(&lookup_refs).await?;
                let tb_meta = fetch_result.tb_meta;
                let source_rows = self.refetch_source_rows(&keys).await?;
                let mut source_map = HashMap::new();
                for row in source_rows {
                    if let Some(row_key) = Self::lookup_match_key(&row, tb_meta.basic())? {
                        source_map.insert(row_key, row);
                    }
                }
                let mut target_map = HashMap::new();
                for row in fetch_result.dst_rows {
                    if let Some(row_key) = Self::lookup_match_key(&row, tb_meta.basic())? {
                        target_map.insert(row_key, row);
                    }
                }

                for (key, lookup_row) in keys.iter().zip(lookup_rows.iter()) {
                    let Some(row_key) = Self::lookup_match_key(lookup_row, tb_meta.basic())? else {
                        continue;
                    };
                    match (source_map.remove(&row_key), target_map.remove(&row_key)) {
                        (Some(source_row), Some(target_row)) => {
                            if let Some(check_result) = Self::compare_src_dst(
                                &source_row,
                                Some(&target_row),
                                tb_meta.as_ref(),
                            )? {
                                let entry = Self::build_check_entry(
                                    check_result,
                                    &source_row,
                                    Some(&target_row),
                                    &mut self.ctx,
                                    tb_meta.as_ref(),
                                )
                                .await?;
                                self.store_entry(lookup_row, row_key, entry).await;
                            } else {
                                self.remove_store_entry(lookup_row, row_key);
                            }
                        }
                        (Some(source_row), None) => {
                            let entry = Self::build_check_entry(
                                CheckInconsistency::Miss,
                                &source_row,
                                None,
                                &mut self.ctx,
                                tb_meta.as_ref(),
                            )
                            .await?;
                            self.store_entry(lookup_row, row_key, entry).await;
                        }
                        (None, Some(_)) => {
                            let entry = self.build_restored_entry(key.clone());
                            self.store_entry(lookup_row, row_key, entry).await;
                        }
                        (None, None) => {
                            self.remove_store_entry(lookup_row, row_key);
                        }
                    }
                }
            }
            if self.store_dirty {
                if let Some(position) = self.last_checkpoint_position.clone() {
                    self.record_checkpoint(position).await.with_context(|| {
                        format!("Checker [{}] failed to persist recheck progress", self.name)
                    })?;
                    self.store_dirty = false;
                }
            }
        }
        log_info!("Checker [{}] RECHECKING finished", self.name);
        Ok(())
    }

    pub(super) fn prepare_checkpoint_write(
        &self,
        position: Position,
    ) -> anyhow::Result<PreparedCheckpointWrite> {
        if !self.store_dirty {
            return Ok(PreparedCheckpointWrite::PositionOnly {
                task_id: self.task_id.clone(),
                position,
            });
        }

        let rows = self.build_state_rows()?;
        let live_keys: BTreeSet<String> = rows.iter().map(|row| row.identity_key.clone()).collect();
        let deletes = self
            .persisted_identity_keys
            .as_ref()
            .into_iter()
            .flatten()
            .filter(|identity_key| !live_keys.contains(*identity_key))
            .cloned()
            .collect();

        Ok(PreparedCheckpointWrite::Full {
            commit: CheckerCheckpointCommit {
                task_id: self.task_id.clone(),
                position,
                upserts: rows,
                deletes,
            },
            persisted_identity_keys: live_keys,
        })
    }

    pub async fn record_checkpoint(&mut self, position: Position) -> anyhow::Result<()> {
        if matches!(position, Position::None) {
            return Ok(());
        }
        self.last_checkpoint_position = Some(position.clone());
        let Some(state_store) = self.ctx.state_store.clone() else {
            return Ok(());
        };

        let checkpoint_write = self.prepare_checkpoint_write(position)?;

        match checkpoint_write {
            PreparedCheckpointWrite::PositionOnly { task_id, position } => {
                state_store.commit_position(&task_id, &position).await?;
            }
            PreparedCheckpointWrite::Full {
                commit,
                persisted_identity_keys,
            } => {
                state_store.commit_checkpoint(&commit).await?;
                self.store_dirty = false;
                self.persisted_identity_keys = Some(persisted_identity_keys);
            }
        }
        Ok(())
    }

    async fn upload_to_s3(
        &self,
        miss_buf: &[u8],
        diff_buf: &[u8],
        sql_buf: &[u8],
        summary_buf: &[u8],
    ) -> anyhow::Result<()> {
        let Some((s3_client, key_prefix)) = &self.ctx.s3_output else {
            return Ok(());
        };
        let p = key_prefix;
        let miss_key = format!("{p}/miss.log");
        let diff_key = format!("{p}/diff.log");
        let summary_key = format!("{p}/summary.log");
        if sql_buf.is_empty() {
            tokio::try_join!(
                s3_client.write(&miss_key, miss_buf.to_vec()),
                s3_client.write(&diff_key, diff_buf.to_vec()),
                s3_client.write(&summary_key, summary_buf.to_vec()),
            )?;
        } else {
            let sql_key = format!("{p}/sql.log");
            tokio::try_join!(
                s3_client.write(&miss_key, miss_buf.to_vec()),
                s3_client.write(&diff_key, diff_buf.to_vec()),
                s3_client.write(&sql_key, sql_buf.to_vec()),
                s3_client.write(&summary_key, summary_buf.to_vec()),
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::Value;
    use tokio::sync::mpsc;

    fn build_recheck_key() -> RecheckKey {
        RecheckKey {
            schema: "target_db".to_string(),
            tb: "target_tb".to_string(),
            is_delete: false,
            pk: BTreeMap::from([
                ("a".to_string(), ColValue::Long(1)),
                ("b".to_string(), ColValue::String("2".to_string())),
            ]),
        }
    }

    #[test]
    fn identity_json_ref_preserves_sorted_id_columns() {
        let entry = CheckEntry {
            key: build_recheck_key(),
            log: CheckLog {
                schema: "test_db".to_string(),
                tb: "test_tb".to_string(),
                target_schema: None,
                target_tb: None,
                id_col_values: HashMap::from([
                    ("b".to_string(), Some("2".to_string())),
                    ("a".to_string(), Some("1".to_string())),
                ]),
                diff_col_values: HashMap::new(),
                src_row: None,
                dst_row: None,
            },
            revise_sql: None,
            diff_cols: Some(vec!["v".to_string()]),
        };

        let identity_json = serde_json::to_string(&build_identity_json_ref(&entry)).unwrap();
        assert_eq!(
            identity_json,
            r#"{"schema":"test_db","tb":"test_tb","id_col_values":{"a":"1","b":"2"}}"#
        );
    }

    #[test]
    fn persisted_recheck_key_omits_full_rows_and_logs() {
        let payload =
            serde_json::to_value(PersistedRecheckKey::try_from_key(&build_recheck_key()).unwrap())
                .unwrap();
        let Value::Object(payload) = payload else {
            panic!("payload should be a json object");
        };

        assert!(payload.contains_key("schema"));
        assert!(payload.contains_key("tb"));
        assert!(payload.contains_key("is_delete"));
        assert!(payload.contains_key("pk"));
    }

    struct StaticChecker {
        tb_meta: Arc<CheckerTbMeta>,
        rows: Vec<RowData>,
    }

    #[async_trait]
    impl Checker for StaticChecker {
        async fn fetch(&mut self, _src_rows: &[&RowData]) -> anyhow::Result<FetchResult> {
            Ok(FetchResult {
                tb_meta: self.tb_meta.clone(),
                dst_rows: self.rows.clone(),
            })
        }
    }

    #[tokio::test]
    async fn run_recheck_removes_key_when_source_and_target_are_both_missing() {
        let tb_meta = Arc::new(CheckerTbMeta::Mongo(
            dt_common::meta::rdb_tb_meta::RdbTbMeta {
                schema: "target_db".to_string(),
                tb: "target_tb".to_string(),
                id_cols: vec!["a".to_string(), "b".to_string()],
                ..Default::default()
            },
        ));
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let mut checker = DataChecker::new(
            StaticChecker {
                tb_meta: tb_meta.clone(),
                rows: Vec::new(),
            },
            "task-1".to_string(),
            CheckContext {
                monitor: Arc::new(Monitor::new("checker", "unit-test", 1, 1, 1)),
                task_monitor: None,
                summary: CheckSummaryLog {
                    start_time: "unit-test".to_string(),
                    ..Default::default()
                },
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
                retry_interval_secs: 0,
                max_retries: 0,
                is_cdc: true,
                check_log_dir: String::new(),
                cdc_check_log_max_file_size: 1,
                cdc_check_log_max_rows: 1,
                s3_output: None,
                cdc_check_log_interval_secs: 1,
                state_store: None,
                source_checker: Some(Arc::new(Mutex::new(Box::new(StaticChecker {
                    tb_meta: tb_meta.clone(),
                    rows: Vec::new(),
                })))),
                expected_resume_position: None,
            },
            super::super::CheckerIo {
                batch_queue: Arc::new(std::sync::Mutex::new(LimitedQueue::new(1))),
                batch_notify: Arc::new(tokio::sync::Notify::new()),
                control_rx,
            },
            "unit-test",
        );
        let key = build_recheck_key();
        let row_key =
            DataChecker::<StaticChecker>::lookup_match_key(&key.to_lookup_row(), tb_meta.basic())
                .unwrap()
                .unwrap();
        checker.store.insert(
            CheckerStoreKey::new(&key.schema, &key.tb, row_key),
            checker.build_restored_entry(key),
        );

        checker.run_recheck().await.unwrap();
        assert!(checker.store.is_empty());
    }
}
