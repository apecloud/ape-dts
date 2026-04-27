use anyhow::Context;
use std::collections::{BTreeSet, HashMap};
use std::fmt::Write;

use super::{
    BoundedLineBuffer, CheckEntry, CheckInconsistency, Checker, CheckerStoreKey, DataChecker,
    RecheckKey,
};
use crate::checker::check_log::{CheckLog, CheckSummaryLog};
use crate::checker::state_store::{CheckerCheckpointCommit, CheckerStateRow};
use dt_common::meta::{position::Position, row_data::RowData, row_type::RowType};
use dt_common::{log_info, log_warn};

fn push_json_string(buf: &mut String, value: &str) {
    buf.push('"');
    for ch in value.chars() {
        match ch {
            '"' => buf.push_str("\\\""),
            '\\' => buf.push_str("\\\\"),
            '\u{08}' => buf.push_str("\\b"),
            '\u{0C}' => buf.push_str("\\f"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            '\t' => buf.push_str("\\t"),
            c if c <= '\u{1F}' => write!(buf, "\\u{:04x}", c as u32).unwrap(),
            c => buf.push(c),
        }
    }
    buf.push('"');
}

fn build_identity_json_from_parts(
    schema: &str,
    tb: &str,
    id_col_values: &[(&str, &Option<String>)],
) -> String {
    let mut id_col_values = id_col_values.to_vec();
    id_col_values.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    let mut buf = String::with_capacity(schema.len() + tb.len() + id_col_values.len() * 32 + 40);
    buf.push_str(r#"{"schema":"#);
    push_json_string(&mut buf, schema);
    buf.push_str(r#","tb":"#);
    push_json_string(&mut buf, tb);
    buf.push_str(r#","id_col_values":{"#);
    for (idx, (key, value)) in id_col_values.iter().enumerate() {
        if idx > 0 {
            buf.push(',');
        }
        push_json_string(&mut buf, key);
        buf.push(':');
        match value {
            Some(value) => push_json_string(&mut buf, value),
            None => buf.push_str("null"),
        }
    }
    buf.push_str("}}");
    buf
}

pub(super) fn build_identity_json(entry: &CheckEntry) -> String {
    let id_col_values = entry
        .log
        .id_col_values
        .iter()
        .map(|(key, value)| (key.as_str(), value))
        .collect::<Vec<_>>();
    build_identity_json_from_parts(&entry.log.schema, &entry.log.tb, &id_col_values)
}

pub(super) fn build_identity_key(entry: &CheckEntry) -> String {
    hex::encode(openssl::sha::sha256(build_identity_json(entry).as_bytes()))
}

fn build_state_row(
    store_key: &CheckerStoreKey,
    entry: &CheckEntry,
) -> anyhow::Result<CheckerStateRow> {
    Ok(CheckerStateRow {
        row_key: store_key.row_key,
        identity_key: build_identity_key(entry),
        payload: serde_json::to_string(&entry.key)?,
    })
}

enum PreparedCheckpointWrite {
    PositionOnly { task_id: String, position: Position },
    Full { commit: CheckerCheckpointCommit },
}

impl<C: Checker> DataChecker<C> {
    const DEFAULT_CDC_LOG_MAX_FILE_SIZE: usize = 100 * 1024 * 1024;
    const DEFAULT_CDC_LOG_MAX_ROWS: usize = 1000;

    /// Writes a point-in-time snapshot whose miss/diff counts reflect current unresolved entries rather than cumulative metrics.
    pub async fn snapshot_and_output(&mut self) -> anyhow::Result<()> {
        if self.init_failed {
            log_warn!(
                "Checker [{}] skipping latest snapshot publish because CDC state initialization failed",
                self.name
            );
            return Ok(());
        }
        self.account_dropped_item_skips();
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
            is_consistent: false,
            miss_count: total_miss,
            diff_count: total_diff,
            skip_count: self.ctx.summary.skip_count,
            sql_count: (total_sql_count > 0).then_some(total_sql_count),
        };
        let mut summary = summary;
        summary.is_consistent = super::is_summary_consistent(&summary, self.init_failed);
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
        } else if let Err(err) = std::fs::remove_file(path.join("sql.log")) {
            if err.kind() != std::io::ErrorKind::NotFound {
                return Err(err.into());
            }
        }
        Ok(())
    }

    fn build_dirty_state_rows(&self) -> anyhow::Result<Vec<CheckerStateRow>> {
        let mut rows = Vec::with_capacity(self.dirty_upserts.len());
        for store_key in &self.dirty_upserts {
            let Some(entry) = self.store.get(store_key) else {
                continue;
            };
            rows.push(build_state_row(store_key, entry)?);
        }
        Ok(rows)
    }

    pub fn restore_store_from_rows(&mut self, rows: Vec<CheckerStateRow>) -> anyhow::Result<()> {
        self.store.clear();
        let mut persisted_identity_keys = BTreeSet::new();
        for row in rows {
            persisted_identity_keys.insert(row.identity_key.clone());
            let key = serde_json::from_str::<RecheckKey>(&row.payload).with_context(|| {
                format!(
                    "Checker [{}] failed to parse state row key [{}]",
                    self.name, row.row_key
                )
            })?;
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
                target_schema: schema_changed.then_some(lookup_row.schema),
                target_tb: schema_changed.then_some(lookup_row.tb),
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

    fn prepare_checkpoint_write(
        &self,
        position: Position,
    ) -> anyhow::Result<PreparedCheckpointWrite> {
        if !self.store_dirty {
            return Ok(PreparedCheckpointWrite::PositionOnly {
                task_id: self.task_id.clone(),
                position,
            });
        }

        let rows = self.build_dirty_state_rows()?;
        let deletes = self.dirty_deletes.values().cloned().collect();

        Ok(PreparedCheckpointWrite::Full {
            commit: CheckerCheckpointCommit {
                task_id: self.task_id.clone(),
                position,
                upserts: rows,
                deletes,
            },
        })
    }

    pub async fn record_checkpoint(&mut self, position: Position) -> anyhow::Result<()> {
        if matches!(position, Position::None) {
            return Ok(());
        }
        self.last_checkpoint_position = Some(position.clone());
        if self.init_failed {
            return Ok(());
        }
        let Some(state_store) = self.ctx.state_store.clone() else {
            return Ok(());
        };

        let checkpoint_write = self.prepare_checkpoint_write(position)?;

        match checkpoint_write {
            PreparedCheckpointWrite::PositionOnly { task_id, position } => {
                state_store.commit_position(&task_id, &position).await?;
            }
            PreparedCheckpointWrite::Full { commit } => {
                state_store.commit_checkpoint(&commit).await?;
                if let Some(persisted_identity_keys) = self.persisted_identity_keys.as_mut() {
                    for identity_key in &commit.deletes {
                        persisted_identity_keys.remove(identity_key);
                    }
                    for row in &commit.upserts {
                        persisted_identity_keys.insert(row.identity_key.clone());
                    }
                }
                self.store_dirty = false;
                self.dirty_upserts.clear();
                self.dirty_deletes.clear();
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
            s3_client.delete(&format!("{p}/sql.log")).await?;
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
    use super::super::{CheckContext, Checker, CheckerIo, CheckerTbMeta, DataChecker, FetchResult};
    use super::*;
    use crate::checker::check_log::CheckSummaryLog;
    use crate::rdb_router::RdbRouter;
    use async_trait::async_trait;
    use dt_common::{monitor::task_monitor::TaskMonitorHandle, utils::limit_queue::LimitedQueue};
    use opendal::{services::Memory, Operator};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::mpsc;

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

    fn unique_temp_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("ape-dts-{name}-{nanos}"))
    }

    fn read_file(path: &Path) -> String {
        fs::read_to_string(path).unwrap()
    }

    fn build_memory_operator() -> Operator {
        Operator::new(Memory::default()).unwrap().finish()
    }

    fn build_cdc_checker(
        check_log_dir: PathBuf,
        s3_output: Option<(Operator, String)>,
    ) -> DataChecker<StaticChecker> {
        let tb_meta = Arc::new(CheckerTbMeta::Mongo(
            dt_common::meta::rdb_tb_meta::RdbTbMeta {
                schema: "target_db".to_string(),
                tb: "target_tb".to_string(),
                id_cols: vec!["id".to_string()],
                ..Default::default()
            },
        ));
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        DataChecker::new(
            StaticChecker {
                tb_meta,
                rows: Vec::new(),
            },
            "task-1".to_string(),
            CheckContext {
                monitor: TaskMonitorHandle::default(),
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
                check_log_dir: check_log_dir.display().to_string(),
                cdc_check_log_max_file_size: 1024,
                cdc_check_log_max_rows: 100,
                s3_output,
                cdc_check_log_interval_secs: 1,
                state_store: None,
                source_checker: None,
                expected_resume_position: None,
            },
            CheckerIo {
                batch_queue: Arc::new(std::sync::Mutex::new(LimitedQueue::new(1))),
                batch_notify: Arc::new(tokio::sync::Notify::new()),
                dropped_items: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                control_rx,
            },
            "unit-test",
        )
    }

    #[tokio::test]
    async fn snapshot_and_output_removes_stale_sql_log_locally_and_on_s3_when_empty() {
        let dir = unique_temp_dir("checker-empty-sql-local");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("sql.log"), "stale sql;\n").unwrap();
        let op = build_memory_operator();
        op.write("prefix/sql.log", "stale sql;\n").await.unwrap();
        let mut checker = build_cdc_checker(dir.clone(), Some((op.clone(), "prefix".to_string())));

        checker.snapshot_and_output().await.unwrap();

        assert!(!dir.join("sql.log").exists());
        assert!(op.stat("prefix/sql.log").await.is_err());
        fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn snapshot_and_output_skips_local_and_s3_publish_when_init_failed() {
        let dir = unique_temp_dir("checker-init-failed");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("miss.log"), "old miss\n").unwrap();
        fs::write(dir.join("diff.log"), "old diff\n").unwrap();
        fs::write(dir.join("summary.log"), "old summary\n").unwrap();
        fs::write(dir.join("sql.log"), "old sql;\n").unwrap();
        let op = build_memory_operator();
        op.write("prefix/miss.log", "old miss\n").await.unwrap();
        op.write("prefix/diff.log", "old diff\n").await.unwrap();
        op.write("prefix/summary.log", "old summary\n")
            .await
            .unwrap();
        op.write("prefix/sql.log", "old sql;\n").await.unwrap();

        let mut checker = build_cdc_checker(dir.clone(), Some((op.clone(), "prefix".to_string())));
        checker.init_failed = true;

        checker.snapshot_and_output().await.unwrap();

        assert_eq!(read_file(&dir.join("miss.log")), "old miss\n");
        assert_eq!(read_file(&dir.join("diff.log")), "old diff\n");
        assert_eq!(read_file(&dir.join("summary.log")), "old summary\n");
        assert_eq!(read_file(&dir.join("sql.log")), "old sql;\n");
        assert_eq!(
            String::from_utf8(op.read("prefix/miss.log").await.unwrap().to_vec()).unwrap(),
            "old miss\n"
        );
        assert_eq!(
            String::from_utf8(op.read("prefix/diff.log").await.unwrap().to_vec()).unwrap(),
            "old diff\n"
        );
        assert_eq!(
            String::from_utf8(op.read("prefix/summary.log").await.unwrap().to_vec()).unwrap(),
            "old summary\n"
        );
        assert_eq!(
            String::from_utf8(op.read("prefix/sql.log").await.unwrap().to_vec()).unwrap(),
            "old sql;\n"
        );
        fs::remove_dir_all(dir).unwrap();
    }
}
