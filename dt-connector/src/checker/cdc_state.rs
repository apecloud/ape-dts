use super::*;
use anyhow::Context;
use std::collections::BTreeSet;

fn build_identity_json(entry: &CheckEntry) -> anyhow::Result<String> {
    serde_json::to_string(&serde_json::json!({
        "schema": entry.log.schema,
        "tb": entry.log.tb,
        "id_col_values": entry.log.id_col_values,
    }))
    .map_err(anyhow::Error::from)
}

fn build_identity_key(identity_json: &str) -> String {
    hex::encode(openssl::sha::sha256(identity_json.as_bytes()))
}

impl<C: Checker> DataChecker<C> {
    const DEFAULT_CDC_LOG_MAX_FILE_SIZE: usize = 100 * 1024 * 1024;
    const DEFAULT_CDC_LOG_MAX_ROWS: usize = 1000;

    pub(super) async fn snapshot_and_output(&self) -> anyhow::Result<()> {
        let max_file_size = usize::try_from(self.ctx.cdc_check_log_max_file_size)
            .ok()
            .filter(|v| *v > 0)
            .unwrap_or(Self::DEFAULT_CDC_LOG_MAX_FILE_SIZE);
        let max_rows = if self.ctx.cdc_check_log_max_rows == 0 {
            Self::DEFAULT_CDC_LOG_MAX_ROWS
        } else {
            self.ctx.cdc_check_log_max_rows
        };
        let mut miss_buf_builder = BoundedNdjsonBuffer::new(max_file_size, max_rows);
        let mut diff_buf_builder = BoundedNdjsonBuffer::new(max_file_size, max_rows);
        let mut sql_lines = Vec::new();
        let mut table_counts: HashMap<String, TableCheckCount> = HashMap::new();

        for entry in self.store.values() {
            let tb_key = format!("{}.{}", entry.log.schema, entry.log.tb);
            let counts = table_counts.entry(tb_key).or_default();

            if entry.is_miss {
                counts.miss_count += 1;
                miss_buf_builder.push(&entry.log);
            } else {
                counts.diff_count += 1;
                diff_buf_builder.push(&entry.log);
            }
            if let Some(sql) = &entry.revise_sql {
                sql_lines.push(sql.clone());
            }
        }
        let miss_buf = miss_buf_builder.into_bytes();
        let diff_buf = diff_buf_builder.into_bytes();
        let sql_buf = if sql_lines.is_empty() {
            Vec::new()
        } else {
            let mut buf = sql_lines.join("\n").into_bytes();
            buf.push(b'\n');
            buf
        };

        let total_miss: usize =
            table_counts.values().map(|c| c.miss_count).sum::<usize>() + self.evicted_miss;
        let total_diff: usize =
            table_counts.values().map(|c| c.diff_count).sum::<usize>() + self.evicted_diff;

        let summary = CheckSummaryLog {
            start_time: self.ctx.summary.start_time.clone(),
            end_time: chrono::Local::now().to_rfc3339(),
            is_consistent: total_miss == 0 && total_diff == 0,
            miss_count: total_miss,
            diff_count: total_diff,
            skip_count: self.ctx.summary.skip_count,
            tables: table_counts,
            sql_count: (!sql_lines.is_empty()).then_some(sql_lines.len()),
        };
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
        std::fs::write(path.join("sql.log"), sql_buf)?;
        Ok(())
    }

    fn build_state_rows(&self) -> anyhow::Result<Vec<CheckerStateRow>> {
        let mut rows = Vec::with_capacity(self.store.len());
        for (key, entry) in &self.store {
            let identity_json = build_identity_json(entry)?;
            rows.push(CheckerStateRow {
                row_key: *key,
                identity_key: build_identity_key(&identity_json),
                identity_json,
                payload: serde_json::to_string(entry)?,
            });
        }
        Ok(rows)
    }

    pub(super) fn restore_store_from_rows(
        &mut self,
        rows: Vec<CheckerStateRow>,
    ) -> anyhow::Result<()> {
        self.store.clear();
        let keep_from = rows.len().saturating_sub(CHECKER_MAX_STORE_SIZE);
        for row in rows.into_iter().skip(keep_from) {
            let entry = serde_json::from_str::<CheckEntry>(&row.payload).with_context(|| {
                format!(
                    "Checker [{}] failed to parse state row key [{}]",
                    self.name, row.row_key
                )
            })?;
            self.store.insert(row.row_key, entry);
        }
        self.update_pending_counter();
        Ok(())
    }

    pub(super) async fn load_initial_state(&mut self) -> anyhow::Result<bool> {
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
        }
        Ok(false)
    }

    pub(super) async fn run_recheck(&mut self) -> anyhow::Result<()> {
        let rows_for_recheck: Vec<RowData> = self
            .store
            .values()
            .map(|entry| entry.src_row_data.clone())
            .collect();
        if rows_for_recheck.is_empty() {
            return Ok(());
        }

        log_info!(
            "Checker [{}] enters RECHECKING, replay {} unresolved rows",
            self.name,
            rows_for_recheck.len()
        );
        let batch_size = self.ctx.batch_size.max(1);
        for chunk in rows_for_recheck.chunks(batch_size) {
            self.process_batch(chunk, false).await?;
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

    pub(super) async fn record_checkpoint(&mut self, position: Position) -> anyhow::Result<()> {
        if matches!(position, Position::None) {
            return Ok(());
        }
        self.last_checkpoint_position = Some(position.clone());
        let Some(state_store) = self.ctx.state_store.clone() else {
            return Ok(());
        };
        let rows = self.build_state_rows()?;
        let live_keys: BTreeSet<String> = rows.iter().map(|row| row.identity_key.clone()).collect();
        let existing_rows = state_store.load_rows(&self.task_id).await?;
        let deletes = existing_rows
            .into_iter()
            .map(|row| row.identity_key)
            .filter(|identity_key| !live_keys.contains(identity_key))
            .collect();
        let commit = CheckerCheckpointCommit {
            task_id: self.task_id.clone(),
            position,
            upserts: rows,
            deletes,
        };
        state_store.commit_checkpoint(&commit).await?;
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
        let sql_key = format!("{p}/sql.log");
        let summary_key = format!("{p}/summary.log");
        tokio::try_join!(
            s3_client.write(&miss_key, miss_buf.to_vec()),
            s3_client.write(&diff_key, diff_buf.to_vec()),
            s3_client.write(&sql_key, sql_buf.to_vec()),
            s3_client.write(&summary_key, summary_buf.to_vec()),
        )?;
        Ok(())
    }
}
