use super::*;

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
            rows.push(CheckerStateRow {
                row_key: *key,
                payload: serde_json::to_string(entry)?,
            });
        }
        Ok(rows)
    }

    fn restore_store_from_rows(&mut self, rows: Vec<CheckerStateRow>) {
        self.store.clear();
        let keep_from = rows.len().saturating_sub(CHECKER_MAX_STORE_SIZE);
        for row in rows.into_iter().skip(keep_from) {
            match serde_json::from_str::<CheckEntry>(&row.payload) {
                Ok(entry) => {
                    self.store.insert(row.row_key, entry);
                }
                Err(err) => {
                    log_warn!(
                        "Checker [{}] failed to parse state row key [{}]: {}",
                        self.name,
                        row.row_key,
                        err
                    );
                }
            }
        }
    }

    pub(super) async fn load_initial_state(&mut self) -> anyhow::Result<bool> {
        if let Some(state_store) = &self.ctx.state_store {
            let task_id = &self.task_id;
            if let Some(bundle) = state_store
                .load_latest_checkpoint(task_id)
                .await
                .with_context(|| {
                    format!(
                        "failed to load latest checker checkpoint for task_id {}",
                        task_id
                    )
                })?
            {
                let manifest_position = match Position::from_str(&bundle.manifest.position) {
                    Ok(position) if !matches!(position, Position::None) => Some(position),
                    Ok(_) => None,
                    Err(err) => {
                        log_warn!(
                            "Checker [{}] failed to parse checkpoint position [{}]: {}",
                            self.name,
                            bundle.manifest.position,
                            err
                        );
                        None
                    }
                };
                if let Some(expected) = &self.ctx.expected_resume_position {
                    if manifest_position.as_ref() != Some(expected) {
                        log_warn!(
                            "Checker [{}] state store checkpoint position [{}] mismatches resumer position [{}], ignore checker state and start empty store",
                            self.name,
                            bundle.manifest.position,
                            expected
                        );
                        self.last_checkpoint_position = Some(expected.clone());
                        return Ok(false);
                    }
                }
                self.restore_store_from_rows(bundle.rows);
                self.evicted_miss = bundle.manifest.evicted_miss;
                self.evicted_diff = bundle.manifest.evicted_diff;
                self.last_checkpoint_position = manifest_position;
                log_info!(
                    "Checker [{}] restored {} store entries from state store checkpoint [{}]",
                    self.name,
                    self.store.len(),
                    bundle.manifest.checkpoint_id
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
                    if let Err(err) = self.record_checkpoint(position).await {
                        log_warn!(
                            "Checker [{}] failed to persist recheck progress: {}",
                            self.name,
                            err
                        );
                    } else {
                        self.store_dirty = false;
                    }
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
        let commit = CheckerCheckpointCommit {
            task_id: self.task_id.clone(),
            checkpoint_id: {
                self.checkpoint_seq = self.checkpoint_seq.wrapping_add(1);
                format!(
                    "{}-{}",
                    chrono::Utc::now().timestamp_millis(),
                    self.checkpoint_seq
                )
            },
            position,
            rows: self.build_state_rows()?,
            evicted_miss: self.evicted_miss,
            evicted_diff: self.evicted_diff,
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
