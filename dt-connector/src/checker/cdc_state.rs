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

#[derive(Clone, Serialize, Deserialize)]
enum PersistedColValue {
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
    Float(f32),
    Double(f64),
    Decimal(String),
    Time(String),
    Date(String),
    DateTime(String),
    Timestamp(String),
    Year(u16),
    String(String),
    RawString(Vec<u8>),
    Blob(Vec<u8>),
    Bit(u64),
    Set(u64),
    Enum(u32),
    Set2(String),
    Enum2(String),
    Json(Vec<u8>),
    Json2(String),
    Json3(serde_json::Value),
    MongoDoc(mongodb::bson::Document),
}

impl From<&ColValue> for PersistedColValue {
    fn from(value: &ColValue) -> Self {
        match value {
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
            ColValue::Float(v) => Self::Float(*v),
            ColValue::Double(v) => Self::Double(*v),
            ColValue::Decimal(v) => Self::Decimal(v.clone()),
            ColValue::Time(v) => Self::Time(v.clone()),
            ColValue::Date(v) => Self::Date(v.clone()),
            ColValue::DateTime(v) => Self::DateTime(v.clone()),
            ColValue::Timestamp(v) => Self::Timestamp(v.clone()),
            ColValue::Year(v) => Self::Year(*v),
            ColValue::String(v) => Self::String(v.clone()),
            ColValue::RawString(v) => Self::RawString(v.clone()),
            ColValue::Blob(v) => Self::Blob(v.clone()),
            ColValue::Bit(v) => Self::Bit(*v),
            ColValue::Set(v) => Self::Set(*v),
            ColValue::Enum(v) => Self::Enum(*v),
            ColValue::Set2(v) => Self::Set2(v.clone()),
            ColValue::Enum2(v) => Self::Enum2(v.clone()),
            ColValue::Json(v) => Self::Json(v.clone()),
            ColValue::Json2(v) => Self::Json2(v.clone()),
            ColValue::Json3(v) => Self::Json3(v.clone()),
            ColValue::MongoDoc(v) => Self::MongoDoc(v.clone()),
        }
    }
}

impl From<PersistedColValue> for ColValue {
    fn from(value: PersistedColValue) -> Self {
        match value {
            PersistedColValue::None => Self::None,
            PersistedColValue::Bool(v) => Self::Bool(v),
            PersistedColValue::Tiny(v) => Self::Tiny(v),
            PersistedColValue::UnsignedTiny(v) => Self::UnsignedTiny(v),
            PersistedColValue::Short(v) => Self::Short(v),
            PersistedColValue::UnsignedShort(v) => Self::UnsignedShort(v),
            PersistedColValue::Long(v) => Self::Long(v),
            PersistedColValue::UnsignedLong(v) => Self::UnsignedLong(v),
            PersistedColValue::LongLong(v) => Self::LongLong(v),
            PersistedColValue::UnsignedLongLong(v) => Self::UnsignedLongLong(v),
            PersistedColValue::Float(v) => Self::Float(v),
            PersistedColValue::Double(v) => Self::Double(v),
            PersistedColValue::Decimal(v) => Self::Decimal(v),
            PersistedColValue::Time(v) => Self::Time(v),
            PersistedColValue::Date(v) => Self::Date(v),
            PersistedColValue::DateTime(v) => Self::DateTime(v),
            PersistedColValue::Timestamp(v) => Self::Timestamp(v),
            PersistedColValue::Year(v) => Self::Year(v),
            PersistedColValue::String(v) => Self::String(v),
            PersistedColValue::RawString(v) => Self::RawString(v),
            PersistedColValue::Blob(v) => Self::Blob(v),
            PersistedColValue::Bit(v) => Self::Bit(v),
            PersistedColValue::Set(v) => Self::Set(v),
            PersistedColValue::Enum(v) => Self::Enum(v),
            PersistedColValue::Set2(v) => Self::Set2(v),
            PersistedColValue::Enum2(v) => Self::Enum2(v),
            PersistedColValue::Json(v) => Self::Json(v),
            PersistedColValue::Json2(v) => Self::Json2(v),
            PersistedColValue::Json3(v) => Self::Json3(v),
            PersistedColValue::MongoDoc(v) => Self::MongoDoc(v),
        }
    }
}

fn persist_col_values(
    col_values: &Option<HashMap<String, ColValue>>,
) -> Option<HashMap<String, PersistedColValue>> {
    col_values.as_ref().map(|values| {
        values
            .iter()
            .map(|(col, value)| (col.clone(), PersistedColValue::from(value)))
            .collect()
    })
}

fn restore_col_values(
    col_values: Option<HashMap<String, PersistedColValue>>,
) -> Option<HashMap<String, ColValue>> {
    col_values.map(|values| {
        values
            .into_iter()
            .map(|(col, value)| (col, ColValue::from(value)))
            .collect()
    })
}

#[derive(Clone, Serialize, Deserialize)]
struct PersistedRowData {
    schema: String,
    tb: String,
    row_type: RowType,
    before: Option<HashMap<String, PersistedColValue>>,
    after: Option<HashMap<String, PersistedColValue>>,
    data_size: usize,
}

impl From<&RowData> for PersistedRowData {
    fn from(row_data: &RowData) -> Self {
        Self {
            schema: row_data.schema.clone(),
            tb: row_data.tb.clone(),
            row_type: row_data.row_type.clone(),
            before: persist_col_values(&row_data.before),
            after: persist_col_values(&row_data.after),
            data_size: row_data.data_size,
        }
    }
}

impl From<PersistedRowData> for RowData {
    fn from(row_data: PersistedRowData) -> Self {
        RowData {
            schema: row_data.schema,
            tb: row_data.tb,
            row_type: row_data.row_type,
            before: restore_col_values(row_data.before),
            after: restore_col_values(row_data.after),
            data_size: row_data.data_size,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct PersistedCheckLog {
    schema: String,
    tb: String,
    target_schema: Option<String>,
    target_tb: Option<String>,
    id_col_values: HashMap<String, Option<String>>,
    diff_col_values: HashMap<String, DiffColValue>,
    src_row: Option<HashMap<String, PersistedColValue>>,
    dst_row: Option<HashMap<String, PersistedColValue>>,
}

impl From<&CheckLog> for PersistedCheckLog {
    fn from(log: &CheckLog) -> Self {
        Self {
            schema: log.schema.clone(),
            tb: log.tb.clone(),
            target_schema: log.target_schema.clone(),
            target_tb: log.target_tb.clone(),
            id_col_values: log.id_col_values.clone(),
            diff_col_values: log.diff_col_values.clone(),
            src_row: persist_col_values(&log.src_row),
            dst_row: persist_col_values(&log.dst_row),
        }
    }
}

impl From<PersistedCheckLog> for CheckLog {
    fn from(log: PersistedCheckLog) -> Self {
        Self {
            schema: log.schema,
            tb: log.tb,
            target_schema: log.target_schema,
            target_tb: log.target_tb,
            id_col_values: log.id_col_values,
            diff_col_values: log.diff_col_values,
            src_row: restore_col_values(log.src_row),
            dst_row: restore_col_values(log.dst_row),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct PersistedCheckEntry {
    log: PersistedCheckLog,
    revise_sql: Option<String>,
    is_miss: bool,
    src_row_data: PersistedRowData,
}

impl From<&CheckEntry> for PersistedCheckEntry {
    fn from(entry: &CheckEntry) -> Self {
        Self {
            log: PersistedCheckLog::from(&entry.log),
            revise_sql: entry.revise_sql.clone(),
            is_miss: entry.is_miss,
            src_row_data: PersistedRowData::from(&entry.src_row_data),
        }
    }
}

impl From<PersistedCheckEntry> for CheckEntry {
    fn from(entry: PersistedCheckEntry) -> Self {
        Self {
            log: CheckLog::from(entry.log),
            revise_sql: entry.revise_sql,
            is_miss: entry.is_miss,
            src_row_data: RowData::from(entry.src_row_data),
        }
    }
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

        let total_miss: usize = table_counts.values().map(|c| c.miss_count).sum();
        let total_diff: usize = table_counts.values().map(|c| c.diff_count).sum();

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
                payload: serde_json::to_string(&PersistedCheckEntry::from(entry))?,
            });
        }
        Ok(rows)
    }

    pub(super) fn restore_store_from_rows(
        &mut self,
        rows: Vec<CheckerStateRow>,
    ) -> anyhow::Result<()> {
        self.store.clear();
        for row in rows {
            let entry =
                serde_json::from_str::<PersistedCheckEntry>(&row.payload).with_context(|| {
                    format!(
                        "Checker [{}] failed to parse state row key [{}]",
                        self.name, row.row_key
                    )
                })?;
            self.store.insert(row.row_key, CheckEntry::from(entry));
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
