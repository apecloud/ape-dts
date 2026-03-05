use std::str::FromStr;

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use sqlx::{query, Row};

use crate::extractor::resumer::ResumerDbPool;
use dt_common::meta::position::Position;

const DEFAULT_STATE_SCHEMA: &str = "apecloud_metadata";
const DEFAULT_STATE_TABLE_PREFIX: &str = "apedts_checker";

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum CheckerLifecyclePhase {
    Running,
    Rechecking,
}

impl CheckerLifecyclePhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "RUNNING",
            Self::Rechecking => "RECHECKING",
        }
    }

    pub fn from_str_value(value: &str) -> Self {
        match value {
            "RUNNING" => Self::Running,
            "RECHECKING" => Self::Rechecking,
            _ => Self::Running,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CheckerEpochState {
    pub task_id: String,
    pub epoch: u64,
    pub phase: CheckerLifecyclePhase,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CheckpointManifest {
    pub task_id: String,
    pub checkpoint_id: String,
    pub position: String,
    pub committed_at: String,
    pub epoch: u64,
    pub phase: CheckerLifecyclePhase,
    pub evicted_miss: usize,
    pub evicted_diff: usize,
}

#[derive(Clone, Debug)]
pub struct CheckerStateRow {
    pub row_key: u128,
    pub payload: String,
}

#[derive(Clone, Debug)]
pub struct CheckerCheckpointBundle {
    pub manifest: CheckpointManifest,
    pub rows: Vec<CheckerStateRow>,
}

#[derive(Clone, Debug)]
pub struct CheckerCheckpointCommit {
    pub task_id: String,
    pub checkpoint_id: String,
    pub position: Position,
    pub rows: Vec<CheckerStateRow>,
    pub epoch_state: CheckerEpochState,
    pub evicted_miss: usize,
    pub evicted_diff: usize,
}

#[async_trait]
pub trait CheckerStateStore: Send + Sync {
    async fn load_epoch_state(&self, task_id: &str) -> Result<Option<CheckerEpochState>>;
    async fn save_epoch_state(&self, state: &CheckerEpochState) -> Result<()>;
    async fn commit_checkpoint(
        &self,
        commit: &CheckerCheckpointCommit,
    ) -> Result<CheckpointManifest>;
    async fn load_latest_checkpoint(
        &self,
        task_id: &str,
    ) -> Result<Option<CheckerCheckpointBundle>>;
}

#[derive(Clone, Debug)]
pub struct SqlCheckerStateStore {
    pool: ResumerDbPool,
    schema: String,
    position_table: String,
    snapshot_table: String,
    manifest_table: String,
    epoch_table: String,
}

impl SqlCheckerStateStore {
    pub async fn new(
        pool: ResumerDbPool,
        schema: &str,
        table_prefix: &str,
    ) -> anyhow::Result<Self> {
        let schema = sanitize_identifier(schema, DEFAULT_STATE_SCHEMA);
        let prefix = sanitize_identifier(table_prefix, DEFAULT_STATE_TABLE_PREFIX);

        let store = Self {
            pool,
            schema,
            position_table: format!("{prefix}_checkpoint_position"),
            snapshot_table: format!("{prefix}_store_snapshot"),
            manifest_table: format!("{prefix}_checkpoint_manifest"),
            epoch_table: format!("{prefix}_epoch_state"),
        };
        store.initialization().await?;
        Ok(store)
    }

    async fn initialization(&self) -> Result<()> {
        match &self.pool {
            ResumerDbPool::MySql(pool) => {
                let create_db_sql = format!("CREATE DATABASE IF NOT EXISTS `{}`", self.schema);
                query(&create_db_sql).execute(pool).await.context(format!(
                    "failed to create checker state schema: {create_db_sql}"
                ))?;

                let position_sql = format!(
                    r#"CREATE TABLE IF NOT EXISTS `{}`.`{}` (
                      task_id varchar(255) NOT NULL,
                      checkpoint_id varchar(128) NOT NULL,
                      position_data text NOT NULL,
                      updated_at varchar(64) NOT NULL,
                      PRIMARY KEY (task_id, checkpoint_id)
                    )"#,
                    self.schema, self.position_table
                );
                query(&position_sql)
                    .execute(pool)
                    .await
                    .context("failed to create checker checkpoint position table")?;

                let snapshot_sql = format!(
                    r#"CREATE TABLE IF NOT EXISTS `{}`.`{}` (
                      task_id varchar(255) NOT NULL,
                      checkpoint_id varchar(128) NOT NULL,
                      row_key varchar(64) NOT NULL,
                      row_payload longtext NOT NULL,
                      updated_at varchar(64) NOT NULL,
                      PRIMARY KEY (task_id, checkpoint_id, row_key)
                    )"#,
                    self.schema, self.snapshot_table
                );
                query(&snapshot_sql)
                    .execute(pool)
                    .await
                    .context("failed to create checker snapshot table")?;

                let manifest_sql = format!(
                    r#"CREATE TABLE IF NOT EXISTS `{}`.`{}` (
                      task_id varchar(255) NOT NULL,
                      checkpoint_id varchar(128) NOT NULL,
                      position_data text NOT NULL,
                      committed_at varchar(64) NOT NULL,
                      epoch bigint UNSIGNED NOT NULL,
                      phase varchar(32) NOT NULL,
                      evicted_miss bigint UNSIGNED NOT NULL,
                      evicted_diff bigint UNSIGNED NOT NULL,
                      PRIMARY KEY (task_id)
                    )"#,
                    self.schema, self.manifest_table
                );
                query(&manifest_sql)
                    .execute(pool)
                    .await
                    .context("failed to create checker manifest table")?;

                let epoch_sql = format!(
                    r#"CREATE TABLE IF NOT EXISTS `{}`.`{}` (
                      task_id varchar(255) NOT NULL,
                      epoch bigint UNSIGNED NOT NULL,
                      phase varchar(32) NOT NULL,
                      updated_at varchar(64) NOT NULL,
                      PRIMARY KEY (task_id)
                    )"#,
                    self.schema, self.epoch_table
                );
                query(&epoch_sql)
                    .execute(pool)
                    .await
                    .context("failed to create checker epoch table")?;
            }
            ResumerDbPool::Postgres(pool) => {
                let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS {}", self.schema);
                query(&create_schema_sql)
                    .execute(pool)
                    .await
                    .context(format!(
                        "failed to create checker state schema: {create_schema_sql}"
                    ))?;

                let position_sql = format!(
                    r#"CREATE TABLE IF NOT EXISTS {}.{} (
                      task_id varchar(255) NOT NULL,
                      checkpoint_id varchar(128) NOT NULL,
                      position_data text NOT NULL,
                      updated_at varchar(64) NOT NULL,
                      PRIMARY KEY (task_id, checkpoint_id)
                    )"#,
                    self.schema, self.position_table
                );
                query(&position_sql)
                    .execute(pool)
                    .await
                    .context("failed to create checker checkpoint position table")?;

                let snapshot_sql = format!(
                    r#"CREATE TABLE IF NOT EXISTS {}.{} (
                      task_id varchar(255) NOT NULL,
                      checkpoint_id varchar(128) NOT NULL,
                      row_key varchar(64) NOT NULL,
                      row_payload text NOT NULL,
                      updated_at varchar(64) NOT NULL,
                      PRIMARY KEY (task_id, checkpoint_id, row_key)
                    )"#,
                    self.schema, self.snapshot_table
                );
                query(&snapshot_sql)
                    .execute(pool)
                    .await
                    .context("failed to create checker snapshot table")?;

                let manifest_sql = format!(
                    r#"CREATE TABLE IF NOT EXISTS {}.{} (
                      task_id varchar(255) NOT NULL,
                      checkpoint_id varchar(128) NOT NULL,
                      position_data text NOT NULL,
                      committed_at varchar(64) NOT NULL,
                      epoch bigint NOT NULL,
                      phase varchar(32) NOT NULL,
                      evicted_miss bigint NOT NULL,
                      evicted_diff bigint NOT NULL,
                      PRIMARY KEY (task_id)
                    )"#,
                    self.schema, self.manifest_table
                );
                query(&manifest_sql)
                    .execute(pool)
                    .await
                    .context("failed to create checker manifest table")?;

                let epoch_sql = format!(
                    r#"CREATE TABLE IF NOT EXISTS {}.{} (
                      task_id varchar(255) NOT NULL,
                      epoch bigint NOT NULL,
                      phase varchar(32) NOT NULL,
                      updated_at varchar(64) NOT NULL,
                      PRIMARY KEY (task_id)
                    )"#,
                    self.schema, self.epoch_table
                );
                query(&epoch_sql)
                    .execute(pool)
                    .await
                    .context("failed to create checker epoch table")?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl CheckerStateStore for SqlCheckerStateStore {
    async fn load_epoch_state(&self, task_id: &str) -> Result<Option<CheckerEpochState>> {
        match &self.pool {
            ResumerDbPool::MySql(pool) => {
                let sql = format!(
                    "SELECT epoch, phase FROM `{}`.`{}` WHERE task_id = ?",
                    self.schema, self.epoch_table
                );
                let row = query(&sql).bind(task_id).fetch_optional(pool).await?;
                let Some(row) = row else {
                    return Ok(None);
                };
                let epoch = row.get::<u64, _>("epoch");
                let phase = row.get::<String, _>("phase");
                Ok(Some(CheckerEpochState {
                    task_id: task_id.to_string(),
                    epoch,
                    phase: CheckerLifecyclePhase::from_str_value(&phase),
                }))
            }
            ResumerDbPool::Postgres(pool) => {
                let sql = format!(
                    "SELECT epoch, phase FROM {}.{} WHERE task_id = $1",
                    self.schema, self.epoch_table
                );
                let row = query(&sql).bind(task_id).fetch_optional(pool).await?;
                let Some(row) = row else {
                    return Ok(None);
                };
                let epoch = row.get::<i64, _>("epoch").max(0) as u64;
                let phase = row.get::<String, _>("phase");
                Ok(Some(CheckerEpochState {
                    task_id: task_id.to_string(),
                    epoch,
                    phase: CheckerLifecyclePhase::from_str_value(&phase),
                }))
            }
        }
    }

    async fn save_epoch_state(&self, state: &CheckerEpochState) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        match &self.pool {
            ResumerDbPool::MySql(pool) => {
                let sql = format!(
                    "INSERT INTO `{}`.`{}` (task_id, epoch, phase, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                    epoch = VALUES(epoch),
                    phase = VALUES(phase),
                    updated_at = VALUES(updated_at)",
                    self.schema, self.epoch_table
                );
                query(&sql)
                    .bind(&state.task_id)
                    .bind(state.epoch)
                    .bind(state.phase.as_str())
                    .bind(now)
                    .execute(pool)
                    .await
                    .context("failed to save checker epoch state")?;
            }
            ResumerDbPool::Postgres(pool) => {
                let sql = format!(
                    "INSERT INTO {}.{} (task_id, epoch, phase, updated_at)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (task_id)
                    DO UPDATE SET
                    epoch = EXCLUDED.epoch,
                    phase = EXCLUDED.phase,
                    updated_at = EXCLUDED.updated_at",
                    self.schema, self.epoch_table
                );
                query(&sql)
                    .bind(&state.task_id)
                    .bind(state.epoch as i64)
                    .bind(state.phase.as_str())
                    .bind(now)
                    .execute(pool)
                    .await
                    .context("failed to save checker epoch state")?;
            }
        }
        Ok(())
    }

    async fn commit_checkpoint(
        &self,
        commit: &CheckerCheckpointCommit,
    ) -> Result<CheckpointManifest> {
        let task_id = &commit.task_id;
        let checkpoint_id = &commit.checkpoint_id;
        let position = &commit.position;
        let rows = &commit.rows;
        let epoch_state = &commit.epoch_state;
        let evicted_miss = commit.evicted_miss;
        let evicted_diff = commit.evicted_diff;
        let position_str = position.to_string();
        let now = Utc::now().to_rfc3339();

        match &self.pool {
            ResumerDbPool::MySql(pool) => {
                let mut tx = pool.begin().await?;

                let position_sql = format!(
                    "INSERT INTO `{}`.`{}` (task_id, checkpoint_id, position_data, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                    position_data = VALUES(position_data),
                    updated_at = VALUES(updated_at)",
                    self.schema, self.position_table
                );
                query(&position_sql)
                    .bind(task_id)
                    .bind(checkpoint_id)
                    .bind(&position_str)
                    .bind(&now)
                    .execute(&mut *tx)
                    .await
                    .context("failed to persist checker position")?;

                let clean_current_snapshot_sql = format!(
                    "DELETE FROM `{}`.`{}` WHERE task_id = ? AND checkpoint_id = ?",
                    self.schema, self.snapshot_table
                );
                query(&clean_current_snapshot_sql)
                    .bind(task_id)
                    .bind(checkpoint_id)
                    .execute(&mut *tx)
                    .await
                    .context("failed to clean current checker snapshot")?;

                if !rows.is_empty() {
                    let snapshot_sql = format!(
                        "INSERT INTO `{}`.`{}` (task_id, checkpoint_id, row_key, row_payload, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON DUPLICATE KEY UPDATE
                        row_payload = VALUES(row_payload),
                        updated_at = VALUES(updated_at)",
                        self.schema, self.snapshot_table
                    );
                    for row in rows {
                        query(&snapshot_sql)
                            .bind(task_id)
                            .bind(checkpoint_id)
                            .bind(row.row_key.to_string())
                            .bind(&row.payload)
                            .bind(&now)
                            .execute(&mut *tx)
                            .await
                            .context("failed to persist checker snapshot row")?;
                    }
                }

                let epoch_sql = format!(
                    "INSERT INTO `{}`.`{}` (task_id, epoch, phase, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                    epoch = VALUES(epoch),
                    phase = VALUES(phase),
                    updated_at = VALUES(updated_at)",
                    self.schema, self.epoch_table
                );
                query(&epoch_sql)
                    .bind(task_id)
                    .bind(epoch_state.epoch)
                    .bind(epoch_state.phase.as_str())
                    .bind(&now)
                    .execute(&mut *tx)
                    .await
                    .context("failed to persist checker epoch in checkpoint")?;

                let manifest_sql = format!(
                    "INSERT INTO `{}`.`{}` (task_id, checkpoint_id, position_data, committed_at, epoch, phase, evicted_miss, evicted_diff)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                    checkpoint_id = VALUES(checkpoint_id),
                    position_data = VALUES(position_data),
                    committed_at = VALUES(committed_at),
                    epoch = VALUES(epoch),
                    phase = VALUES(phase),
                    evicted_miss = VALUES(evicted_miss),
                    evicted_diff = VALUES(evicted_diff)",
                    self.schema, self.manifest_table
                );
                query(&manifest_sql)
                    .bind(task_id)
                    .bind(checkpoint_id)
                    .bind(&position_str)
                    .bind(&now)
                    .bind(epoch_state.epoch)
                    .bind(epoch_state.phase.as_str())
                    .bind(evicted_miss as u64)
                    .bind(evicted_diff as u64)
                    .execute(&mut *tx)
                    .await
                    .context("failed to persist checker manifest")?;

                let purge_old_position_sql = format!(
                    "DELETE FROM `{}`.`{}` WHERE task_id = ? AND checkpoint_id <> ?",
                    self.schema, self.position_table
                );
                query(&purge_old_position_sql)
                    .bind(task_id)
                    .bind(checkpoint_id)
                    .execute(&mut *tx)
                    .await
                    .context("failed to purge old checker positions")?;

                let purge_old_snapshot_sql = format!(
                    "DELETE FROM `{}`.`{}` WHERE task_id = ? AND checkpoint_id <> ?",
                    self.schema, self.snapshot_table
                );
                query(&purge_old_snapshot_sql)
                    .bind(task_id)
                    .bind(checkpoint_id)
                    .execute(&mut *tx)
                    .await
                    .context("failed to purge old checker snapshots")?;

                tx.commit().await?;
            }
            ResumerDbPool::Postgres(pool) => {
                let mut tx = pool.begin().await?;

                let position_sql = format!(
                    "INSERT INTO {}.{} (task_id, checkpoint_id, position_data, updated_at)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (task_id, checkpoint_id)
                    DO UPDATE SET
                    position_data = EXCLUDED.position_data,
                    updated_at = EXCLUDED.updated_at",
                    self.schema, self.position_table
                );
                query(&position_sql)
                    .bind(task_id)
                    .bind(checkpoint_id)
                    .bind(&position_str)
                    .bind(&now)
                    .execute(&mut *tx)
                    .await
                    .context("failed to persist checker position")?;

                let clean_current_snapshot_sql = format!(
                    "DELETE FROM {}.{} WHERE task_id = $1 AND checkpoint_id = $2",
                    self.schema, self.snapshot_table
                );
                query(&clean_current_snapshot_sql)
                    .bind(task_id)
                    .bind(checkpoint_id)
                    .execute(&mut *tx)
                    .await
                    .context("failed to clean current checker snapshot")?;

                if !rows.is_empty() {
                    let snapshot_sql = format!(
                        "INSERT INTO {}.{} (task_id, checkpoint_id, row_key, row_payload, updated_at)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (task_id, checkpoint_id, row_key)
                        DO UPDATE SET
                        row_payload = EXCLUDED.row_payload,
                        updated_at = EXCLUDED.updated_at",
                        self.schema, self.snapshot_table
                    );
                    for row in rows {
                        query(&snapshot_sql)
                            .bind(task_id)
                            .bind(checkpoint_id)
                            .bind(row.row_key.to_string())
                            .bind(&row.payload)
                            .bind(&now)
                            .execute(&mut *tx)
                            .await
                            .context("failed to persist checker snapshot row")?;
                    }
                }

                let epoch_sql = format!(
                    "INSERT INTO {}.{} (task_id, epoch, phase, updated_at)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (task_id)
                    DO UPDATE SET
                    epoch = EXCLUDED.epoch,
                    phase = EXCLUDED.phase,
                    updated_at = EXCLUDED.updated_at",
                    self.schema, self.epoch_table
                );
                query(&epoch_sql)
                    .bind(task_id)
                    .bind(epoch_state.epoch as i64)
                    .bind(epoch_state.phase.as_str())
                    .bind(&now)
                    .execute(&mut *tx)
                    .await
                    .context("failed to persist checker epoch in checkpoint")?;

                let manifest_sql = format!(
                    "INSERT INTO {}.{} (task_id, checkpoint_id, position_data, committed_at, epoch, phase, evicted_miss, evicted_diff)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (task_id)
                    DO UPDATE SET
                    checkpoint_id = EXCLUDED.checkpoint_id,
                    position_data = EXCLUDED.position_data,
                    committed_at = EXCLUDED.committed_at,
                    epoch = EXCLUDED.epoch,
                    phase = EXCLUDED.phase,
                    evicted_miss = EXCLUDED.evicted_miss,
                    evicted_diff = EXCLUDED.evicted_diff",
                    self.schema, self.manifest_table
                );
                query(&manifest_sql)
                    .bind(task_id)
                    .bind(checkpoint_id)
                    .bind(&position_str)
                    .bind(&now)
                    .bind(epoch_state.epoch as i64)
                    .bind(epoch_state.phase.as_str())
                    .bind(evicted_miss as i64)
                    .bind(evicted_diff as i64)
                    .execute(&mut *tx)
                    .await
                    .context("failed to persist checker manifest")?;

                let purge_old_position_sql = format!(
                    "DELETE FROM {}.{} WHERE task_id = $1 AND checkpoint_id <> $2",
                    self.schema, self.position_table
                );
                query(&purge_old_position_sql)
                    .bind(task_id)
                    .bind(checkpoint_id)
                    .execute(&mut *tx)
                    .await
                    .context("failed to purge old checker positions")?;

                let purge_old_snapshot_sql = format!(
                    "DELETE FROM {}.{} WHERE task_id = $1 AND checkpoint_id <> $2",
                    self.schema, self.snapshot_table
                );
                query(&purge_old_snapshot_sql)
                    .bind(task_id)
                    .bind(checkpoint_id)
                    .execute(&mut *tx)
                    .await
                    .context("failed to purge old checker snapshots")?;

                tx.commit().await?;
            }
        }

        Ok(CheckpointManifest {
            task_id: task_id.to_string(),
            checkpoint_id: checkpoint_id.to_string(),
            position: position_str,
            committed_at: now,
            epoch: epoch_state.epoch,
            phase: epoch_state.phase.clone(),
            evicted_miss,
            evicted_diff,
        })
    }

    async fn load_latest_checkpoint(
        &self,
        task_id: &str,
    ) -> Result<Option<CheckerCheckpointBundle>> {
        match &self.pool {
            ResumerDbPool::MySql(pool) => {
                let manifest_sql = format!(
                    "SELECT checkpoint_id, position_data, committed_at, epoch, phase, evicted_miss, evicted_diff
                    FROM `{}`.`{}`
                    WHERE task_id = ?",
                    self.schema, self.manifest_table
                );
                let manifest_row = query(&manifest_sql)
                    .bind(task_id)
                    .fetch_optional(pool)
                    .await?;
                let Some(manifest_row) = manifest_row else {
                    return Ok(None);
                };
                let checkpoint_id = manifest_row.get::<String, _>("checkpoint_id");
                let manifest = CheckpointManifest {
                    task_id: task_id.to_string(),
                    checkpoint_id: checkpoint_id.clone(),
                    position: manifest_row.get::<String, _>("position_data"),
                    committed_at: manifest_row.get::<String, _>("committed_at"),
                    epoch: manifest_row.get::<u64, _>("epoch"),
                    phase: CheckerLifecyclePhase::from_str_value(
                        &manifest_row.get::<String, _>("phase"),
                    ),
                    evicted_miss: manifest_row.get::<u64, _>("evicted_miss") as usize,
                    evicted_diff: manifest_row.get::<u64, _>("evicted_diff") as usize,
                };

                let snapshot_sql = format!(
                    "SELECT row_key, row_payload FROM `{}`.`{}`
                    WHERE task_id = ? AND checkpoint_id = ?",
                    self.schema, self.snapshot_table
                );
                let snapshot_rows = query(&snapshot_sql)
                    .bind(task_id)
                    .bind(&checkpoint_id)
                    .fetch_all(pool)
                    .await?;
                let mut rows = Vec::with_capacity(snapshot_rows.len());
                for row in snapshot_rows {
                    let row_key_raw = row.get::<String, _>("row_key");
                    let row_key = u128::from_str(&row_key_raw).with_context(|| {
                        format!("invalid checker row key [{row_key_raw}] in state store")
                    })?;
                    let payload = row.get::<String, _>("row_payload");
                    rows.push(CheckerStateRow { row_key, payload });
                }

                Ok(Some(CheckerCheckpointBundle { manifest, rows }))
            }
            ResumerDbPool::Postgres(pool) => {
                let manifest_sql = format!(
                    "SELECT checkpoint_id, position_data, committed_at, epoch, phase, evicted_miss, evicted_diff
                    FROM {}.{}
                    WHERE task_id = $1",
                    self.schema, self.manifest_table
                );
                let manifest_row = query(&manifest_sql)
                    .bind(task_id)
                    .fetch_optional(pool)
                    .await?;
                let Some(manifest_row) = manifest_row else {
                    return Ok(None);
                };
                let checkpoint_id = manifest_row.get::<String, _>("checkpoint_id");
                let manifest = CheckpointManifest {
                    task_id: task_id.to_string(),
                    checkpoint_id: checkpoint_id.clone(),
                    position: manifest_row.get::<String, _>("position_data"),
                    committed_at: manifest_row.get::<String, _>("committed_at"),
                    epoch: manifest_row.get::<i64, _>("epoch").max(0) as u64,
                    phase: CheckerLifecyclePhase::from_str_value(
                        &manifest_row.get::<String, _>("phase"),
                    ),
                    evicted_miss: manifest_row.get::<i64, _>("evicted_miss").max(0) as usize,
                    evicted_diff: manifest_row.get::<i64, _>("evicted_diff").max(0) as usize,
                };

                let snapshot_sql = format!(
                    "SELECT row_key, row_payload FROM {}.{}
                    WHERE task_id = $1 AND checkpoint_id = $2",
                    self.schema, self.snapshot_table
                );
                let snapshot_rows = query(&snapshot_sql)
                    .bind(task_id)
                    .bind(&checkpoint_id)
                    .fetch_all(pool)
                    .await?;
                let mut rows = Vec::with_capacity(snapshot_rows.len());
                for row in snapshot_rows {
                    let row_key_raw = row.get::<String, _>("row_key");
                    let row_key = u128::from_str(&row_key_raw).with_context(|| {
                        format!("invalid checker row key [{row_key_raw}] in state store")
                    })?;
                    let payload = row.get::<String, _>("row_payload");
                    rows.push(CheckerStateRow { row_key, payload });
                }

                Ok(Some(CheckerCheckpointBundle { manifest, rows }))
            }
        }
    }
}

fn sanitize_identifier(raw: &str, default_value: &str) -> String {
    let normalized = raw.trim();
    if normalized.is_empty()
        || !normalized
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return default_value.to_string();
    }
    normalized.to_lowercase()
}
