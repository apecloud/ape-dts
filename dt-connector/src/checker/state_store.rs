use std::str::FromStr;

use anyhow::{Context, Result};
use chrono::Utc;
use mongodb::{
    bson::{doc, Bson, Document},
    options::{IndexOptions, ReplaceOptions},
    Client, IndexModel,
};
use sqlx::{query, ColumnIndex, Database, Decode, MySql, PgPool, Pool, Postgres, Row, Type};

use dt_common::meta::position::Position;

const DEFAULT_STATE_SCHEMA: &str = "apecloud_metadata";
const DEFAULT_STATE_TABLE_PREFIX: &str = "apedts_checker";
const POSTGRES_LEGACY_MANIFEST_COLUMNS: [&str; 3] = ["epoch", "check_enabled", "phase"];

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CheckpointManifest {
    pub task_id: String,
    pub checkpoint_id: String,
    pub position: String,
    pub committed_at: String,
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
    pub evicted_miss: usize,
    pub evicted_diff: usize,
}

#[derive(Clone, Debug)]
enum CheckerStateStoreBackend {
    MySql(Pool<MySql>),
    Postgres(Pool<Postgres>),
    Mongo(Client),
}

#[derive(Clone, Debug)]
pub struct CheckerStateStore {
    backend: CheckerStateStoreBackend,
    schema: String,
    snapshot_table: String,
    manifest_table: String,
}

impl CheckerStateStore {
    pub async fn new_mysql(
        pool: Pool<MySql>,
        schema: &str,
        table_prefix: &str,
    ) -> anyhow::Result<Self> {
        Self::new_with_backend(CheckerStateStoreBackend::MySql(pool), schema, table_prefix).await
    }

    pub async fn new_postgres(
        pool: Pool<Postgres>,
        schema: &str,
        table_prefix: &str,
    ) -> anyhow::Result<Self> {
        Self::new_with_backend(
            CheckerStateStoreBackend::Postgres(pool),
            schema,
            table_prefix,
        )
        .await
    }

    pub async fn new_mongo(
        client: Client,
        schema: &str,
        table_prefix: &str,
    ) -> anyhow::Result<Self> {
        Self::new_with_backend(
            CheckerStateStoreBackend::Mongo(client),
            schema,
            table_prefix,
        )
        .await
    }

    async fn new_with_backend(
        backend: CheckerStateStoreBackend,
        schema: &str,
        table_prefix: &str,
    ) -> anyhow::Result<Self> {
        let schema = sanitize_identifier(schema, DEFAULT_STATE_SCHEMA);
        let prefix = sanitize_identifier(table_prefix, DEFAULT_STATE_TABLE_PREFIX);

        let store = Self {
            backend,
            schema,
            snapshot_table: format!("{prefix}_store_snapshot"),
            manifest_table: format!("{prefix}_checkpoint_manifest"),
        };
        store.initialization().await?;
        Ok(store)
    }

    async fn initialization(&self) -> Result<()> {
        match &self.backend {
            CheckerStateStoreBackend::MySql(pool) => {
                let create_db_sql = format!("CREATE DATABASE IF NOT EXISTS `{}`", self.schema);
                query(&create_db_sql).execute(pool).await.context(format!(
                    "failed to create checker state schema: {create_db_sql}"
                ))?;

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
            }
            CheckerStateStoreBackend::Postgres(pool) => {
                let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS {}", self.schema);
                query(&create_schema_sql)
                    .execute(pool)
                    .await
                    .context(format!(
                        "failed to create checker state schema: {create_schema_sql}"
                    ))?;

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

                self.reset_postgres_legacy_manifest_if_needed(pool).await?;

                let manifest_sql = format!(
                    r#"CREATE TABLE IF NOT EXISTS {}.{} (
                      task_id varchar(255) NOT NULL,
                      checkpoint_id varchar(128) NOT NULL,
                      position_data text NOT NULL,
                      committed_at varchar(64) NOT NULL,
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
            }
            CheckerStateStoreBackend::Mongo(client) => {
                let database = client.database(&self.schema);
                let snapshot_collection = database.collection::<Document>(&self.snapshot_table);
                snapshot_collection
                    .create_index(
                        IndexModel::builder()
                            .keys(doc! {
                                "task_id": 1,
                                "checkpoint_id": 1,
                                "row_key": 1,
                            })
                            .options(IndexOptions::builder().unique(true).build())
                            .build(),
                        None,
                    )
                    .await
                    .context("failed to create checker snapshot index")?;

                let manifest_collection = database.collection::<Document>(&self.manifest_table);
                manifest_collection
                    .create_index(
                        IndexModel::builder()
                            .keys(doc! { "task_id": 1 })
                            .options(IndexOptions::builder().unique(true).build())
                            .build(),
                        None,
                    )
                    .await
                    .context("failed to create checker manifest index")?;
            }
        }
        Ok(())
    }
}

impl CheckerStateStore {
    async fn reset_postgres_legacy_manifest_if_needed(&self, pool: &PgPool) -> Result<()> {
        let columns_sql = "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2";
        let column_rows = query(columns_sql)
            .bind(&self.schema)
            .bind(&self.manifest_table)
            .fetch_all(pool)
            .await
            .context("failed to inspect checker manifest table schema")?;

        let has_legacy_columns = column_rows.into_iter().any(|row| {
            let column_name = row.get::<String, _>("column_name");
            POSTGRES_LEGACY_MANIFEST_COLUMNS.contains(&column_name.as_str())
        });

        if !has_legacy_columns {
            return Ok(());
        }

        let drop_manifest_sql = format!(
            "DROP TABLE IF EXISTS {}.{}",
            self.schema, self.manifest_table
        );
        query(&drop_manifest_sql)
            .execute(pool)
            .await
            .context("failed to rebuild legacy checker manifest table")?;
        Ok(())
    }
}

impl CheckerStateStore {
    pub async fn commit_checkpoint(
        &self,
        commit: &CheckerCheckpointCommit,
    ) -> Result<CheckpointManifest> {
        let task_id = &commit.task_id;
        let checkpoint_id = &commit.checkpoint_id;
        let position = &commit.position;
        let rows = &commit.rows;
        let evicted_miss = commit.evicted_miss;
        let evicted_diff = commit.evicted_diff;
        let position_str = position.to_string();
        let now = Utc::now().to_rfc3339();

        match &self.backend {
            CheckerStateStoreBackend::MySql(pool) => {
                let mut tx = pool.begin().await?;

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

                let manifest_sql = format!(
                    "INSERT INTO `{}`.`{}` (task_id, checkpoint_id, position_data, committed_at, evicted_miss, evicted_diff)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                    checkpoint_id = VALUES(checkpoint_id),
                    position_data = VALUES(position_data),
                    committed_at = VALUES(committed_at),
                    evicted_miss = VALUES(evicted_miss),
                    evicted_diff = VALUES(evicted_diff)",
                    self.schema, self.manifest_table
                );
                query(&manifest_sql)
                    .bind(task_id)
                    .bind(checkpoint_id)
                    .bind(&position_str)
                    .bind(&now)
                    .bind(evicted_miss as u64)
                    .bind(evicted_diff as u64)
                    .execute(&mut *tx)
                    .await
                    .context("failed to persist checker manifest")?;

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
            CheckerStateStoreBackend::Postgres(pool) => {
                let mut tx = pool.begin().await?;

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

                let manifest_sql = format!(
                    "INSERT INTO {}.{} (task_id, checkpoint_id, position_data, committed_at, evicted_miss, evicted_diff)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (task_id)
                    DO UPDATE SET
                    checkpoint_id = EXCLUDED.checkpoint_id,
                    position_data = EXCLUDED.position_data,
                    committed_at = EXCLUDED.committed_at,
                    evicted_miss = EXCLUDED.evicted_miss,
                    evicted_diff = EXCLUDED.evicted_diff",
                    self.schema, self.manifest_table
                );
                query(&manifest_sql)
                    .bind(task_id)
                    .bind(checkpoint_id)
                    .bind(&position_str)
                    .bind(&now)
                    .bind(evicted_miss as i64)
                    .bind(evicted_diff as i64)
                    .execute(&mut *tx)
                    .await
                    .context("failed to persist checker manifest")?;

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
            CheckerStateStoreBackend::Mongo(client) => {
                let snapshot_collection = client
                    .database(&self.schema)
                    .collection::<Document>(&self.snapshot_table);
                snapshot_collection
                    .delete_many(
                        doc! {
                            "task_id": task_id,
                            "checkpoint_id": checkpoint_id,
                        },
                        None,
                    )
                    .await
                    .context("failed to clean current checker snapshot")?;

                if !rows.is_empty() {
                    let snapshot_docs: Vec<_> = rows
                        .iter()
                        .map(|row| {
                            doc! {
                                "task_id": task_id,
                                "checkpoint_id": checkpoint_id,
                                "row_key": row.row_key.to_string(),
                                "row_payload": row.payload.as_str(),
                                "updated_at": now.as_str(),
                            }
                        })
                        .collect();
                    snapshot_collection
                        .insert_many(snapshot_docs, None)
                        .await
                        .context("failed to persist checker snapshot rows")?;
                }

                let manifest_collection = client
                    .database(&self.schema)
                    .collection::<Document>(&self.manifest_table);
                manifest_collection
                    .replace_one(
                        doc! { "task_id": task_id },
                        doc! {
                            "task_id": task_id,
                            "checkpoint_id": checkpoint_id,
                            "position_data": position_str.as_str(),
                            "committed_at": now.as_str(),
                            "evicted_miss": evicted_miss as i64,
                            "evicted_diff": evicted_diff as i64,
                        },
                        Some(ReplaceOptions::builder().upsert(true).build()),
                    )
                    .await
                    .context("failed to persist checker manifest")?;

                snapshot_collection
                    .delete_many(
                        doc! {
                            "task_id": task_id,
                            "checkpoint_id": { "$ne": checkpoint_id },
                        },
                        None,
                    )
                    .await
                    .context("failed to purge old checker snapshots")?;
            }
        }

        Ok(build_manifest(
            task_id,
            ManifestParts {
                checkpoint_id: checkpoint_id.to_string(),
                position: position_str,
                committed_at: now,
                evicted_miss,
                evicted_diff,
            },
        ))
    }

    pub async fn load_latest_checkpoint(
        &self,
        task_id: &str,
    ) -> Result<Option<CheckerCheckpointBundle>> {
        match &self.backend {
            CheckerStateStoreBackend::MySql(pool) => {
                let manifest_sql = format!(
                    "SELECT checkpoint_id, position_data, committed_at, evicted_miss, evicted_diff
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
                let manifest = build_manifest(
                    task_id,
                    ManifestParts {
                        checkpoint_id: checkpoint_id.clone(),
                        position: manifest_row.get::<String, _>("position_data"),
                        committed_at: manifest_row.get::<String, _>("committed_at"),
                        evicted_miss: manifest_row.get::<u64, _>("evicted_miss") as usize,
                        evicted_diff: manifest_row.get::<u64, _>("evicted_diff") as usize,
                    },
                );

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
                let rows = parse_snapshot_rows(snapshot_rows)?;
                Ok(Some(CheckerCheckpointBundle { manifest, rows }))
            }
            CheckerStateStoreBackend::Postgres(pool) => {
                let manifest_sql = format!(
                    "SELECT checkpoint_id, position_data, committed_at, evicted_miss, evicted_diff
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
                let manifest = build_manifest(
                    task_id,
                    ManifestParts {
                        checkpoint_id: checkpoint_id.clone(),
                        position: manifest_row.get::<String, _>("position_data"),
                        committed_at: manifest_row.get::<String, _>("committed_at"),
                        evicted_miss: manifest_row.get::<i64, _>("evicted_miss").max(0) as usize,
                        evicted_diff: manifest_row.get::<i64, _>("evicted_diff").max(0) as usize,
                    },
                );

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
                let rows = parse_snapshot_rows(snapshot_rows)?;
                Ok(Some(CheckerCheckpointBundle { manifest, rows }))
            }
            CheckerStateStoreBackend::Mongo(client) => {
                let manifest_collection = client
                    .database(&self.schema)
                    .collection::<Document>(&self.manifest_table);
                let manifest_doc = manifest_collection
                    .find_one(doc! { "task_id": task_id }, None)
                    .await?
                    .map(|doc| parse_mongo_manifest(task_id, doc))
                    .transpose()?;
                let Some(manifest) = manifest_doc else {
                    return Ok(None);
                };

                let snapshot_collection = client
                    .database(&self.schema)
                    .collection::<Document>(&self.snapshot_table);
                let mut cursor = snapshot_collection
                    .find(
                        doc! {
                            "task_id": task_id,
                            "checkpoint_id": manifest.checkpoint_id.as_str(),
                        },
                        None,
                    )
                    .await?;
                let mut rows = Vec::new();
                while cursor.advance().await? {
                    let row_doc = cursor.deserialize_current()?;
                    rows.push(parse_mongo_snapshot_row(row_doc)?);
                }
                Ok(Some(CheckerCheckpointBundle { manifest, rows }))
            }
        }
    }
}

struct ManifestParts {
    checkpoint_id: String,
    position: String,
    committed_at: String,
    evicted_miss: usize,
    evicted_diff: usize,
}

fn build_manifest(task_id: &str, parts: ManifestParts) -> CheckpointManifest {
    CheckpointManifest {
        task_id: task_id.to_string(),
        checkpoint_id: parts.checkpoint_id,
        position: parts.position,
        committed_at: parts.committed_at,
        evicted_miss: parts.evicted_miss,
        evicted_diff: parts.evicted_diff,
    }
}

fn parse_snapshot_rows<DB: Database, R: Row<Database = DB>>(
    snapshot_rows: Vec<R>,
) -> Result<Vec<CheckerStateRow>>
where
    for<'r> &'r str: ColumnIndex<R>,
    for<'r> String: Decode<'r, DB> + Type<DB>,
{
    let mut rows = Vec::with_capacity(snapshot_rows.len());
    for row in snapshot_rows {
        let row_key_raw = row.get::<String, _>("row_key");
        let row_key = u128::from_str(&row_key_raw)
            .with_context(|| format!("invalid checker row key [{row_key_raw}] in state store"))?;
        let payload = row.get::<String, _>("row_payload");
        rows.push(CheckerStateRow { row_key, payload });
    }
    Ok(rows)
}

fn parse_mongo_manifest(task_id: &str, doc: Document) -> Result<CheckpointManifest> {
    Ok(build_manifest(
        task_id,
        ManifestParts {
            checkpoint_id: mongo_string_field(&doc, "checkpoint_id")?,
            position: mongo_string_field(&doc, "position_data")?,
            committed_at: mongo_string_field(&doc, "committed_at")?,
            evicted_miss: mongo_usize_field(&doc, "evicted_miss")?,
            evicted_diff: mongo_usize_field(&doc, "evicted_diff")?,
        },
    ))
}

fn parse_mongo_snapshot_row(doc: Document) -> Result<CheckerStateRow> {
    let row_key_raw = mongo_string_field(&doc, "row_key")?;
    let row_key = u128::from_str(&row_key_raw)
        .with_context(|| format!("invalid checker row key [{row_key_raw}] in state store"))?;
    let payload = mongo_string_field(&doc, "row_payload")?;
    Ok(CheckerStateRow { row_key, payload })
}

fn mongo_string_field(doc: &Document, field: &str) -> Result<String> {
    match doc.get(field) {
        Some(Bson::String(value)) => Ok(value.clone()),
        Some(other) => {
            anyhow::bail!("checker state store field [{field}] should be string, got {other:?}")
        }
        None => anyhow::bail!("checker state store missing required field [{field}]"),
    }
}

fn mongo_usize_field(doc: &Document, field: &str) -> Result<usize> {
    match doc.get(field) {
        Some(Bson::Int32(value)) => Ok((*value).max(0) as usize),
        Some(Bson::Int64(value)) => Ok((*value).max(0) as usize),
        Some(other) => {
            anyhow::bail!("checker state store field [{field}] should be integer, got {other:?}")
        }
        None => anyhow::bail!("checker state store missing required field [{field}]"),
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
