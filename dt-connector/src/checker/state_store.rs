use std::str::FromStr;

use anyhow::{Context, Result};
use chrono::Utc;
use sqlx::{query, ColumnIndex, Database, Decode, MySql, Pool, Postgres, QueryBuilder, Row, Type};

use dt_common::{config::resumer_config::ResumerConfig, meta::position::Position};

use crate::extractor::resumer::{utils::ResumerUtil, ResumerDbPool, ResumerType};

const DEFAULT_ROWS_TABLE: &str = "apedts_unconsistent_rows";
const SNAPSHOT_INSERT_BIND_COUNT: usize = 6;
const MYSQL_SNAPSHOT_BATCH_ROWS: usize = 1000;
const POSTGRES_MAX_BIND_PARAMS: usize = 65_535;
const POSTGRES_SNAPSHOT_BATCH_ROWS: usize = POSTGRES_MAX_BIND_PARAMS / SNAPSHOT_INSERT_BIND_COUNT;
const SNAPSHOT_DELETE_BATCH_ROWS: usize = 1000;

#[derive(Clone, Debug)]
pub struct CheckerStateRow {
    pub row_key: u128,
    pub identity_key: String,
    pub identity_json: String,
    pub payload: String,
}

#[derive(Clone, Debug)]
pub struct CheckerCheckpointCommit {
    pub task_id: String,
    pub position: Position,
    pub upserts: Vec<CheckerStateRow>,
    pub deletes: Vec<String>,
}

#[derive(Clone, Debug)]
enum CheckerStateStoreBackend {
    MySql(Pool<MySql>),
    Postgres(Pool<Postgres>),
}

#[derive(Clone, Debug)]
pub struct CheckerStateStore {
    backend: CheckerStateStoreBackend,
    schema: String,
    position_table: String,
    rows_table: String,
}

impl CheckerStateStore {
    pub async fn new(pool: ResumerDbPool, resumer_config: &ResumerConfig) -> anyhow::Result<Self> {
        let (schema, position_table) = match resumer_config {
            ResumerConfig::FromDB {
                table_full_name, ..
            } => ResumerUtil::get_full_table_name(table_full_name)?,
            _ => anyhow::bail!("checker state store only supports ResumerConfig::FromDB"),
        };

        let backend = match pool {
            ResumerDbPool::MySql(pool) => CheckerStateStoreBackend::MySql(pool),
            ResumerDbPool::Postgres(pool) => CheckerStateStoreBackend::Postgres(pool),
        };

        let store = Self {
            backend,
            schema,
            position_table,
            rows_table: DEFAULT_ROWS_TABLE.to_string(),
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

                let rows_sql = format!(
                    r#"CREATE TABLE IF NOT EXISTS `{}`.`{}` (
                          task_id varchar(255) NOT NULL,
                          identity_key char(64) NOT NULL,
                          row_key varchar(64) NOT NULL,
                          identity_json longtext NOT NULL,
                          row_payload longtext NOT NULL,
                          updated_at varchar(64) NOT NULL,
                          PRIMARY KEY (task_id, identity_key)
                        )"#,
                    self.schema, self.rows_table
                );
                query(&rows_sql)
                    .execute(pool)
                    .await
                    .context("failed to create checker unresolved rows table")?;
            }
            CheckerStateStoreBackend::Postgres(pool) => {
                let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", self.schema);
                query(&create_schema_sql)
                    .execute(pool)
                    .await
                    .context(format!(
                        "failed to create checker state schema: {create_schema_sql}"
                    ))?;

                let rows_sql = format!(
                    r#"CREATE TABLE IF NOT EXISTS "{}"."{}" (
                          task_id varchar(255) NOT NULL,
                          identity_key char(64) NOT NULL,
                          row_key varchar(64) NOT NULL,
                          identity_json text NOT NULL,
                          row_payload text NOT NULL,
                          updated_at varchar(64) NOT NULL,
                          PRIMARY KEY (task_id, identity_key)
                        )"#,
                    self.schema, self.rows_table
                );
                query(&rows_sql)
                    .execute(pool)
                    .await
                    .context("failed to create checker unresolved rows table")?;
            }
        }
        Ok(())
    }

    async fn insert_mysql_rows(
        &self,
        tx: &mut sqlx::Transaction<'_, MySql>,
        task_id: &str,
        rows: &[CheckerStateRow],
        now: &str,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let prefix = format!(
            "INSERT INTO `{}`.`{}` \
            (task_id, identity_key, row_key, identity_json, row_payload, updated_at) ",
            self.schema, self.rows_table
        );
        for chunk in rows.chunks(MYSQL_SNAPSHOT_BATCH_ROWS) {
            let mut builder = QueryBuilder::<MySql>::new(prefix.clone());
            builder.push_values(chunk, |mut b, row| {
                b.push_bind(task_id)
                    .push_bind(&row.identity_key)
                    .push_bind(row.row_key.to_string())
                    .push_bind(&row.identity_json)
                    .push_bind(&row.payload)
                    .push_bind(now);
            });
            builder.push(
                " ON DUPLICATE KEY UPDATE \
                 row_key = VALUES(row_key), \
                 identity_json = VALUES(identity_json), \
                 row_payload = VALUES(row_payload), \
                 updated_at = VALUES(updated_at)",
            );
            builder
                .build()
                .execute(&mut **tx)
                .await
                .context("failed to persist checker unresolved rows")?;
        }

        Ok(())
    }

    async fn insert_postgres_rows(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        task_id: &str,
        rows: &[CheckerStateRow],
        now: &str,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let prefix = format!(
            "INSERT INTO \"{}\".\"{}\" \
            (task_id, identity_key, row_key, identity_json, row_payload, updated_at) ",
            self.schema, self.rows_table
        );
        for chunk in rows.chunks(POSTGRES_SNAPSHOT_BATCH_ROWS.max(1)) {
            let mut builder = QueryBuilder::<Postgres>::new(prefix.clone());
            builder.push_values(chunk, |mut b, row| {
                b.push_bind(task_id)
                    .push_bind(&row.identity_key)
                    .push_bind(row.row_key.to_string())
                    .push_bind(&row.identity_json)
                    .push_bind(&row.payload)
                    .push_bind(now);
            });
            builder.push(
                " ON CONFLICT (task_id, identity_key) DO UPDATE SET \
                 row_key = EXCLUDED.row_key, \
                 identity_json = EXCLUDED.identity_json, \
                 row_payload = EXCLUDED.row_payload, \
                 updated_at = EXCLUDED.updated_at",
            );
            builder
                .build()
                .execute(&mut **tx)
                .await
                .context("failed to persist checker unresolved rows")?;
        }

        Ok(())
    }

    async fn delete_mysql_rows(
        &self,
        tx: &mut sqlx::Transaction<'_, MySql>,
        task_id: &str,
        identity_keys: &[String],
    ) -> Result<()> {
        for chunk in identity_keys.chunks(SNAPSHOT_DELETE_BATCH_ROWS) {
            let mut builder = QueryBuilder::<MySql>::new(format!(
                "DELETE FROM `{}`.`{}` WHERE task_id = ",
                self.schema, self.rows_table
            ));
            builder.push_bind(task_id);
            builder.push(" AND identity_key IN (");
            {
                let mut separated = builder.separated(", ");
                for identity_key in chunk {
                    separated.push_bind(identity_key);
                }
            }
            builder.push(")");
            builder
                .build()
                .execute(&mut **tx)
                .await
                .context("failed to delete checker unresolved rows")?;
        }
        Ok(())
    }

    async fn delete_postgres_rows(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        task_id: &str,
        identity_keys: &[String],
    ) -> Result<()> {
        for chunk in identity_keys.chunks(SNAPSHOT_DELETE_BATCH_ROWS) {
            let mut builder = QueryBuilder::<Postgres>::new(format!(
                "DELETE FROM \"{}\".\"{}\" WHERE task_id = ",
                self.schema, self.rows_table
            ));
            builder.push_bind(task_id);
            builder.push(" AND identity_key IN (");
            {
                let mut separated = builder.separated(", ");
                for identity_key in chunk {
                    separated.push_bind(identity_key);
                }
            }
            builder.push(")");
            builder
                .build()
                .execute(&mut **tx)
                .await
                .context("failed to delete checker unresolved rows")?;
        }
        Ok(())
    }

    async fn upsert_mysql_cdc_position(
        &self,
        tx: &mut sqlx::Transaction<'_, MySql>,
        task_id: &str,
        position: &Position,
    ) -> Result<()> {
        let sql = format!(
            "INSERT INTO `{}`.`{}` (task_id, resumer_type, position_key, position_data) \
             VALUES (?, ?, ?, ?) \
             ON DUPLICATE KEY UPDATE \
             position_data = VALUES(position_data), \
             updated_at = CURRENT_TIMESTAMP",
            self.schema, self.position_table
        );
        query(&sql)
            .bind(task_id)
            .bind(ResumerType::CdcDoing.to_string())
            .bind(ResumerUtil::get_key_from_position(position))
            .bind(position.to_string())
            .execute(&mut **tx)
            .await
            .context("failed to persist shared cdc checkpoint")?;
        Ok(())
    }

    async fn upsert_postgres_cdc_position(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        task_id: &str,
        position: &Position,
    ) -> Result<()> {
        let sql = format!(
            "INSERT INTO \"{}\".\"{}\" (task_id, resumer_type, position_key, position_data) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (task_id, resumer_type, position_key) \
             DO UPDATE SET \
             position_data = EXCLUDED.position_data, \
             updated_at = CURRENT_TIMESTAMP",
            self.schema, self.position_table
        );
        query(&sql)
            .bind(task_id)
            .bind(ResumerType::CdcDoing.to_string())
            .bind(ResumerUtil::get_key_from_position(position))
            .bind(position.to_string())
            .execute(&mut **tx)
            .await
            .context("failed to persist shared cdc checkpoint")?;
        Ok(())
    }

    pub async fn commit_position(&self, task_id: &str, position: &Position) -> Result<()> {
        match &self.backend {
            CheckerStateStoreBackend::MySql(pool) => {
                let mut tx = pool.begin().await?;
                self.upsert_mysql_cdc_position(&mut tx, task_id, position)
                    .await?;
                tx.commit().await?;
            }
            CheckerStateStoreBackend::Postgres(pool) => {
                let mut tx = pool.begin().await?;
                self.upsert_postgres_cdc_position(&mut tx, task_id, position)
                    .await?;
                tx.commit().await?;
            }
        }

        Ok(())
    }

    pub async fn commit_checkpoint(&self, commit: &CheckerCheckpointCommit) -> Result<()> {
        let now = Utc::now().to_rfc3339();

        match &self.backend {
            CheckerStateStoreBackend::MySql(pool) => {
                let mut tx = pool.begin().await?;
                self.delete_mysql_rows(&mut tx, &commit.task_id, &commit.deletes)
                    .await?;
                self.insert_mysql_rows(&mut tx, &commit.task_id, &commit.upserts, &now)
                    .await?;
                self.upsert_mysql_cdc_position(&mut tx, &commit.task_id, &commit.position)
                    .await?;
                tx.commit().await?;
            }
            CheckerStateStoreBackend::Postgres(pool) => {
                let mut tx = pool.begin().await?;
                self.delete_postgres_rows(&mut tx, &commit.task_id, &commit.deletes)
                    .await?;
                self.insert_postgres_rows(&mut tx, &commit.task_id, &commit.upserts, &now)
                    .await?;
                self.upsert_postgres_cdc_position(&mut tx, &commit.task_id, &commit.position)
                    .await?;
                tx.commit().await?;
            }
        }

        Ok(())
    }

    pub async fn load_rows(&self, task_id: &str) -> Result<Vec<CheckerStateRow>> {
        match &self.backend {
            CheckerStateStoreBackend::MySql(pool) => {
                let sql = format!(
                    "SELECT row_key, identity_key, identity_json, row_payload \
                     FROM `{}`.`{}` WHERE task_id = ? ORDER BY identity_key",
                    self.schema, self.rows_table
                );
                let rows = query(&sql).bind(task_id).fetch_all(pool).await?;
                parse_snapshot_rows(rows)
            }
            CheckerStateStoreBackend::Postgres(pool) => {
                let sql = format!(
                    "SELECT row_key, identity_key, identity_json, row_payload \
                     FROM \"{}\".\"{}\" WHERE task_id = $1 ORDER BY identity_key",
                    self.schema, self.rows_table
                );
                let rows = query(&sql).bind(task_id).fetch_all(pool).await?;
                parse_snapshot_rows(rows)
            }
        }
    }
}

fn parse_snapshot_rows<DB: Database, R: Row<Database = DB>>(
    rows: Vec<R>,
) -> Result<Vec<CheckerStateRow>>
where
    for<'r> &'r str: ColumnIndex<R>,
    for<'r> String: Decode<'r, DB> + Type<DB>,
{
    let mut parsed = Vec::with_capacity(rows.len());
    for row in rows {
        let row_key_raw = row.get::<String, _>("row_key");
        let row_key = u128::from_str(&row_key_raw)
            .with_context(|| format!("invalid checker row key [{row_key_raw}] in state store"))?;
        parsed.push(CheckerStateRow {
            row_key,
            identity_key: row.get::<String, _>("identity_key"),
            identity_json: row.get::<String, _>("identity_json"),
            payload: row.get::<String, _>("row_payload"),
        });
    }
    Ok(parsed)
}
