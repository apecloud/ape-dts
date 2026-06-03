use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use sqlx::query;

use crate::extractor::resumer::{
    recorder::Recorder, utils::ResumerUtil, ResumerDbPool, ResumerType,
};
use dt_common::{config::resumer_config::ResumerConfig, log_info, meta::position::Position};

pub struct DatabaseRecorder {
    task_id: String,
    pool: ResumerDbPool,
    schema: String,
    table: String,
}

impl DatabaseRecorder {
    pub async fn new(
        task_id: &str,
        resumer_config: &ResumerConfig,
        pool: ResumerDbPool,
        is_init: bool,
    ) -> anyhow::Result<Self> {
        let recorder = match resumer_config {
            ResumerConfig::FromDB {
                table_full_name, ..
            } => {
                let (schema, table) = ResumerUtil::get_full_table_name(table_full_name)?;
                Self {
                    task_id: task_id.to_string(),
                    pool,
                    schema,
                    table,
                }
            }
            _ => {
                bail!("databaseRecorder only supports ResumerConfig::FromDB")
            }
        };
        recorder.initialization(is_init, task_id).await?;
        Ok(recorder)
    }

    async fn initialization(&self, is_init: bool, task_id: &str) -> Result<()> {
        log_info!(
            "DatabaseRecorderInner initialized, task_id: {}, schema: {}, table: {}",
            self.task_id,
            self.schema,
            self.table
        );
        match &self.pool {
            ResumerDbPool::MySql(pool) => {
                let db_sql = format!("CREATE DATABASE IF NOT EXISTS `{}`", self.schema);
                let tb_sql = format!(
                    r#"
                    CREATE TABLE IF NOT EXISTS `{}`.`{}` (
                      id bigint AUTO_INCREMENT PRIMARY KEY,
                      task_id varchar(255) NOT NULL,
                      resumer_type varchar(255) NOT NULL,
                      position_key varchar(255) NOT NULL,
                      position_data text,
                      created_at timestamp DEFAULT CURRENT_TIMESTAMP,
                      updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                      UNIQUE KEY `uk_task_id_task_type_position_key` (task_id, resumer_type, position_key)
                    )"#,
                    self.schema, self.table
                );
                query(&db_sql)
                    .execute(pool)
                    .await
                    .context(format!("failed to create database: {}", db_sql))?;
                query(&tb_sql)
                    .execute(pool)
                    .await
                    .context(format!("failed to create table: {}", tb_sql))?;
                if is_init {
                    let delete_sql = format!(
                        "DELETE FROM `{}`.`{}` WHERE task_id = ?",
                        self.schema, self.table
                    );
                    query(&delete_sql)
                        .bind(task_id)
                        .execute(pool)
                        .await
                        .context(format!(
                            "failed to delete resumer records for task_id={} with sql: {}",
                            task_id, delete_sql
                        ))?;
                }
                Ok(())
            }
            ResumerDbPool::Postgres(pool) => {
                let db_sql = format!("CREATE SCHEMA IF NOT EXISTS {}", self.schema);
                let tb_sql = format!(
                    r#"
                    CREATE TABLE IF NOT EXISTS {}.{} (
                      id bigserial PRIMARY KEY,
                      task_id varchar(255) NOT NULL,
                      resumer_type varchar(100) NOT NULL,
                      position_key varchar(255) NOT NULL,
                      position_data text,
                      created_at timestamp DEFAULT CURRENT_TIMESTAMP,
                      updated_at timestamp DEFAULT CURRENT_TIMESTAMP,
                      CONSTRAINT uk_task_id_task_type_position_key UNIQUE (task_id, resumer_type, position_key)
                    )"#,
                    self.schema, self.table
                );

                query(&db_sql)
                    .execute(pool)
                    .await
                    .context(format!("failed to create schema: {}", db_sql))?;
                query(&tb_sql)
                    .execute(pool)
                    .await
                    .context(format!("failed to create table: {}", tb_sql))?;
                let sequence_sql = self.pg_sync_id_sequence_sql();
                query(&sequence_sql).execute(pool).await.context(format!(
                    "failed to sync resumer id sequence with sql: {}",
                    sequence_sql
                ))?;
                if is_init {
                    let delete_sql = format!(
                        "DELETE FROM {}.{} WHERE task_id = $1",
                        self.schema, self.table
                    );
                    query(&delete_sql)
                        .bind(task_id)
                        .execute(pool)
                        .await
                        .context(format!(
                            "failed to delete resumer records for task_id={} with sql: {}",
                            task_id, delete_sql
                        ))?;
                }
                Ok(())
            }
        }
    }

    fn pg_sync_id_sequence_sql(&self) -> String {
        let table_full_name = format!("{}.{}", self.schema, self.table);
        let escaped_table_full_name = table_full_name.replace('\'', "''");
        format!(
            "SELECT setval(
                pg_get_serial_sequence('{}', 'id'),
                COALESCE((SELECT MAX(id) FROM {}), 1),
                COALESCE((SELECT MAX(id) FROM {}), 0) > 0
            )",
            escaped_table_full_name, table_full_name, table_full_name
        )
    }
}

#[async_trait]
impl Recorder for DatabaseRecorder {
    async fn record_position(&self, position: &Position) -> Result<()> {
        let resumer_type = ResumerType::from_position(position);
        if matches!(resumer_type, ResumerType::NotSupported) {
            log_info!("recorder not supported resumer type: {:?}", position);
            return Ok(());
        }

        match &self.pool {
            ResumerDbPool::MySql(pool) => {
                let sql = format!(
                    "INSERT INTO `{}`.`{}` (task_id, resumer_type, position_key, position_data)
                    VALUES (?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                        position_data = VALUES(position_data),
                        updated_at = CURRENT_TIMESTAMP",
                    self.schema, self.table
                );

                query(&sql)
                    .bind(&self.task_id)
                    .bind(resumer_type.to_string())
                    .bind(ResumerUtil::get_key_from_position(position))
                    .bind(position.to_string())
                    .execute(pool)
                    .await
                    .with_context(|| format!("failed to upsert position record with sql: {sql}"))?;
                Ok(())
            }
            ResumerDbPool::Postgres(pool) => {
                let sql = format!(
                    "INSERT INTO {}.{} (task_id, resumer_type, position_key, position_data)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (task_id, resumer_type, position_key)
                    DO UPDATE SET
                        position_data = EXCLUDED.position_data,
                        updated_at = CURRENT_TIMESTAMP",
                    self.schema, self.table
                );

                query(&sql)
                    .bind(&self.task_id)
                    .bind(resumer_type.to_string())
                    .bind(ResumerUtil::get_key_from_position(position))
                    .bind(position.to_string())
                    .execute(pool)
                    .await
                    .with_context(|| format!("failed to upsert position record with sql: {sql}"))?;
                Ok(())
            }
        }
    }
}
