use std::str::FromStr;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use futures::TryStreamExt;
use mongodb::bson::doc;
use sqlx::{query, Error, Row};

use crate::extractor::resumer::{
    recovery::Recovery,
    utils::{RedisResumerRecord, ResumerUtil},
    ResumerDbPool, ResumerType,
};
use dt_common::{
    config::resumer_config::ResumerConfig, log_info, log_warn, meta::position::Position,
    utils::redis_util::RedisUtil,
};

pub struct DatabaseRecovery {
    task_id: String,
    pool: ResumerDbPool,
    schema: String,
    table: String,

    resumer_doing: DashMap<String, String>,
    resumer_finished: DashMap<String, bool>,
}

impl DatabaseRecovery {
    pub async fn new(
        task_id: &str,
        resumer_config: &ResumerConfig,
        pool: ResumerDbPool,
    ) -> Result<Self> {
        let recovery = match resumer_config {
            ResumerConfig::FromDB {
                table_full_name, ..
            } => {
                let (schema, table) = ResumerUtil::get_full_table_name(table_full_name)?;
                Self {
                    task_id: task_id.to_string(),
                    pool,
                    schema,
                    table,
                    resumer_doing: DashMap::new(),
                    resumer_finished: DashMap::new(),
                }
            }
            _ => {
                bail!("databaseRecovery only supports ResumerConfig::FromDB")
            }
        };
        recovery.initialization().await?;
        Ok(recovery)
    }

    fn cache_position_row(
        &self,
        resumer_type_str: &str,
        position_key: String,
        position_value_str: String,
    ) {
        if let Ok(resumer_type) = ResumerType::from_str(resumer_type_str) {
            match resumer_type {
                ResumerType::SnapshotDoing | ResumerType::CdcDoing => {
                    self.resumer_doing.insert(position_key, position_value_str);
                }
                ResumerType::SnapshotFinished => {
                    self.resumer_finished.insert(position_key, true);
                }
                _ => {
                    log_info!(
                        "resumer type: {} with task_id: {} not supported yet, skip this position",
                        resumer_type_str,
                        self.task_id
                    );
                }
            }
        } else {
            log_warn!(
                "invalid resumer type: {} with task_id: {}, skip this position",
                resumer_type_str,
                self.task_id
            );
        }
    }

    async fn initialization(&self) -> Result<()> {
        let sql = format!(
            r#"SELECT resumer_type, position_key, position_data 
               FROM {}.{} 
               WHERE task_id = '{}'
            "#,
            self.schema, self.table, self.task_id
        );

        match &self.pool {
            ResumerDbPool::MySql(pool) => {
                let mut position_rows = query(&sql).fetch(pool);
                loop {
                    match position_rows.try_next().await {
                        Ok(Some(row)) => {
                            let resumer_type_str: String = row.get("resumer_type");
                            let position_key: String = row.get("position_key");
                            let position_data: String = row.get("position_data");
                            self.cache_resumer_record(
                                &resumer_type_str,
                                position_key,
                                Some(position_data),
                            );
                        }
                        Ok(None) => {
                            break;
                        }
                        Err(e) => {
                            match &e {
                                Error::RowNotFound => {
                                    log::info!(
                                        "No resume position data found for task_id: {}, will start from beginning",
                                        self.task_id
                                    );
                                    break;
                                }
                                Error::Database(db_err) => {
                                    // MySQL error code 1146: Table doesn't exist, 1049: Unknown database
                                    if db_err.code().as_deref() == Some("1146")
                                        || db_err.code().as_deref() == Some("1049")
                                    {
                                        log::info!(
                                            "Resume table {}.{} does not exist, will start from beginning",
                                            self.schema, self.table
                                        );
                                        break;
                                    } else {
                                        bail!(
                                            "Failed to query resume position from database: {:?}",
                                            e
                                        );
                                    }
                                }
                                _ => {
                                    bail!("Failed to query resume position from database: {:?}", e);
                                }
                            }
                        }
                    }
                }
            }
            ResumerDbPool::Postgres(pool) => {
                let mut position_rows = query(&sql).fetch(pool);
                loop {
                    match position_rows.try_next().await {
                        Ok(Some(row)) => {
                            let resumer_type_str: String = row.get("resumer_type");
                            let position_key: String = row.get("position_key");
                            let position_data: String = row.get("position_data");
                            self.cache_resumer_record(
                                &resumer_type_str,
                                position_key,
                                Some(position_data),
                            );
                        }
                        Ok(None) => {
                            break;
                        }
                        Err(e) => {
                            match &e {
                                Error::RowNotFound => {
                                    log::info!(
                                        "No resume position data found for task_id: {}, will start from beginning",
                                        self.task_id
                                    );
                                    break;
                                }
                                Error::Database(db_err) => {
                                    // // PostgreSQL error code 42P01: undefined_table
                                    if db_err.code().as_deref() == Some("42P01") {
                                        log::info!(
                                            "Resume table {}.{} does not exist, will start from beginning",
                                            self.schema, self.table
                                        );
                                        break;
                                    } else {
                                        bail!(
                                            "Failed to query resume position from database: {:?}",
                                            e
                                        );
                                    }
                                }
                                _ => {
                                    bail!("Failed to query resume position from database: {:?}", e);
                                }
                            }
                        }
                    }
                }
            }
            ResumerDbPool::Mongo(client) => {
                let collection = client
                    .database(&self.schema)
                    .collection::<mongodb::bson::Document>(&self.table);
                let mut position_rows = collection.find(doc! { "task_id": &self.task_id }).await?;
                while let Some(row) = position_rows.try_next().await? {
                    let resumer_type_str = match row.get_str("resumer_type") {
                        Ok(value) => value.to_string(),
                        Err(e) => {
                            log_warn!(
                                "invalid MongoDB resumer row without resumer_type for task_id: {}, error: {}",
                                self.task_id,
                                e
                            );
                            continue;
                        }
                    };
                    let position_key = match row.get_str("position_key") {
                        Ok(value) => value.to_string(),
                        Err(e) => {
                            log_warn!(
                                "invalid MongoDB resumer row without position_key for task_id: {}, resumer_type: {}, error: {}",
                                self.task_id,
                                resumer_type_str,
                                e
                            );
                            continue;
                        }
                    };
                    let position_value_str =
                        row.get_str("position_data").unwrap_or_default().to_string();
                    self.cache_position_row(&resumer_type_str, position_key, position_value_str);
                }
            }
            ResumerDbPool::Redis(redis_conn) => {
                let mut conn =
                    RedisUtil::create_redis_conn(&redis_conn.url, &redis_conn.connection_auth)
                        .await?;
                let pattern = ResumerUtil::get_redis_resumer_scan_pattern(
                    &self.task_id,
                    redis_conn.hash_tag.as_deref(),
                );
                let keys = ResumerUtil::scan_redis_keys(&mut conn, &pattern)?;
                for key in keys {
                    let Some(value) = redis::cmd("GET")
                        .arg(&key)
                        .query::<Option<String>>(&mut conn)
                        .with_context(|| format!("failed to get Redis resumer key: {}", key))?
                    else {
                        continue;
                    };
                    let record: RedisResumerRecord =
                        serde_json::from_str(&value).with_context(|| {
                            format!("failed to parse Redis resumer value for key: {}", key)
                        })?;
                    self.cache_resumer_record(
                        &record.resumer_type,
                        record.position_key,
                        Some(record.position_data),
                    );
                }
            }
        }
        Ok(())
    }

    fn cache_resumer_record(
        &self,
        resumer_type_str: &str,
        position_key: String,
        position_data: Option<String>,
    ) {
        if let Ok(resumer_type) = ResumerType::from_str(resumer_type_str) {
            match resumer_type {
                ResumerType::SnapshotDoing | ResumerType::CdcDoing => {
                    if let Some(position_data) = position_data {
                        self.resumer_doing.insert(position_key, position_data);
                    }
                }
                ResumerType::SnapshotFinished => {
                    self.resumer_finished.insert(position_key, true);
                }
                _ => {
                    log_info!(
                        "resumer type: {} with task_id: {} not supported yet, skip this position",
                        resumer_type_str,
                        self.task_id
                    );
                }
            }
        } else {
            log_warn!(
                "invalid resumer type: {} with task_id: {}, skip this position",
                resumer_type_str,
                self.task_id
            );
        }
    }
}

#[async_trait]
impl Recovery for DatabaseRecovery {
    async fn check_snapshot_finished(&self, schema: &str, tb: &str) -> bool {
        let resumer_key = ResumerUtil::get_key_from_base(
            (schema.to_string(), tb.to_string()),
            ResumerType::SnapshotFinished,
        );
        self.resumer_finished.contains_key(&resumer_key)
    }

    async fn get_snapshot_resume_position(
        &self,
        schema: &str,
        tb: &str,
        _checkpoint: bool,
    ) -> Option<Position> {
        let resumer_key = ResumerUtil::get_key_from_base(
            (schema.to_string(), tb.to_string()),
            ResumerType::SnapshotDoing,
        );
        let position_str = self.resumer_doing.get(&resumer_key).map(|p| p.to_owned());
        if let Some(position_str) = position_str {
            let position = Position::from_log(&position_str);
            match &position {
                Position::RdbSnapshot { .. } => return Some(position),
                _ => return None,
            }
        }
        None
    }

    async fn get_cdc_resume_position(&self) -> Option<Position> {
        let resumer_key =
            ResumerUtil::get_key_from_base(("".to_string(), "".to_string()), ResumerType::CdcDoing);
        let position_str = self.resumer_doing.get(&resumer_key).map(|p| p.to_owned());
        if let Some(position_str) = position_str {
            return Some(Position::from_log(&position_str));
        }
        None
    }

    async fn get_cdc_resume_positions(&self) -> Vec<Position> {
        self.resumer_doing
            .iter()
            .filter_map(|entry| {
                let position = Position::from_log(entry.value());
                (!matches!(position, Position::None)).then_some(position)
            })
            .collect()
    }
}
