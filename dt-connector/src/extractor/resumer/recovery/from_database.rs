use std::str::FromStr;

use anyhow::{bail, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use futures::TryStreamExt;
use sqlx::{query, Error, Row};

use crate::extractor::resumer::{
    recovery::Recovery, utils::ResumerUtil, ResumerDbPool, ResumerType,
};
use dt_common::{
    config::resumer_config::ResumerConfig, log_info, log_warn, meta::position::Position,
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
                            if let Ok(resumer_type) = ResumerType::from_str(&resumer_type_str) {
                                match resumer_type {
                                    ResumerType::SnapshotDoing => {
                                        let position_key: String = row.get("position_key");
                                        let position_value_str: String = row.get("position_data");
                                        self.resumer_doing.insert(position_key, position_value_str);
                                    }
                                    ResumerType::CdcDoing => {
                                        let position_key: String = row.get("position_key");
                                        let position_value_str: String = row.get("position_data");
                                        self.resumer_doing.insert(position_key, position_value_str);
                                    }
                                    ResumerType::SnapshotFinished => {
                                        let position_key: String = row.get("position_key");
                                        self.resumer_finished.insert(position_key, true);
                                    }
                                    _ => {
                                        log_info!("resumer type: {} with task_id: {} not supported yet, skip this position", resumer_type_str, self.task_id);
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
                            if let Ok(resumer_type) = ResumerType::from_str(&resumer_type_str) {
                                match resumer_type {
                                    ResumerType::SnapshotDoing => {
                                        let position_key: String = row.get("position_key");
                                        let position_value_str: String = row.get("position_data");
                                        self.resumer_doing.insert(position_key, position_value_str);
                                    }
                                    ResumerType::CdcDoing => {
                                        let position_key: String = row.get("position_key");
                                        let position_value_str: String = row.get("position_data");
                                        self.resumer_doing.insert(position_key, position_value_str);
                                    }
                                    ResumerType::SnapshotFinished => {
                                        let position_key: String = row.get("position_key");
                                        self.resumer_finished.insert(position_key, true);
                                    }
                                    _ => {
                                        log_info!("resumer type: {} with task_id: {} not supported yet, skip this position", resumer_type_str, self.task_id);
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
        }
        Ok(())
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
                Position::RdbSnapshot { .. } | Position::FoxlakeS3 { .. } => return Some(position),
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
}
