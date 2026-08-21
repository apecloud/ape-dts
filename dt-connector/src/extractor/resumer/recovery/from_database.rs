use std::str::FromStr;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use dt_common::{
    config::config_enums::DbType,
    config::resumer_config::ResumerConfig,
    error::{
        classify_mssql_error, classify_sqlx_error, DtError, DtErrorContextExt, DtResultExt,
        EndpointRole, ErrorCode, ErrorObject, Stage,
    },
    log_info, log_warn,
    meta::{adaptor::mssql_col_value_convertor::MssqlColValueConvertor, position::Position},
    utils::{redis_util::RedisUtil, sql_util::SqlUtil},
};
use futures::TryStreamExt;
use mongodb::bson::doc;
use sqlx::{query, Error as SqlxError, Row};
use tiberius::Query;

use crate::extractor::resumer::{
    recovery::Recovery,
    utils::{RedisResumerRecord, ResumerUtil},
    ResumerDbPool, ResumerType,
};

pub struct DatabaseRecovery {
    task_id: String,
    pool: ResumerDbPool,
    db: String,
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
                db_type,
                table_full_name,
                ..
            } => {
                let (db, schema, table) =
                    ResumerUtil::get_checkpoint_db_schema_tb(table_full_name, db_type)?;
                Self {
                    task_id: task_id.to_string(),
                    pool,
                    db,
                    schema,
                    table,
                    resumer_doing: DashMap::new(),
                    resumer_finished: DashMap::new(),
                }
            }
            _ => {
                bail!(DtError::invalid_config(
                    "database checkpoint recovery requires resume_type=from_db",
                ))
            }
        };
        recovery
            .initialization()
            .await
            .stage(Stage::Resumer)
            .endpoint(EndpointRole::Metadata)?;
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
        match &self.pool {
            ResumerDbPool::MySql(pool) => {
                let sql = format!(
                    "SELECT resumer_type, position_key, position_data \
                     FROM {}.{} WHERE task_id = ?",
                    self.schema, self.table
                );
                let mut position_rows = query(&sql).bind(&self.task_id).fetch(pool);
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
                        Err(error) => match error {
                            SqlxError::RowNotFound => {
                                log::info!(
                                        "No resume position data found for task_id: {}, will start from beginning",
                                        self.task_id
                                    );
                                break;
                            }
                            _ => {
                                let is_missing_resume_store = classify_sqlx_error(&error)
                                    .error_code()
                                    .is_some_and(Self::is_missing_resume_store);
                                let error = error
                                    .code(ErrorCode::CheckpointReadFailed)
                                    .message("failed to query resume position from database")
                                    .object(ErrorObject {
                                        schema: Some(self.schema.clone()),
                                        table: Some(self.table.clone()),
                                        ..Default::default()
                                    });
                                if is_missing_resume_store {
                                    log::info!(
                                            "Resume table {}.{} does not exist, will start from beginning",
                                            self.schema, self.table
                                        );
                                    break;
                                }
                                return Err(error);
                            }
                        },
                    }
                }
            }
            ResumerDbPool::Postgres(pool) => {
                let sql = format!(
                    "SELECT resumer_type, position_key, position_data \
                     FROM {}.{} WHERE task_id = $1",
                    self.schema, self.table
                );
                let mut position_rows = query(&sql).bind(&self.task_id).fetch(pool);
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
                        Err(error) => match error {
                            SqlxError::RowNotFound => {
                                log::info!(
                                        "No resume position data found for task_id: {}, will start from beginning",
                                        self.task_id
                                    );
                                break;
                            }
                            _ => {
                                let is_missing_resume_store = classify_sqlx_error(&error)
                                    .error_code()
                                    .is_some_and(Self::is_missing_resume_store);
                                let error = error
                                    .code(ErrorCode::CheckpointReadFailed)
                                    .message("failed to query resume position from database")
                                    .object(ErrorObject {
                                        schema: Some(self.schema.clone()),
                                        table: Some(self.table.clone()),
                                        ..Default::default()
                                    });
                                if is_missing_resume_store {
                                    log::info!(
                                            "Resume table {}.{} does not exist, will start from beginning",
                                            self.schema, self.table
                                        );
                                    break;
                                }
                                return Err(error);
                            }
                        },
                    }
                }
            }
            ResumerDbPool::Mssql(pool) => {
                let full_table_name = self.mssql_full_table_name();
                let mut query = Query::new(format!(
                    "SELECT resumer_type, position_key, position_data \
                     FROM {full_table_name} WHERE task_id = @P1"
                ));
                query.bind(self.task_id.as_str());

                let mut connection = pool
                    .get()
                    .await
                    .code(ErrorCode::CheckpointReadFailed)
                    .message("failed to acquire MSSQL checkpoint connection")?;
                let stream = match query.query(connection.client_mut()).await {
                    Ok(stream) => stream,
                    Err(error) => return self.handle_mssql_query_error(error),
                };
                let position_rows = match stream.into_first_result().await {
                    Ok(rows) => rows,
                    Err(error) => return self.handle_mssql_query_error(error),
                };

                for row in position_rows {
                    let resumer_type_str =
                        MssqlColValueConvertor::from_query_required_string(&row, "resumer_type")
                            .code(ErrorCode::CheckpointReadFailed)
                            .message("failed to parse MSSQL checkpoint resumer_type")
                            .object(self.checkpoint_error_object())?;
                    let position_key =
                        MssqlColValueConvertor::from_query_required_string(&row, "position_key")
                            .code(ErrorCode::CheckpointReadFailed)
                            .message("failed to parse MSSQL checkpoint position_key")
                            .object(self.checkpoint_error_object())?;
                    let position_data =
                        MssqlColValueConvertor::from_query_optional_string(&row, "position_data")
                            .code(ErrorCode::CheckpointReadFailed)
                            .message("failed to parse MSSQL checkpoint position_data")
                            .object(self.checkpoint_error_object())?;
                    self.cache_resumer_record(&resumer_type_str, position_key, position_data);
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

    fn mssql_full_table_name(&self) -> String {
        SqlUtil::render_rdb_table(&DbType::Mssql, &self.db, &self.schema, &self.table)
    }

    fn handle_mssql_query_error(&self, error: tiberius::error::Error) -> Result<()> {
        let is_missing_resume_store = classify_mssql_error(&error)
            .error_code()
            .is_some_and(Self::is_missing_resume_store);
        if is_missing_resume_store {
            log::info!(
                "Resume table {} does not exist, will start from beginning",
                self.mssql_full_table_name()
            );
            return Ok(());
        }

        Err(error
            .code(ErrorCode::CheckpointReadFailed)
            .message("failed to query resume position from MSSQL")
            .object(self.checkpoint_error_object()))
    }

    fn checkpoint_error_object(&self) -> ErrorObject {
        ErrorObject {
            schema: Some(self.schema.clone()),
            table: Some(self.table.clone()),
            ..Default::default()
        }
    }

    fn is_missing_resume_store(code: ErrorCode) -> bool {
        matches!(
            code,
            ErrorCode::ObjectNotFound | ErrorCode::DatabaseNotFound
        )
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
    async fn check_snapshot_finished(&self, db: &str, schema: &str, tb: &str) -> bool {
        let resumer_key = ResumerUtil::get_key_from_base(
            (db.to_string(), schema.to_string(), tb.to_string()),
            ResumerType::SnapshotFinished,
        );
        self.resumer_finished.contains_key(&resumer_key)
    }

    async fn get_snapshot_resume_position(
        &self,
        db: &str,
        schema: &str,
        tb: &str,
        _checkpoint: bool,
    ) -> Option<Position> {
        let resumer_key = ResumerUtil::get_key_from_base(
            (db.to_string(), schema.to_string(), tb.to_string()),
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
        let resumer_key = ResumerUtil::get_key_from_base(
            (String::new(), String::new(), String::new()),
            ResumerType::CdcDoing,
        );
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
