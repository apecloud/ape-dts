use anyhow::{bail, Context, Result};
use async_std::path::Path;
use async_trait::async_trait;
use dashmap::DashMap;
use tokio::{fs::File, io::AsyncBufReadExt, io::BufReader};

use crate::extractor::resumer::{
    recovery::{Recovery, RecoverySnapshotCache},
    CURRENT_POSITION_LOG_FLAG, TAIL_POSITION_COUNT,
};
use dt_common::{
    config::{config_enums::TaskType, resumer_config::ResumerConfig},
    log_warn,
    meta::position::Position,
    utils::file_util::FileUtil,
};

const CDC_CURRENT_POSITION_KEY: &str = "current_position";
const CDC_CHECKPOINT_POSITION_KEY: &str = "checkpoint_position";

pub struct LogRecovery {
    task_type: TaskType,

    resume_config_file: String,
    resume_log_dir: String,

    snapshot_cache: RecoverySnapshotCache,
    cdc_cache: DashMap<String, Position>,
}

impl LogRecovery {
    pub async fn new(task_type: TaskType, resumer_config: &ResumerConfig) -> Result<Self> {
        let recovery = match resumer_config {
            ResumerConfig::FromLog {
                log_dir,
                config_file,
            } => Self {
                task_type,
                resume_config_file: config_file.to_string(),
                resume_log_dir: log_dir.to_string(),
                snapshot_cache: RecoverySnapshotCache {
                    current_tb_positions: DashMap::new(),
                    checkpoint_tb_positions: DashMap::new(),
                    finished_tbs: DashMap::new(),
                },
                cdc_cache: DashMap::new(),
            },
            _ => {
                bail!("logRecovery only supports ResumerConfig::FromLog");
            }
        };
        recovery.initialization().await?;
        Ok(recovery)
    }

    fn parse_snapshot_line(&self, line: &str) -> Result<()> {
        let tb_positions = if line.contains(CURRENT_POSITION_LOG_FLAG) {
            &self.snapshot_cache.current_tb_positions
        } else {
            &self.snapshot_cache.checkpoint_tb_positions
        };

        match Position::from_log(line) {
            Position::RdbSnapshot {
                schema,
                tb,
                order_col,
                value,
                ..
            } => {
                tb_positions.insert((schema, tb, order_col), value);
            }

            Position::FoxlakeS3 {
                schema,
                tb,
                s3_meta_file,
            } => {
                tb_positions.insert((schema, tb, String::new()), s3_meta_file);
            }

            Position::RdbSnapshotFinished { schema, tb, .. } => {
                self.snapshot_cache.finished_tbs.insert((schema, tb), true);
            }

            _ => {}
        }
        Ok(())
    }

    fn parse_cdc_line(&self, line: &str) -> Result<()> {
        let position = Position::from_log(line);
        // ignore position log lines like:
        // 2025-02-18 04:13:04.655541 | checkpoint_position | {"type":"None"}
        if position == Position::None {
            return Ok(());
        }

        if line.contains(CURRENT_POSITION_LOG_FLAG) {
            self.cdc_cache
                .insert(CDC_CURRENT_POSITION_KEY.to_string(), position);
        } else {
            self.cdc_cache
                .insert(CDC_CHECKPOINT_POSITION_KEY.to_string(), position);
        }
        Ok(())
    }

    async fn parse_config_file<F>(
        &self,
        config_file_path: &str,
        tail_limit: Option<usize>,
        handler: F,
    ) -> Result<()>
    where
        F: Fn(&Self, &str) -> Result<()>,
    {
        if Path::new(config_file_path).exists().await {
            if let Some(tail_limit) = tail_limit {
                let lines = FileUtil::tail(config_file_path, tail_limit).await?;
                for line in lines {
                    handler(self, &line)?;
                }
            } else {
                let file = File::open(config_file_path).await.with_context(|| {
                    format!(
                        "failed to open recovery config file: [{}] while it exists",
                        config_file_path
                    )
                })?;
                let mut lines = BufReader::new(file).lines();
                while let Some(line) = lines.next_line().await? {
                    handler(self, &line)?;
                }
            }
        } else {
            log_warn!(
                "recovery config file: [{}] does not exist",
                config_file_path
            );
        }
        Ok(())
    }

    async fn initialization(&self) -> Result<()> {
        match self.task_type {
            TaskType::Snapshot => {
                if !self.resume_config_file.is_empty() {
                    self.parse_config_file(&self.resume_config_file, None, |self_ref, line| {
                        self_ref.parse_snapshot_line(line)
                    })
                    .await?;
                }

                if !self.resume_log_dir.is_empty() {
                    self.parse_config_file(
                        &format!("{}/position.log", self.resume_log_dir),
                        Some(TAIL_POSITION_COUNT),
                        |self_ref, line| self_ref.parse_snapshot_line(line),
                    )
                    .await?;

                    self.parse_config_file(
                        &format!("{}/finished.log", self.resume_log_dir),
                        None,
                        |self_ref, line| self_ref.parse_snapshot_line(line),
                    )
                    .await?;
                }
            }
            TaskType::Cdc => {
                if !self.resume_config_file.is_empty() {
                    self.parse_config_file(&self.resume_config_file, None, |self_ref, line| {
                        self_ref.parse_cdc_line(line)
                    })
                    .await?;
                }

                if !self.resume_log_dir.is_empty() {
                    self.parse_config_file(
                        &format!("{}/position.log", self.resume_log_dir),
                        Some(TAIL_POSITION_COUNT),
                        |self_ref, line| self_ref.parse_cdc_line(line),
                    )
                    .await?;
                }
            }
            _ => bail!("logRecovery not supports TaskType: {:?}", self.task_type),
        }
        Ok(())
    }
}

#[async_trait]
impl Recovery for LogRecovery {
    async fn check_snapshot_finished(&self, schema: &str, tb: &str) -> bool {
        self.snapshot_cache
            .finished_tbs
            .contains_key(&(schema.to_string(), tb.to_string()))
    }

    async fn get_snapshot_resume_position(
        &self,
        schema: &str,
        tb: &str,
        col: &str,
        checkpoint: bool,
    ) -> Option<String> {
        let key = (schema.to_string(), tb.to_string(), col.to_string());
        let tb_positions =
            if !checkpoint && self.snapshot_cache.current_tb_positions.contains_key(&key) {
                &self.snapshot_cache.current_tb_positions
            } else {
                &self.snapshot_cache.checkpoint_tb_positions
            };

        if let Some(value) = tb_positions.get(&key) {
            Some(value.to_owned())
        } else {
            None
        }
    }

    async fn get_cdc_resume_position(&self) -> Option<Position> {
        if let Some(commit_position) = self.cdc_cache.get(CDC_CHECKPOINT_POSITION_KEY) {
            Some(commit_position.clone())
        } else {
            self.cdc_cache
                .get(CDC_CURRENT_POSITION_KEY)
                .map(|p| p.clone())
        }
    }
}
