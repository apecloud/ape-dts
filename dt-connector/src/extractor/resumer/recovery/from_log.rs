use anyhow::{bail, Context, Result};
use async_std::path::Path;
use async_trait::async_trait;
use dashmap::DashMap;
use tokio::{fs::File, io::AsyncBufReadExt, io::BufReader};

use crate::extractor::resumer::{
    recovery::{Recovery, RecoverySnapshotCache},
    utils::ResumerUtil,
    CURRENT_POSITION_LOG_FLAG, TAIL_POSITION_COUNT,
};
use dt_common::{
    config::{
        config_enums::{TaskKind, TaskType},
        resumer_config::ResumerConfig,
    },
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
        let position = Position::from_log(line);
        match &position {
            Position::RdbSnapshot { schema, tb, .. } | Position::FoxlakeS3 { schema, tb, .. } => {
                tb_positions.insert((schema.clone(), tb.clone()), position);
            }
            Position::RdbSnapshotFinished { schema, tb, .. } => {
                self.snapshot_cache
                    .finished_tbs
                    .insert((schema.clone(), tb.clone()), true);
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
            self.cdc_cache.insert(
                Self::cdc_cache_key(CDC_CURRENT_POSITION_KEY, &position),
                position,
            );
        } else {
            self.cdc_cache.insert(
                Self::cdc_cache_key(CDC_CHECKPOINT_POSITION_KEY, &position),
                position,
            );
        }
        Ok(())
    }

    fn cdc_cache_key(prefix: &str, position: &Position) -> String {
        format!(
            "{}:{}",
            prefix,
            ResumerUtil::get_key_from_position(position)
        )
    }

    fn get_cdc_resume_positions_by_prefix(&self, prefix: &str) -> Vec<Position> {
        let key_prefix = format!("{}:", prefix);
        let mut positions = self
            .cdc_cache
            .iter()
            .filter_map(|entry| {
                entry
                    .key()
                    .starts_with(&key_prefix)
                    .then(|| (entry.key().clone(), entry.value().clone()))
            })
            .collect::<Vec<_>>();
        positions.sort_by(|left, right| left.0.cmp(&right.0));
        positions
            .into_iter()
            .map(|(_, position)| position)
            .collect()
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
        match self.task_type.kind {
            TaskKind::Snapshot => {
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
            TaskKind::Cdc => {
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
        checkpoint: bool,
    ) -> Option<Position> {
        let key = (schema.to_string(), tb.to_string());
        let tb_positions =
            if !checkpoint && self.snapshot_cache.current_tb_positions.contains_key(&key) {
                &self.snapshot_cache.current_tb_positions
            } else {
                &self.snapshot_cache.checkpoint_tb_positions
            };
        tb_positions.get(&key).map(|p| p.clone())
    }
    async fn get_cdc_resume_position(&self) -> Option<Position> {
        let positions = self.get_cdc_resume_positions().await;
        positions.into_iter().next()
    }

    async fn get_cdc_resume_positions(&self) -> Vec<Position> {
        let checkpoint_positions =
            self.get_cdc_resume_positions_by_prefix(CDC_CHECKPOINT_POSITION_KEY);
        if !checkpoint_positions.is_empty() {
            checkpoint_positions
        } else {
            self.get_cdc_resume_positions_by_prefix(CDC_CURRENT_POSITION_KEY)
        }
    }
}

#[cfg(test)]
mod tests {
    use dashmap::DashMap;

    use super::{LogRecovery, RecoverySnapshotCache};
    use crate::extractor::resumer::recovery::Recovery;
    use dt_common::{
        config::config_enums::{TaskKind, TaskType},
        meta::position::Position,
    };

    fn new_log_recovery() -> LogRecovery {
        LogRecovery {
            task_type: TaskType::new(TaskKind::Cdc, None),
            resume_config_file: String::new(),
            resume_log_dir: String::new(),
            snapshot_cache: RecoverySnapshotCache {
                current_tb_positions: DashMap::new(),
                checkpoint_tb_positions: DashMap::new(),
                finished_tbs: DashMap::new(),
            },
            cdc_cache: DashMap::new(),
        }
    }

    fn redis_position(node_id: &str, repl_offset: u64) -> Position {
        Position::Redis {
            node_id: Some(node_id.to_string()),
            address: Some(format!("127.0.0.1:{repl_offset}")),
            repl_id: format!("repl-{node_id}"),
            repl_port: 10008,
            repl_offset,
            now_db_id: 0,
            timestamp: String::new(),
        }
    }

    #[tokio::test]
    async fn cdc_log_recovery_keeps_cluster_checkpoint_positions_by_node() {
        let recovery = new_log_recovery();
        let node_1_old = redis_position("node-1", 10);
        let node_1_new = redis_position("node-1", 30);
        let node_2 = redis_position("node-2", 20);

        recovery
            .parse_cdc_line(&format!("checkpoint_position | {}", node_1_old))
            .unwrap();
        recovery
            .parse_cdc_line(&format!("checkpoint_position | {}", node_2))
            .unwrap();
        recovery
            .parse_cdc_line(&format!("checkpoint_position | {}", node_1_new))
            .unwrap();

        let positions = recovery.get_cdc_resume_positions().await;
        assert_eq!(positions, vec![node_1_new, node_2]);
    }

    #[tokio::test]
    async fn cdc_log_recovery_prefers_checkpoints_over_current_positions() {
        let recovery = new_log_recovery();
        let current = redis_position("node-1", 10);
        let checkpoint = redis_position("node-1", 20);

        recovery
            .parse_cdc_line(&format!("current_position | {}", current))
            .unwrap();
        recovery
            .parse_cdc_line(&format!("checkpoint_position | {}", checkpoint.clone()))
            .unwrap();

        assert_eq!(recovery.get_cdc_resume_positions().await, vec![checkpoint]);
    }
}
