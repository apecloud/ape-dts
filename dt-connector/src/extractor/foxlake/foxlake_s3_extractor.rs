use std::{cmp, collections::HashMap, path::PathBuf, str::FromStr, sync::Arc};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use dt_common::{
    config::config_enums::{DbType, RdbParallelType},
    config::s3_config::S3Config,
    log_debug, log_info, log_warn,
    meta::{dt_data::DtData, foxlake::s3_file_meta::S3FileMeta, position::Position},
    monitor::{monitor_task_id, task_monitor::TaskMonitorHandle},
    utils::time_util::TimeUtil,
};
use opendal::options::ListOptions;
use opendal::Operator;
use tokio::task::JoinSet;

use crate::{
    extractor::{
        base_extractor::{BaseExtractor, ExtractState},
        extractor_monitor::ExtractorMonitor,
        resumer::recovery::Recovery,
    },
    Extractor,
};

pub struct FoxlakeS3Extractor {
    pub s3_config: S3Config,
    pub s3_client: Operator,
    pub base_extractor: BaseExtractor,
    pub extract_state: ExtractState,
    pub batch_size: usize,
    pub parallel_size: usize,
    pub schema: String,
    pub tb: String,
    pub schema_tbs: HashMap<String, Vec<String>>,
    pub parallel_type: RdbParallelType,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

const FINISHED: &str = "finished";
const WAIT_FILE_SECS: u64 = 5;

struct TableMonitorGuard {
    handle: TaskMonitorHandle,
    task_id: String,
}

impl Drop for TableMonitorGuard {
    fn drop(&mut self) {
        self.handle.unregister_monitor(&self.task_id);
    }
}

#[async_trait]
impl Extractor for FoxlakeS3Extractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        if self.parallel_size < 1 {
            anyhow::bail!("parallel_size must be greater than 0");
        }
        if matches!(self.parallel_type, RdbParallelType::Chunk) {
            anyhow::bail!("foxlake s3 extractor does not support parallel_type=chunk");
        }

        let tables = self.collect_tables();
        let mut join_set = JoinSet::new();
        let mut iter = tables.into_iter();

        while join_set.len() < self.parallel_size {
            let Some((schema, tb)) = iter.next() else {
                break;
            };
            let this = self.clone_for_dispatch();
            join_set.spawn(async move { this.run_table_worker(schema, tb).await });
        }

        while let Some(result) = join_set.join_next().await {
            result.map_err(|e| anyhow!("foxlake table worker join error: {}", e))??;

            if let Some((schema, tb)) = iter.next() {
                let this = self.clone_for_dispatch();
                join_set.spawn(async move { this.run_table_worker(schema, tb).await });
            }
        }

        self.base_extractor
            .wait_task_finish(&mut self.extract_state)
            .await
    }
}

impl FoxlakeS3Extractor {
    fn collect_tables(&self) -> Vec<(String, String)> {
        if !self.schema.is_empty() && !self.tb.is_empty() {
            return vec![(self.schema.clone(), self.tb.clone())];
        }

        let mut tables = Vec::new();
        for (schema, tbs) in &self.schema_tbs {
            for tb in tbs {
                tables.push((schema.clone(), tb.clone()));
            }
        }
        tables
    }

    fn clone_for_dispatch(&self) -> Self {
        Self {
            s3_config: self.s3_config.clone(),
            s3_client: self.s3_client.clone(),
            base_extractor: self.base_extractor.clone(),
            extract_state: ExtractState {
                monitor: ExtractorMonitor {
                    monitor: self.extract_state.monitor.monitor.clone(),
                    default_task_id: self.extract_state.monitor.default_task_id.clone(),
                    count_window: self.extract_state.monitor.count_window,
                    time_window_secs: self.extract_state.monitor.time_window_secs,
                    last_flush_time: tokio::time::Instant::now(),
                    flushed_counters: Default::default(),
                    counters: Default::default(),
                },
                data_marker: self.extract_state.data_marker.clone(),
                time_filter: self.extract_state.time_filter.clone(),
            },
            batch_size: self.batch_size,
            parallel_size: self.parallel_size,
            schema: self.schema.clone(),
            tb: self.tb.clone(),
            schema_tbs: self.schema_tbs.clone(),
            parallel_type: self.parallel_type.clone(),
            recovery: self.recovery.clone(),
        }
    }

    async fn run_table_worker(&self, schema: String, tb: String) -> anyhow::Result<()> {
        let task_id = monitor_task_id::from_schema_tb(&schema, &tb);
        let monitor_handle = self.extract_state.monitor.monitor.clone();
        let monitor = monitor_handle.build_monitor("extractor", &task_id);
        monitor_handle.register_monitor(&task_id, monitor.clone());
        let _guard = TableMonitorGuard {
            handle: monitor_handle.clone(),
            task_id: task_id.clone(),
        };

        let extractor_monitor = ExtractorMonitor::new(monitor_handle.clone(), task_id).await;
        let data_marker = self.extract_state.data_marker.clone();
        let mut extract_state = self
            .extract_state
            .derive_for_table(extractor_monitor, data_marker)
            .await;
        let base_extractor = self.base_extractor.clone();

        log_info!(
            "FoxlakeS3Extractor starts, schema: `{}`, tb: `{}`, batch_size: {}",
            schema,
            tb,
            self.batch_size
        );

        let mut start_after = if let Some(handler) = &self.recovery {
            if let Some(Position::FoxlakeS3 { s3_meta_file, .. }) = handler
                .get_snapshot_resume_position(&schema, &tb, false)
                .await
            {
                log_info!(
                    "[{}.{}] recovery from [{}]:[{}]",
                    schema,
                    tb,
                    "",
                    s3_meta_file
                );
                Some(s3_meta_file)
            } else {
                None
            }
        } else {
            None
        };

        loop {
            let mut finished = false;
            let meta_files = self.list_meta_file(&schema, &tb, &start_after).await?;
            let continuous_meta_files = Self::find_continuous_files(&meta_files, &start_after);
            if continuous_meta_files.len() != meta_files.len() {
                log_warn!(
                    "meta files are not continuous, start_after: {:?}, meta_files: {}",
                    start_after,
                    meta_files.join(",")
                );
            }

            if continuous_meta_files.is_empty() {
                TimeUtil::sleep_millis(WAIT_FILE_SECS * 1000).await;
                continue;
            }

            for file in &continuous_meta_files {
                if file.ends_with(FINISHED) {
                    finished = true;
                    break;
                }
                let file_meta = self.get_meta(file).await?;
                let dt_data = DtData::Foxlake { file_meta };
                let position = Position::FoxlakeS3 {
                    schema: schema.clone(),
                    tb: tb.clone(),
                    s3_meta_file: file.to_owned(),
                };
                base_extractor
                    .push_dt_data(&mut extract_state, dt_data, position)
                    .await?;
            }

            if finished {
                let dt_data = DtData::Foxlake {
                    file_meta: S3FileMeta {
                        push_epoch: i64::MAX,
                        ..Default::default()
                    },
                };
                base_extractor
                    .push_dt_data(&mut extract_state, dt_data, Position::None)
                    .await?;
                break;
            }

            start_after = continuous_meta_files.last().map(|s: &String| s.to_string());
        }

        base_extractor.push_snapshot_finished(
            &schema,
            &tb,
            Position::RdbSnapshotFinished {
                db_type: DbType::Foxlake.to_string(),
                schema: schema.clone(),
                tb: tb.clone(),
            },
        )?;
        extract_state.monitor.try_flush(true).await;
        Ok(())
    }

    async fn get_meta(&self, key: &str) -> anyhow::Result<S3FileMeta> {
        let output = self
            .s3_client
            .read(key)
            .await
            .with_context(|| format!("failed to get s3 object, key: {}", &key))?;

        let content = String::from_utf8(output.to_vec())?;
        S3FileMeta::from_str(&content).with_context(|| format!("invalid s3 file meta: [{}]", key))
    }

    async fn list_meta_file(
        &self,
        schema: &str,
        tb: &str,
        start_after: &Option<String>,
    ) -> anyhow::Result<Vec<String>> {
        let mut prefix = format!("{}/{}/meta/", schema, tb);
        if !self.s3_config.root_dir.is_empty() {
            prefix = format!("{}/{}", self.s3_config.root_dir, prefix);
        }

        log_debug!(
            "list meta files, prefix: {}, start_after: {:?}",
            &prefix,
            start_after
        );

        let contents = self
            .s3_client
            .list_options(
                &prefix,
                ListOptions {
                    start_after: start_after.clone(),
                    limit: Some(self.batch_size),
                    ..Default::default()
                },
            )
            .await
            .with_context(|| {
                format!(
                    "failed to list s3 objects, prefix: {}, start_after: {:?}",
                    &prefix, start_after
                )
            })?;

        let mut file_names = Vec::new();
        for entry in contents {
            let (path, _) = entry.into_parts();
            file_names.push(path);
        }

        log_debug!(
            "found meta files, count: {}, last file: {:?}",
            file_names.len(),
            file_names.last()
        );

        Ok(file_names)
    }

    fn find_continuous_files(meta_files: &[String], start_after: &Option<String>) -> Vec<String> {
        let mut prev_meta_file = &String::new();
        let (mut prev_id, mut prev_sequence) = (0, 0);
        if let Some(v) = start_after {
            (prev_id, prev_sequence) = Self::parse_meta_file_name(v);
            prev_meta_file = v;
        }

        let mut discontinue_from = meta_files.len();
        for i in 0..meta_files.len() {
            let meta_file = &meta_files[i];
            if i == meta_files.len() - 1 && meta_file.ends_with(FINISHED) {
                continue;
            }

            let (id, sequence) = Self::parse_meta_file_name(meta_file);
            if id == 0 || id < prev_id {
                return Vec::new();
            }

            if id != prev_id {
                if prev_id != 0 && sequence != 0 {
                    discontinue_from = cmp::min(discontinue_from, i);
                    break;
                }

                log_info!(
                    "found new id, previous meta file: {}, current meta file: {}",
                    prev_meta_file,
                    meta_file
                );

                prev_id = id;
                prev_sequence = sequence;
                prev_meta_file = meta_file;
                discontinue_from = meta_files.len();
                continue;
            }

            if sequence != prev_sequence + 1 {
                discontinue_from = cmp::min(discontinue_from, i);
                log_warn!(
                    "sequence discontinuity, previous meta file: {}, current meta file: {}",
                    prev_meta_file,
                    meta_file
                )
            }

            prev_sequence = sequence;
            prev_meta_file = meta_file;
        }

        meta_files[..discontinue_from].to_vec()
    }

    fn parse_meta_file_name(meta_file: &str) -> (u64, u64) {
        if let Some(file_name) = PathBuf::from(meta_file).file_name() {
            if let Some(file_name_str) = file_name.to_str() {
                let parts: Vec<&str> = file_name_str.split('_').collect();
                if parts.len() >= 2 {
                    let id = parts[0].parse::<u64>().unwrap_or_default();
                    let sequence = parts[1].parse::<u64>().unwrap_or_default();
                    return (id, sequence);
                }
            }
        }
        (0, 0)
    }
}
