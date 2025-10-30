use std::{cmp, path::PathBuf, str::FromStr, sync::Arc};

use anyhow::Context;
use async_trait::async_trait;
use dt_common::{
    config::s3_config::S3Config,
    log_debug, log_info, log_warn,
    meta::{dt_data::DtData, foxlake::s3_file_meta::S3FileMeta, position::Position},
    utils::time_util::TimeUtil,
};
use opendal::options::ListOptions;
use opendal::Operator;
use tokio::io::AsyncReadExt;

use crate::{
    extractor::{base_extractor::BaseExtractor, resumer::recovery::Recovery},
    Extractor,
};

pub struct FoxlakeS3Extractor {
    pub s3_config: S3Config,
    pub s3_client: Operator,
    pub base_extractor: BaseExtractor,
    pub batch_size: usize,
    pub schema: String,
    pub tb: String,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

const FINISHED: &str = "finished";
const WAIT_FILE_SECS: u64 = 5;

#[async_trait]
impl Extractor for FoxlakeS3Extractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        log_info!(
            "FoxlakeS3Extractor starts, schema: `{}`, tb: `{}`, batch_size: {}",
            self.schema,
            self.tb,
            self.batch_size
        );
        self.extract_internal().await?;
        self.base_extractor.wait_task_finish().await
    }
}

impl FoxlakeS3Extractor {
    async fn extract_internal(&mut self) -> anyhow::Result<()> {
        let mut start_after = if let Some(handler) = &self.recovery {
            if let Some(value) = handler
                .get_snapshot_resume_position(&self.schema, &self.tb, "", false)
                .await
            {
                log_info!(
                    "[{}.{}] recovery from [{}]:[{}]",
                    self.schema,
                    self.tb,
                    "",
                    value
                );
                Some(value)
            } else {
                None
            }
        } else {
            None
        };
        loop {
            let mut finished = false;
            let meta_files = self.list_meta_file(&start_after).await?;

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

            for file in continuous_meta_files.iter() {
                if file.ends_with(FINISHED) {
                    finished = true;
                    break;
                }
                let file_meta = self.get_meta(file).await?;
                let dt_data = DtData::Foxlake { file_meta };
                let position = Position::FoxlakeS3 {
                    schema: self.schema.clone(),
                    tb: self.tb.clone(),
                    s3_meta_file: file.to_owned(),
                };
                self.base_extractor.push_dt_data(dt_data, position).await?;
            }

            if finished {
                let dt_data = DtData::Foxlake {
                    file_meta: S3FileMeta {
                        push_epoch: i64::MAX,
                        ..Default::default()
                    },
                };
                self.base_extractor
                    .push_dt_data(dt_data, Position::None)
                    .await?;
                break;
            }

            // set start_after if only continuous_meta_files is NOT empty
            start_after = continuous_meta_files.last().map(|s: &String| s.to_string());
        }
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

    async fn list_meta_file(&self, start_after: &Option<String>) -> anyhow::Result<Vec<String>> {
        let mut prefix = format!("{}/{}/meta/", self.schema, self.tb);
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
            // finished file
            if i == meta_files.len() - 1 && meta_file.ends_with(FINISHED) {
                continue;
            }

            let (id, sequence) = Self::parse_meta_file_name(meta_file);
            // should never happen
            if id == 0 || id < prev_id {
                return Vec::new();
            }

            if id != prev_id {
                // This is the first file pushed by the same id, which means the pusher progress has restarted.
                // Abnormal exit of the previous pusher may lead to the discontinuity of the file sequence.
                // Ignore the discontinuity of previous files since they were pushed by previous pusher.

                // the first sequence of the new pusher should be 0
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
                // reset when a new pusher found
                discontinue_from = meta_files.len();
                continue;
            }

            // discontinuity is caused by multiple threads pushing orc files in pusher progress.
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

#[cfg(test)]
mod tests {
    use super::FoxlakeS3Extractor;

    #[test]
    fn test_parse_meta_file_name() {
        let meta_file =
            "data/meta/1721796946_0000000012_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc";
        let (id, sequence) = FoxlakeS3Extractor::parse_meta_file_name(meta_file);
        assert_eq!(id, 1721796946);
        assert_eq!(sequence, 12);
    }

    #[test]
    fn test_check_continuity() {
        let check_continuous_files_count =
            |meta_files: Vec<&str>, start_after: &Option<String>, expect_count: usize| {
                let meta_files: Vec<String> = meta_files.iter().map(|s| s.to_string()).collect();
                let continuous_files =
                    FoxlakeS3Extractor::find_continuous_files(&meta_files, start_after);
                assert_eq!(continuous_files.len(), expect_count);
            };

        // start_after = None
        // case 1
        let meta_files = vec![
            "data/meta/1721796946_0000000000_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000001_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        check_continuous_files_count(meta_files, &None, 2);

        // case 2
        let meta_files = vec![
            "data/meta/1721796946_0000000002_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000003_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        check_continuous_files_count(meta_files, &None, 2);

        // case 3
        let meta_files = vec![
            "data/meta/1721796946_0000000002_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000004_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        check_continuous_files_count(meta_files, &None, 1);

        // start_after != None
        // case 1
        let start_after = Some(
            "data/meta/1721796946_0000000010_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc"
                .to_string(),
        );
        let meta_files = vec![
            "data/meta/1721796946_0000000011_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000012_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        check_continuous_files_count(meta_files, &start_after, 2);

        // case 2
        let meta_files = vec![
            "data/meta/1721796946_0000000012_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000013_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        check_continuous_files_count(meta_files, &start_after, 0);

        // case 3
        let meta_files = vec![
            "data/meta/1721796946_0000000011_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000013_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        check_continuous_files_count(meta_files, &start_after, 1);

        // case 4
        let meta_files = vec![
            "data/meta/1721796946_0000000011_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000012_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000014_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000015_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        check_continuous_files_count(meta_files, &start_after, 2);

        // case 5, new sequencer_id 1721800418 found and it's first sequence is 0000000000
        let meta_files = vec![
            "data/meta/1721796946_0000000011_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000013_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000000_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000001_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        check_continuous_files_count(meta_files, &start_after, 4);

        // case 6, new sequencer_id 1721800418 found but it's first sequence is 0000000001
        let meta_files = vec![
            "data/meta/1721796946_0000000011_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000013_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000001_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000002_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        check_continuous_files_count(meta_files, &start_after, 1);

        // case 7
        let meta_files = vec![
            "data/meta/1721796946_0000000011_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000012_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000001_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000002_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        check_continuous_files_count(meta_files, &start_after, 2);

        // case 8
        let meta_files = vec![
            "data/meta/1721796946_0000000011_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000012_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000000_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000001_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000004_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        check_continuous_files_count(meta_files, &start_after, 4);

        // case 9
        let meta_files = vec![
            "data/meta/1721796946_0000000011_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000012_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000000_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000001_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000004_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800555_0000000000_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        check_continuous_files_count(meta_files, &start_after, 6);

        // case 10
        let meta_files = vec![
            "data/meta/1721796946_0000000011_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000012_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000000_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000001_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000004_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800555_0000000000_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800555_0000000002_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        check_continuous_files_count(meta_files, &start_after, 6);

        // case 11
        let meta_files = vec![
            "data/meta/1721800418_0000000000_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        check_continuous_files_count(meta_files, &start_after, 1);

        // case 12
        let meta_files = vec![
            "data/meta/1721800418_0000000001_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        check_continuous_files_count(meta_files, &start_after, 0);

        // finished
        let meta_files = vec![
            "data/meta/1721796946_0000000011_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000012_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/finished",
        ];
        check_continuous_files_count(meta_files, &start_after, 3);

        // empty
        check_continuous_files_count(Vec::new(), &start_after, 0);
    }
}
