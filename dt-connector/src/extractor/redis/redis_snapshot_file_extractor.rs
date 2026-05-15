use async_trait::async_trait;
use tokio::{fs::metadata, fs::File, io::AsyncReadExt};

use super::StreamReader;
use crate::extractor::base_extractor::{BaseExtractor, ExtractState};
use crate::extractor::redis::rdb::rdb_parser::RdbParser;
use crate::extractor::redis::rdb::reader::rdb_reader::RdbReader;
use crate::extractor::redis::redis_psync_extractor::RedisPsyncExtractor;
use crate::Extractor;
use dt_common::log_info;
use dt_common::meta::position::Position;
use dt_common::rdb_filter::RdbFilter;

pub struct RedisSnapshotFileExtractor {
    pub file_path: String,
    pub filter: RdbFilter,
    pub base_extractor: BaseExtractor,
    pub extract_state: ExtractState,
}

struct RdbFileReader {
    pub file: File,
}

#[async_trait]
impl Extractor for RedisSnapshotFileExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        let file = File::open(&self.file_path)
            .await
            .expect("rdb file not found");
        let metadata = metadata(&self.file_path)
            .await
            .expect("rdb file with wrong meta");
        let mut file_reader = RdbFileReader { file };
        let mut stream_reader: Box<&mut (dyn StreamReader + Send)> = Box::new(&mut file_reader);

        let reader = RdbReader {
            conn: &mut stream_reader,
            rdb_length: metadata.len() as usize,
            position: 0,
            copy_raw: false,
            raw_bytes: Vec::new(),
        };

        let mut parser = RdbParser {
            reader,
            repl_stream_db_id: 0,
            now_db_id: 0,
            expire_ms: 0,
            idle: 0,
            freq: 0,
            is_end: false,
        };

        let version = parser.load_meta().await?;
        log_info!("source redis version: {:?}", version);

        loop {
            if let Some(entry) = parser.load_entry().await? {
                RedisPsyncExtractor::push_to_buf(
                    &self.base_extractor,
                    &mut self.extract_state,
                    &mut self.filter,
                    entry,
                    Position::None,
                )
                .await?;
            }

            if parser.is_end {
                log_info!(
                    "end extracting data from rdb, all count: {}",
                    self.extract_state.monitor.counters.pushed_record_count
                );
                break;
            }
        }
        self.base_extractor
            .wait_task_finish(&mut self.extract_state)
            .await
    }
}

#[async_trait]
impl StreamReader for RdbFileReader {
    async fn read_bytes(&mut self, size: usize) -> anyhow::Result<Vec<u8>> {
        let mut buf = vec![0; size];
        self.file.read_exact(&mut buf).await?;
        Ok(buf)
    }
}
