use async_trait::async_trait;
use std::io::{Cursor, Read};

pub mod rdb;
pub mod redis_client;
pub mod redis_cluster_psync_extractor;
pub mod redis_psync_extractor;
pub mod redis_reshard_extractor;
pub mod redis_resp_reader;
pub mod redis_resp_types;
pub mod redis_scan_extractor;
pub mod redis_snapshot_file_extractor;

#[async_trait]
pub trait StreamReader {
    async fn read_bytes(&mut self, size: usize) -> anyhow::Result<Vec<u8>>;
}

#[async_trait]
impl StreamReader for Cursor<&[u8]> {
    async fn read_bytes(&mut self, size: usize) -> anyhow::Result<Vec<u8>> {
        let mut buf = vec![0; size];
        self.read_exact(&mut buf)?;
        Ok(buf)
    }
}
