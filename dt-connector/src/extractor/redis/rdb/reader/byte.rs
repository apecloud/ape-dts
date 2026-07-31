use crate::extractor::redis::StreamReader;

use super::rdb_reader::RdbReader;

impl RdbReader<'_> {
    pub async fn read_byte(&mut self) -> anyhow::Result<u8> {
        let buf = self.read_bytes(1).await?;
        Ok(buf[0])
    }
}
