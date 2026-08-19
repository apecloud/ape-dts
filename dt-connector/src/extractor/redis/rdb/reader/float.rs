use byteorder::{ByteOrder, LittleEndian};

use super::rdb_reader::RdbReader;
use crate::extractor::redis::StreamReader;

impl RdbReader<'_> {
    pub async fn read_float(&mut self) -> anyhow::Result<f64> {
        let n = self.read_u8().await?;
        let v = match n {
            253 => f64::NAN,
            254 => f64::INFINITY,
            255 => f64::NEG_INFINITY,
            _ => {
                let buf = self.read_bytes(n as usize).await?;
                let s = String::from_utf8(buf)?;
                let v: f64 = s.parse()?;
                v
            }
        };
        Ok(v)
    }

    pub async fn read_double(&mut self) -> anyhow::Result<f64> {
        let buf = self.read_bytes(8).await?;
        Ok(LittleEndian::read_f64(&buf))
    }
}
