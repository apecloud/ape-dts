use anyhow::bail;

use super::rdb_reader::RdbReader;
use crate::error::extractor::redis_rdb as rdb_error;
use crate::extractor::redis::StreamReader;
use dt_common::meta::redis::redis_object::RedisString;

const RDB_ENC_INT8: u8 = 0;
const RDB_ENC_INT16: u8 = 1;
const RDB_ENC_INT32: u8 = 2;
const RDB_ENC_LZF: u8 = 3;

impl RdbReader<'_> {
    pub async fn read_string(&mut self) -> anyhow::Result<RedisString> {
        let (len, special) = self.read_encoded_length().await?;
        let bytes = if special {
            match len as u8 {
                RDB_ENC_INT8 => self.read_i8().await?.to_string().as_bytes().to_vec(),

                RDB_ENC_INT16 => self.read_i16().await?.to_string().as_bytes().to_vec(),

                RDB_ENC_INT32 => self.read_i32().await?.to_string().as_bytes().to_vec(),

                RDB_ENC_LZF => {
                    let in_len = self.read_length().await?;
                    let out_len = self.read_length().await?;
                    let in_buf = self.read_bytes(in_len as usize).await?;
                    self.lzf_decompress(&in_buf, out_len as usize)?
                }

                _ => {
                    bail! {rdb_error(format!(
                        "Unknown string encode type {}",
                        len
                    ))}
                }
            }
        } else {
            self.read_bytes(len as usize).await?
        };
        Ok(RedisString { bytes })
    }

    fn lzf_decompress(&self, in_buf: &[u8], out_len: usize) -> anyhow::Result<Vec<u8>> {
        let mut out = vec![0u8; out_len];

        let mut i = 0;
        let mut o = 0;
        while i < in_buf.len() {
            let ctrl = in_buf[i] as usize;
            i += 1;
            if ctrl < 32 {
                for _x in 0..=ctrl {
                    out[o] = in_buf[i];
                    i += 1;
                    o += 1;
                }
            } else {
                let mut length = ctrl >> 5;
                if length == 7 {
                    length += in_buf[i] as usize;
                    i += 1;
                }

                let mut ref_ = o - ((ctrl & 0x1f) << 8) - in_buf[i] as usize - 1;
                i += 1;

                for _x in 0..=length + 1 {
                    out[o] = out[ref_];
                    ref_ += 1;
                    o += 1;
                }
            }
        }

        if o != out_len {
            bail! {rdb_error(format!(
                "lzf decompress failed: out_len: {}, o: {}",
                out_len, o
            ))}
        } else {
            Ok(out)
        }
    }
}
