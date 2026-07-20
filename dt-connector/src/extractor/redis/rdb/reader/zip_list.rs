use std::io::Cursor;

use crate::error_boundary::extractor::redis_rdb as rdb_error;
use anyhow::bail;
use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt};
use dt_common::meta::redis::redis_object::RedisString;

use crate::extractor::redis::StreamReader;

use super::rdb_reader::RdbReader;

const ZIP_STR_06B: u8 = 0x00;
const ZIP_STR_14B: u8 = 0x01;
const ZIP_STR_32B: u8 = 0x02;

const ZIP_INT_04B: u8 = 0x0f;

const ZIP_INT_08B: u8 = 0xfe;
const ZIP_INT_16B: u8 = 0xc0;
const ZIP_INT_24B: u8 = 0xf0;
const ZIP_INT_32B: u8 = 0xd0;
const ZIP_INT_64B: u8 = 0xe0;

impl RdbReader<'_> {
    pub async fn read_zip_list(&mut self) -> anyhow::Result<Vec<RedisString>> {
        // The general layout of the ziplist is as follows:
        // <zlbytes> <zltail> <zllen> <entry> <entry> ... <entry> <zlend>

        let buf = self.read_string().await?;
        let mut reader = Cursor::new(buf.as_bytes());

        let _ = reader.read_u32::<LittleEndian>()?; // zlbytes
        let _ = reader.read_u32::<LittleEndian>()?; // zltail

        let size = reader.read_u16::<LittleEndian>()? as usize;
        let mut elements = Vec::new();
        if size == 65535 {
            // 2^16-1, we need to traverse the entire list to know how many items it holds.
            loop {
                let first_byte = reader.read_u8()?;
                if first_byte == 0xFE {
                    break;
                }
                let ele = Self::read_zip_list_entry(&mut reader, first_byte).await?;
                elements.push(ele);
            }
        } else {
            for _ in 0..size {
                let first_byte = reader.read_u8()?;
                let ele = Self::read_zip_list_entry(&mut reader, first_byte).await?;
                elements.push(ele);
            }

            let last_byte = reader.read_u8()?;
            if last_byte != 0xFF {
                bail! {rdb_error(format!(
                    "invalid zipList lastByte encoding: {}",
                    last_byte
                ))}
            }
        }

        Ok(elements)
    }

    async fn read_zip_list_entry(
        reader: &mut Cursor<&[u8]>,
        first_byte: u8,
    ) -> anyhow::Result<RedisString> {
        // read prevlen
        if first_byte == 0xFE {
            let _prevlen = reader.read_u32::<LittleEndian>()?;
        }

        // read encoding
        let first_byte = reader.read_bytes(1).await?[0];
        let first_2_bits = (first_byte & 0xc0) >> 6; // first 2 bits of encoding
        match first_2_bits {
            ZIP_STR_06B => {
                let length = (first_byte & 0x3f) as usize; // 0x3f = 00111111
                let buf = reader.read_bytes(length).await?;
                return Ok(RedisString::from(buf));
            }

            ZIP_STR_14B => {
                let second_byte = reader.read_u8()?;
                let length = (((first_byte & 0x3f) as u16) << 8) | second_byte as u16;
                let buf = reader.read_bytes(length as usize).await?;
                return Ok(RedisString::from(buf));
            }

            ZIP_STR_32B => {
                let mut buf = reader.read_bytes(4).await?;
                let length = BigEndian::read_u32(&buf);
                buf = reader.read_bytes(length as usize).await?;
                return Ok(RedisString::from(buf));
            }

            _ => {}
        }

        match first_byte {
            ZIP_INT_08B => {
                let v = reader.read_i8()?;
                return Ok(RedisString::from(v.to_string()));
            }

            ZIP_INT_16B => {
                let v = reader.read_i16::<LittleEndian>()?;
                return Ok(RedisString::from(v.to_string()));
            }

            ZIP_INT_24B => {
                let v = reader.read_i24::<LittleEndian>()?;
                return Ok(RedisString::from(v.to_string()));
            }

            ZIP_INT_32B => {
                let v = reader.read_i32::<LittleEndian>()?;
                return Ok(RedisString::from(v.to_string()));
            }

            ZIP_INT_64B => {
                let v = reader.read_i64::<LittleEndian>()?;
                return Ok(RedisString::from(v.to_string()));
            }

            _ => {}
        }

        if first_byte >> 4 == ZIP_INT_04B {
            let v = (first_byte & 0x0f) as i8 - 1;
            if v < 0 || v > 12 {
                bail! {rdb_error(format!(
                    "invalid zipInt04B encoding: {}",
                    v
                ))}
            }
            return Ok(RedisString::from(v.to_string()));
        }

        bail! {rdb_error(format!(
            "invalid encoding: {}",
            first_byte
        ))}
    }
}
