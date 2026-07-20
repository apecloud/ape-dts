use std::io::Cursor;

use crate::error_boundary::extractor::redis_rdb as rdb_error;
use anyhow::bail;
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use dt_common::meta::redis::redis_object::{RedisString, SetObject};

use crate::extractor::redis::{rdb::reader::rdb_reader::RdbReader, StreamReader};

pub struct SetParser {}

impl SetParser {
    pub async fn load_from_buffer(
        reader: &mut RdbReader<'_>,
        key: RedisString,
        type_byte: u8,
    ) -> anyhow::Result<SetObject> {
        let mut obj = SetObject::new();
        obj.key = key;

        match type_byte {
            super::RDB_TYPE_SET => Self::read_str_set(&mut obj, reader).await?,
            super::RDB_TYPE_SET_INTSET => Self::read_int_set(&mut obj, reader).await?,
            super::RDB_TYPE_SET_LISTPACK => obj.elements = reader.read_list_pack().await?,
            _ => {
                bail! {rdb_error(format!(
                    "unknown set type. type_byte=[{}]",
                    type_byte
                ))}
            }
        }
        Ok(obj)
    }

    pub async fn read_str_set(
        obj: &mut SetObject,
        reader: &mut RdbReader<'_>,
    ) -> anyhow::Result<()> {
        let size = reader.read_length().await? as usize;
        for _i in 0..size {
            obj.elements.push(reader.read_string().await?);
        }
        Ok(())
    }

    pub async fn read_int_set(
        obj: &mut SetObject,
        reader: &mut RdbReader<'_>,
    ) -> anyhow::Result<()> {
        let buf = reader.read_string().await?;
        let mut reader = Cursor::new(buf.as_bytes());

        let encoding_type = reader.read_u32::<LittleEndian>()? as usize;
        let size = reader.read_u32::<LittleEndian>()?;
        for _ in 0..size {
            let buf = reader.read_bytes(encoding_type).await?;
            let int_str = match encoding_type {
                2 => LittleEndian::read_i16(&buf).to_string(),
                4 => LittleEndian::read_i32(&buf).to_string(),
                8 => LittleEndian::read_i64(&buf).to_string(),
                _ => {
                    bail! {rdb_error(format!(
                        "unknown int encoding type: {:x}",
                        encoding_type
                    ))}
                }
            };
            obj.elements.push(RedisString::from(int_str));
        }
        Ok(())
    }
}
