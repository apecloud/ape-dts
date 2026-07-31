use anyhow::bail;
use dt_common::error::DtError;
use dt_common::meta::redis::redis_object::{RedisString, ZSetEntry, ZsetObject};

use crate::extractor::redis::rdb::reader::rdb_reader::RdbReader;

pub struct ZsetParser {}

impl ZsetParser {
    pub async fn load_from_buffer(
        reader: &mut RdbReader<'_>,
        key: RedisString,
        type_byte: u8,
    ) -> anyhow::Result<ZsetObject> {
        let mut obj = ZsetObject::new();
        obj.key = key;

        match type_byte {
            super::RDB_TYPE_ZSET => Self::read_zset(&mut obj, reader, false).await?,
            super::RDB_TYPE_ZSET_2 => Self::read_zset(&mut obj, reader, true).await?,
            super::RDB_TYPE_ZSET_ZIPLIST => Self::read_zset_zip_list(&mut obj, reader).await?,
            super::RDB_TYPE_ZSET_LISTPACK => Self::read_zset_list_pack(&mut obj, reader).await?,
            _ => {
                bail! {DtError::redis_rdb(format!(
                    "unknown zset type. type_byte=[{}]",
                    type_byte
                ))}
            }
        }
        Ok(obj)
    }

    async fn read_zset(
        obj: &mut ZsetObject,
        reader: &mut RdbReader<'_>,
        is_zset_2: bool,
    ) -> anyhow::Result<()> {
        let size = reader.read_length().await?;
        for _ in 0..size {
            let member = reader.read_string().await?;
            let score = if is_zset_2 {
                reader.read_double().await?.to_string()
            } else {
                reader.read_float().await?.to_string()
            };

            let entry = ZSetEntry {
                member,
                score: RedisString::from(score),
            };
            obj.elements.push(entry);
        }
        Ok(())
    }

    async fn read_zset_zip_list(
        obj: &mut ZsetObject,
        reader: &mut RdbReader<'_>,
    ) -> anyhow::Result<()> {
        let list = reader.read_zip_list().await?;
        Self::parse_zset_result(obj, list)
    }

    async fn read_zset_list_pack(
        obj: &mut ZsetObject,
        reader: &mut RdbReader<'_>,
    ) -> anyhow::Result<()> {
        let list = reader.read_list_pack().await?;
        Self::parse_zset_result(obj, list)
    }

    fn parse_zset_result(obj: &mut ZsetObject, list: Vec<RedisString>) -> anyhow::Result<()> {
        let size = list.len();
        if size % 2 != 0 {
            bail! {DtError::redis_rdb(format!(
                "zset list pack size is not even. size=[{}]",
                size
            ))}
        }

        for i in (0..size).step_by(2) {
            let member = list[i].clone();
            let score = list[i + 1].clone();
            obj.elements.push(ZSetEntry { member, score });
        }
        Ok(())
    }
}
