use anyhow::bail;

use crate::error_boundary::extractor_error::rdb_error;
use crate::extractor::redis::rdb::reader::rdb_reader::RdbReader;
use dt_common::meta::redis::redis_object::{HashObject, RedisString};

pub struct HashParser {}

impl HashParser {
    pub async fn load_from_buffer(
        reader: &mut RdbReader<'_>,
        key: RedisString,
        type_byte: u8,
    ) -> anyhow::Result<HashObject> {
        let mut obj = HashObject::new();
        obj.key = key;

        match type_byte {
            super::RDB_TYPE_HASH => Self::read_hash(&mut obj, reader).await?,
            super::RDB_TYPE_HASH_ZIPMAP => Self::read_hash_zip_map(&mut obj, reader).await?,
            super::RDB_TYPE_HASH_ZIPLIST => Self::read_hash_zip_list(&mut obj, reader).await?,
            super::RDB_TYPE_HASH_LISTPACK => Self::read_hash_list_pack(&mut obj, reader).await?,
            super::RDB_TYPE_HASH_METADATA_PRE_GA => {
                Self::read_hash_ttl(&mut obj, reader, true).await?
            }
            super::RDB_TYPE_HASH_METADATA => Self::read_hash_ttl(&mut obj, reader, false).await?,
            super::RDB_TYPE_HASH_LISTPACK_EX_PRE_GA => {
                Self::read_hash_list_pack_ttl(&mut obj, reader, true).await?
            }
            super::RDB_TYPE_HASH_LISTPACK_EX => {
                Self::read_hash_list_pack_ttl(&mut obj, reader, false).await?
            }
            _ => {
                bail! {rdb_error(format!(
                    "unknown hash type. type_byte=[{}]",
                    type_byte
                ))}
            }
        }
        Ok(obj)
    }

    async fn read_hash(obj: &mut HashObject, reader: &mut RdbReader<'_>) -> anyhow::Result<()> {
        let size = reader.read_length().await?;
        for _ in 0..size {
            let key = reader.read_string().await?;
            let value = reader.read_string().await?;
            obj.value.insert(key, (value, None));
        }
        Ok(())
    }

    async fn read_hash_zip_map(
        _obj: &mut HashObject,
        _reader: &mut RdbReader<'_>,
    ) -> anyhow::Result<()> {
        bail! {rdb_error("not implemented rdb_type_zip_map")}
    }

    async fn read_hash_zip_list(
        obj: &mut HashObject,
        reader: &mut RdbReader<'_>,
    ) -> anyhow::Result<()> {
        let list = reader.read_zip_list().await?;
        let size = list.len();
        for i in (0..size).step_by(2) {
            let key = list[i].clone();
            let value = list[i + 1].clone();
            obj.value.insert(key, (value, None));
        }
        Ok(())
    }

    async fn read_hash_list_pack(
        obj: &mut HashObject,
        reader: &mut RdbReader<'_>,
    ) -> anyhow::Result<()> {
        let list = reader.read_list_pack().await?;
        let size = list.len();
        for i in (0..size).step_by(2) {
            let key = list[i].clone();
            let value = list[i + 1].clone();
            obj.value.insert(key, (value, None));
        }
        Ok(())
    }

    async fn read_hash_ttl(
        obj: &mut HashObject,
        reader: &mut RdbReader<'_>,
        is_pre: bool,
    ) -> anyhow::Result<()> {
        let min_expire: i64 = if is_pre {
            reader.read_u64().await? as i64
        } else {
            0
        };
        let size = reader.read_length().await?;
        for _ in 0..size {
            let mut expire: i64 = reader.read_length().await? as i64; // 这里报错：illegal length special=true
            if !is_pre && expire != 0 {
                expire += min_expire - 1
            }
            let key = reader.read_string().await?;
            let value = reader.read_string().await?;
            obj.value.insert(
                key,
                (
                    value,
                    Some(RedisString::from(expire.to_string().as_bytes().to_vec())),
                ),
            );
        }
        Ok(())
    }

    async fn read_hash_list_pack_ttl(
        obj: &mut HashObject,
        reader: &mut RdbReader<'_>,
        is_pre: bool,
    ) -> anyhow::Result<()> {
        if !is_pre {
            // read minExpire
            reader.read_u64().await?;
        }
        let list = reader.read_list_pack().await?;
        let size = list.len();
        for i in (0..size).step_by(3) {
            let key = list[i].clone();
            let value = list[i + 1].clone();
            let expire = list[i + 2].as_bytes();
            obj.value
                .insert(key, (value, Some(RedisString::from(expire.to_vec()))));
        }
        Ok(())
    }
}
