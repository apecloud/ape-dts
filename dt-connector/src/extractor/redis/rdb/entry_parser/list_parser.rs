use anyhow::bail;
use dt_common::error::DtError;
use dt_common::meta::redis::redis_object::{ListObject, RedisString};

use crate::extractor::redis::rdb::reader::rdb_reader::RdbReader;

const QUICKLIST_NODE_CONTAINER_PLAIN: u64 = 1;
const QUICKLIST_NODE_CONTAINER_PACKED: u64 = 2;

pub struct ListParser {}

impl ListParser {
    pub async fn load_from_buffer(
        reader: &mut RdbReader<'_>,
        key: RedisString,
        type_byte: u8,
    ) -> anyhow::Result<ListObject> {
        let mut obj = ListObject::new();
        obj.key = key;

        match type_byte {
            super::RDB_TYPE_LIST => Self::read_list(&mut obj, reader).await?,
            super::RDB_TYPE_LIST_ZIPLIST => obj.elements = reader.read_zip_list().await?,
            super::RDB_TYPE_LIST_QUICKLIST => Self::read_quick_list(&mut obj, reader).await?,
            super::RDB_TYPE_LIST_QUICKLIST_2 => Self::read_quick_list_2(&mut obj, reader).await?,
            _ => {
                bail! {DtError::redis_rdb(format!(
                    "unknown list type {}",
                    type_byte
                ))}
            }
        }
        Ok(obj)
    }

    async fn read_list(obj: &mut ListObject, reader: &mut RdbReader<'_>) -> anyhow::Result<()> {
        let size = reader.read_length().await?;
        for _ in 0..size {
            let ele = reader.read_string().await?;
            obj.elements.push(ele);
        }
        Ok(())
    }

    async fn read_quick_list(
        obj: &mut ListObject,
        reader: &mut RdbReader<'_>,
    ) -> anyhow::Result<()> {
        let size = reader.read_length().await?;
        for _ in 0..size {
            let zip_list_elements = reader.read_zip_list().await?;
            obj.elements.extend(zip_list_elements);
        }
        Ok(())
    }

    async fn read_quick_list_2(
        obj: &mut ListObject,
        reader: &mut RdbReader<'_>,
    ) -> anyhow::Result<()> {
        let size = reader.read_length().await?;

        for _ in 0..size {
            let container = reader.read_length().await?;
            match container {
                QUICKLIST_NODE_CONTAINER_PLAIN => {
                    let ele = reader.read_string().await?;
                    obj.elements.push(ele);
                }

                QUICKLIST_NODE_CONTAINER_PACKED => {
                    let listpack_elements = reader.read_list_pack().await?;
                    obj.elements.extend(listpack_elements);
                }

                _ => {
                    bail! {DtError::redis_rdb(format!(
                        "unknown quicklist container {}",
                        container
                    ))}
                }
            }
        }
        Ok(())
    }
}
