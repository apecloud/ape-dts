use dt_common::meta::redis::redis_object::{RedisObject, RedisString};

use crate::extractor::redis::rdb::reader::rdb_reader::RdbReader;

use super::{
    hash_parser::HashParser, list_parser::ListParser, module2_parser::ModuleParser,
    set_parser::SetParser, stream_parser::StreamParser, string_parser::StringParser,
    zset_parser::ZsetParser,
};

pub struct EntryParser {}

impl EntryParser {
    pub async fn parse_object(
        reader: &mut RdbReader<'_>,
        type_byte: u8,
        key: RedisString,
    ) -> anyhow::Result<RedisObject> {
        let obj = match type_byte {
            super::RDB_TYPE_STRING => {
                RedisObject::String(StringParser::load_from_buffer(reader, key, type_byte).await?)
            }

            super::RDB_TYPE_LIST
            | super::RDB_TYPE_LIST_ZIPLIST
            | super::RDB_TYPE_LIST_QUICKLIST
            | super::RDB_TYPE_LIST_QUICKLIST_2 => {
                RedisObject::List(ListParser::load_from_buffer(reader, key, type_byte).await?)
            }

            super::RDB_TYPE_SET | super::RDB_TYPE_SET_INTSET | super::RDB_TYPE_SET_LISTPACK => {
                RedisObject::Set(SetParser::load_from_buffer(reader, key, type_byte).await?)
            }

            super::RDB_TYPE_ZSET
            | super::RDB_TYPE_ZSET_2
            | super::RDB_TYPE_ZSET_ZIPLIST
            | super::RDB_TYPE_ZSET_LISTPACK => {
                RedisObject::Zset(ZsetParser::load_from_buffer(reader, key, type_byte).await?)
            }

            super::RDB_TYPE_HASH
            | super::RDB_TYPE_HASH_ZIPMAP
            | super::RDB_TYPE_HASH_ZIPLIST
            | super::RDB_TYPE_HASH_LISTPACK
            | super::RDB_TYPE_HASH_METADATA_PRE_GA
            | super::RDB_TYPE_HASH_LISTPACK_EX_PRE_GA
            | super::RDB_TYPE_HASH_METADATA
            | super::RDB_TYPE_HASH_LISTPACK_EX => {
                RedisObject::Hash(HashParser::load_from_buffer(reader, key, type_byte).await?)
            }

            super::RDB_TYPE_STREAM_LISTPACKS
            | super::RDB_TYPE_STREAM_LISTPACKS_2
            | super::RDB_TYPE_STREAM_LISTPACKS_3 => {
                RedisObject::Stream(StreamParser::load_from_buffer(reader, key, type_byte).await?)
            }

            super::RDB_TYPE_MODULE | super::RDB_TYPE_MODULE_2 => {
                RedisObject::Module(ModuleParser::load_from_buffer(reader, key, type_byte).await?)
            }

            _ => {
                log::error!("unknown type byte: {}, key: {}", type_byte, key);
                RedisObject::Unknown
            }
        };

        Ok(obj)
    }
}
