use dt_common::error::Error;
use dt_meta::redis::redis_object::{RedisString, StringObject};

use crate::extractor::redis::rdb::reader::rdb_reader::RdbReader;

pub struct StringLoader {}

impl StringLoader {
    pub fn load_from_buffer(
        reader: &mut RdbReader,
        key: RedisString,
        _type_byte: u8,
    ) -> Result<StringObject, Error> {
        let mut obj = StringObject::new();
        obj.key = key;
        obj.value = reader.read_string()?;
        Ok(obj)
    }
}