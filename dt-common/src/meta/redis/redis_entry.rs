use anyhow::Context;

use super::{
    command::key_parser::KeyParser,
    redis_object::{RedisCmd, RedisObject, RedisString},
};

#[derive(Debug, Clone)]
pub struct RedisEntry {
    pub id: u64,
    // whether the command is decoded from dump.rdb file
    pub is_base: bool,
    pub db_id: i64,
    pub timestamp_ms: u64,

    pub expire_ms: i64,
    pub expire_at_ms: Option<i64>,
    pub key: RedisString,
    pub value: RedisObject,
    pub value_type_byte: u8,
    pub raw_bytes: Vec<u8>,

    pub cmd: RedisCmd,
    pub data_size: usize,
    pub slot: i32,
    pub freq: i64,
}

impl RedisEntry {
    pub fn new() -> Self {
        Self {
            id: 0,
            is_base: false,
            db_id: 0,
            timestamp_ms: 0,

            expire_ms: 0,
            expire_at_ms: None,
            key: RedisString::new(),
            value: RedisObject::Unknown,
            raw_bytes: Vec::new(),
            value_type_byte: 0,

            cmd: RedisCmd::new(),
            data_size: 0,
            slot: 0,
            freq: -1,
        }
    }

    pub fn is_raw(&self) -> bool {
        self.is_base && !self.raw_bytes.is_empty()
    }

    pub fn get_data_malloc_size(&self) -> usize {
        if self.data_size > 0 {
            self.data_size
        } else if self.is_raw() {
            self.raw_bytes.len()
        } else {
            self.key.bytes.len() + self.value.get_malloc_size() + self.cmd.get_malloc_size()
        }
    }

    pub fn get_type(&self) -> String {
        self.value.get_type()
    }

    pub fn cal_slots(&mut self, key_parser: &KeyParser) -> anyhow::Result<Vec<u16>> {
        if self.is_base {
            Ok(vec![KeyParser::calc_slot(self.key.as_bytes())])
        } else {
            if self.cmd.keys.is_empty() {
                self.cmd
                    .parse_keys(key_parser)
                    .with_context(|| format!("failed to parse keys for cmd: [{}]", self.cmd))?;
            }

            let mut slots = Vec::new();
            for key in self.cmd.keys.iter() {
                slots.push(KeyParser::calc_slot(key.as_bytes()))
            }
            Ok(slots)
        }
    }
}
