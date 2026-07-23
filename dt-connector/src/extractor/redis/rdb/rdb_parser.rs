use anyhow::{bail, Context};
use sqlx::types::chrono;

use super::{entry_parser::entry_parser::EntryParser, reader::rdb_reader::RdbReader};
use crate::extractor::redis::{rdb::entry_parser::module2_parser::ModuleParser, StreamReader};
use dt_common::meta::redis::{redis_entry::RedisEntry, redis_object::RedisCmd};
use dt_common::{error::DtError, log_debug, log_info};

const K_FLAG_SLOT_INFO: u8 = 0xf4; // (244) (Redis 7.4+) RDB_OPCODE_SLOT_INFO: slot info
const _K_FLAG_FUNCTION2: u8 = 0xf5; // (245) function library data
const _K_FLAG_FUNCTION: u8 = 0xf6; // (246) old function library data for 7.0 rc1 and rc2
const K_FLAG_MODULE_AUX: u8 = 0xf7; // (247) Module auxiliary data.
const K_FLAG_IDLE: u8 = 0xf8; // (248) LRU idle time.
const K_FLAG_FREQ: u8 = 0xf9; // (249) LFU frequency.
const K_FLAG_AUX: u8 = 0xfa; // (250) RDB aux field.
const K_FLAG_RESIZE_DB: u8 = 0xfb; // (251) Hash table resize hint.
const K_FLAG_EXPIRE_MS: u8 = 0xfc; // (252) Expire time in milliseconds.
const K_FLAG_EXPIRE: u8 = 0xfd; // (253) Old expire time in seconds.
const K_FLAG_SELECT: u8 = 0xfe; // (254) DB number of the following keys.
const K_EOF: u8 = 0xff; // (255) End of the RDB file.

const RDB_MODULE_OPCODE_EOF: u64 = 0; // End of module value.
const RDB_MODULE_OPCODE_SINT: u64 = 1; // Signed integer.
const RDB_MODULE_OPCODE_UINT: u64 = 2; // Unsigned integer.
const RDB_MODULE_OPCODE_FLOAT: u64 = 3; // Float.
const RDB_MODULE_OPCODE_DOUBLE: u64 = 4; // Double.
const RDB_MODULE_OPCODE_STRING: u64 = 5; // String.

pub struct RdbParser<'a> {
    pub reader: RdbReader<'a>,
    pub repl_stream_db_id: i64,
    pub now_db_id: i64,
    pub expire_ms: i64,
    pub idle: i64,
    pub freq: i64,

    pub is_end: bool,
}

impl RdbParser<'_> {
    pub async fn load_meta(&mut self) -> anyhow::Result<String> {
        // magic
        let mut buf = self.reader.read_bytes(5).await?;
        let magic = String::from_utf8(buf)
            .context(DtError::redis_rdb("Redis RDB header is not valid UTF-8"))?;
        if magic != "REDIS" {
            bail! {DtError::redis_rdb("invalid rdb format")}
        }

        // version
        buf = self.reader.read_bytes(4).await?;
        let version = String::from_utf8(buf)?;
        Ok(version)
    }

    pub async fn load_entry(&mut self) -> anyhow::Result<Option<RedisEntry>> {
        let type_byte = self.reader.read_byte().await?;
        log_debug!("rdb type_byte: {}", type_byte);

        match type_byte {
            K_FLAG_SLOT_INFO => {
                self.reader.read_length().await?; // slot id
                self.reader.read_length().await?; // slot size
                self.reader.read_length().await?; // expires slot size
            }
            K_FLAG_MODULE_AUX => {
                let module_id = self.reader.read_length().await?; // module id
                let module_name = ModuleParser::module_type_name_by_id(module_id);
                log_info!(
                    "RDB module aux: module_id=[{}], module_name=[{}]",
                    module_id,
                    module_name
                );
                // refer: https://github.com/redis/redis/blob/unstable/src/rdb.c#L3183
                let _when_opcode = self.reader.read_length().await?;
                let _when = self.reader.read_length().await?;
                let mut opcode = self.reader.read_length().await?;
                while opcode != RDB_MODULE_OPCODE_EOF {
                    match opcode {
                        RDB_MODULE_OPCODE_SINT | RDB_MODULE_OPCODE_UINT => {
                            self.reader.read_length().await?;
                        }
                        RDB_MODULE_OPCODE_FLOAT => {
                            self.reader.read_float().await?;
                        }
                        RDB_MODULE_OPCODE_DOUBLE => {
                            self.reader.read_double().await?;
                        }
                        RDB_MODULE_OPCODE_STRING => {
                            self.reader.read_string().await?;
                        }
                        _ => {
                            bail! {DtError::redis_rdb(format!(
                                "module aux opcode not found. module_name=[{}], opcode=[{}]",
                                module_name, opcode
                            ))}
                        }
                    }
                    opcode = self.reader.read_length().await?;
                }
            }

            K_FLAG_IDLE => {
                // OBJECT IDELTIME NOT captured in rdb snapshot
                self.idle = self.reader.read_length().await? as i64;
            }

            K_FLAG_FREQ => {
                // OBJECT FREQ NOT captured in rdb snapshot
                self.freq = self.reader.read_u8().await? as i64;
            }

            K_FLAG_AUX => {
                let key = String::try_from(self.reader.read_string().await?).context(
                    DtError::redis_rdb("Redis RDB metadata key is not valid UTF-8"),
                )?;
                let value = self.reader.read_string().await?;
                match key.as_str() {
                    "repl-stream-db" => {
                        let value = String::try_from(value).context(DtError::redis_rdb(
                            "Redis RDB replication database value is not valid UTF-8",
                        ))?;
                        self.repl_stream_db_id = value
                            .parse::<i64>()
                            .context(DtError::redis_rdb("replication database value is invalid"))?;
                        log_info!("RDB repl-stream-db: {}", self.repl_stream_db_id);
                    }

                    "lua" => {
                        let mut cmd = RedisCmd::new();
                        cmd.add_str_arg("script");
                        cmd.add_str_arg("load");
                        cmd.add_redis_arg(&value);
                        log_info!("LUA script: {:?}", value);

                        let mut entry = RedisEntry::new();
                        entry.is_base = true;
                        entry.db_id = self.now_db_id;
                        entry.cmd = cmd;
                        return Ok(Some(entry));
                    }

                    _ => {
                        log_info!("RDB AUX fields. key=[{}], value=[{:?}]", key, value);
                    }
                }
            }

            K_FLAG_RESIZE_DB => {
                let db_size = self.reader.read_length().await?;
                let expire_size = self.reader.read_length().await?;
                log_info!(
                    "RDB resize db. db_size=[{}], expire_size=[{}]",
                    db_size,
                    expire_size
                )
            }

            K_FLAG_EXPIRE_MS => {
                let mut expire_ms = self.reader.read_u64().await? as i64;
                expire_ms -= chrono::Utc::now().timestamp_millis();
                if expire_ms < 0 {
                    expire_ms = 1
                }
                self.expire_ms = expire_ms;
            }

            K_FLAG_EXPIRE => {
                let mut expire_ms = self.reader.read_u32().await? as i64 * 1000;
                expire_ms -= chrono::Utc::now().timestamp_millis();
                if expire_ms < 0 {
                    expire_ms = 1
                }
                self.expire_ms = expire_ms;
            }

            K_FLAG_SELECT => {
                self.now_db_id = self.reader.read_length().await? as i64;
            }

            K_EOF => {
                self.is_end = true;
                self.reader
                    .read_bytes(self.reader.rdb_length - self.reader.position)
                    .await?;
            }

            _ => {
                let key = self.reader.read_string().await?;
                self.reader.copy_raw = true;
                let value =
                    EntryParser::parse_object(&mut self.reader, type_byte, key.clone()).await;
                self.reader.copy_raw = false;

                let value = value.context(DtError::redis_rdb("failed to parse Redis RDB entry"))?;
                let mut entry = RedisEntry::new();
                entry.is_base = true;
                entry.db_id = self.now_db_id;
                entry.raw_bytes = self.reader.drain_raw_bytes();
                entry.key = key;
                entry.value = value;
                entry.value_type_byte = type_byte;
                entry.expire_ms = self.expire_ms;
                // reset expire_ms
                self.expire_ms = 0;
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }
}
