use crate::error_boundary::extractor_error::rdb_error;
use anyhow::bail;
use dt_common::log_info;
use dt_common::meta::redis::redis_object::{ModuleObject, RedisString};

use crate::extractor::redis::rdb::reader::rdb_reader::RdbReader;

pub struct ModuleParser {}

const MODULE_TYPE_NAME_CHAR_SET: &str =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

impl ModuleParser {
    pub async fn load_from_buffer(
        reader: &mut RdbReader<'_>,
        key: RedisString,
        type_byte: u8,
    ) -> anyhow::Result<ModuleObject> {
        if type_byte == super::RDB_TYPE_MODULE {
            bail! {rdb_error(format!(
                "module type with version 1 is not supported, key=[{}]",
                key
            ))}
        }

        let module_id = reader.read_length().await?;
        let module_name = Self::module_type_name_by_id(module_id);

        log_info!("load module2 type: [{}] with raw", module_name);
        Self::skip_module_data(reader).await?;

        Ok(ModuleObject::new())
    }

    pub fn module_type_name_by_id(module_id: u64) -> String {
        let mut name_list: Vec<u8> = vec![0; 9];
        let mut module_id = module_id >> 10;
        let name_char_set = MODULE_TYPE_NAME_CHAR_SET.chars().collect::<Vec<char>>();

        for i in (0..9).rev() {
            name_list[i] = name_char_set[(module_id & 63) as usize] as u8;
            module_id >>= 6;
        }
        String::from_utf8_lossy(&name_list).into_owned()
    }

    async fn skip_module_data(reader: &mut RdbReader<'_>) -> anyhow::Result<()> {
        let mut opcode = reader.read_length().await?;
        while opcode != 0 {
            match opcode {
                1 | 2 => {
                    reader.read_length().await?;
                }
                3 => {
                    reader.read_float().await?;
                }
                4 => {
                    reader.read_double().await?;
                }
                5 => {
                    reader.read_string().await?;
                }
                _ => {
                    bail! {rdb_error(format!(
                        "unknown module opcode: {}", opcode
                    ))}
                }
            }
            opcode = reader.read_length().await?;
        }
        Ok(())
    }
}
