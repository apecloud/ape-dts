use std::io::Cursor;

use anyhow::bail;
use byteorder::{LittleEndian, ReadBytesExt};
use dt_common::error::DtError;
use dt_common::meta::redis::redis_object::RedisString;

use crate::extractor::redis::StreamReader;

use super::rdb_reader::RdbReader;

const LP_ENCODING_7BIT_UINT_MASK: u8 = 0x80; // 10000000
const LP_ENCODING_7BIT_UINT: u8 = 0x00; // 00000000

const LP_ENCODING_6BIT_STR_MASK: u8 = 0xC0; // 11000000
const LP_ENCODING_6BIT_STR: u8 = 0x80; // 10000000

const LP_ENCODING_13BIT_INT_MASK: u8 = 0xE0; // 11100000
const LP_ENCODING_13BIT_INT: u8 = 0xC0; // 11000000

const LP_ENCODING_12BIT_STR_MASK: u8 = 0xF0; // 11110000
const LP_ENCODING_12BIT_STR: u8 = 0xE0; // 11100000

const LP_ENCODING_16BIT_INT_MASK: u8 = 0xFF; // 11111111
const LP_ENCODING_16BIT_INT: u8 = 0xF1; // 11110001

const LP_ENCODING_24BIT_INT_MASK: u8 = 0xFF; // 11111111
const LP_ENCODING_24BIT_INT: u8 = 0xF2; // 11110010

const LP_ENCODING_32BIT_INT_MASK: u8 = 0xFF; // 11111111
const LP_ENCODING_32BIT_INT: u8 = 0xF3; // 11110011

const LP_ENCODING_64BIT_INT_MASK: u8 = 0xFF; // 11111111
const LP_ENCODING_64BIT_INT: u8 = 0xF4; // 11110100

const LP_ENCODING_32BIT_STR_MASK: u8 = 0xFF; // 11111111
const LP_ENCODING_32BIT_STR: u8 = 0xF0; // 11110000

impl RdbReader<'_> {
    pub async fn read_list_pack(&mut self) -> anyhow::Result<Vec<RedisString>> {
        let buf = self.read_string().await?;
        let mut reader = Cursor::new(buf.as_bytes());

        let _all_bytes = reader.read_u32::<LittleEndian>()?; // discard the number of bytes
        let size = reader.read_u16::<LittleEndian>()?;

        let mut elements = Vec::new();
        for _ in 0..size {
            let ele = Self::read_listpack_entry(&mut reader).await?;
            elements.push(ele);
        }

        let last_byte = reader.read_u8()?;
        if last_byte != 0xFF {
            bail! {DtError::redis_rdb("read_listpack: last byte is not 0xFF")};
        }
        Ok(elements)
    }

    // https://github.com/redis/redis/blob/unstable/src/listpack.c lpGetWithSize
    async fn read_listpack_entry(reader: &mut Cursor<&[u8]>) -> anyhow::Result<RedisString> {
        let mut val: i64;
        let mut uval: u64;
        let negstart: u64;
        let negmax: u64;

        let first_byte = reader.read_u8()?;
        if (first_byte & LP_ENCODING_7BIT_UINT_MASK) == LP_ENCODING_7BIT_UINT {
            // 7bit uint
            uval = u64::from(first_byte & 0x7f); // 0x7f is 01111111
            negmax = 0;
            negstart = u64::MAX; // 7 bit ints are always positive
            let _ = reader.read_bytes(Self::lp_encode_backlen(1)).await?; // encode: 1 byte
        } else if (first_byte & LP_ENCODING_6BIT_STR_MASK) == LP_ENCODING_6BIT_STR {
            // 6bit length str
            let length = usize::from(first_byte & 0x3f); // 0x3f is 00111111
            let ele = reader.read_bytes(length).await?;
            let _ = reader
                .read_bytes(Self::lp_encode_backlen(1 + length))
                .await?; // encode: 1byte, str: length

            let ele = RedisString::from(ele);
            return Ok(ele);
            // return Ok(RedisString::from(ele));
        } else if (first_byte & LP_ENCODING_13BIT_INT_MASK) == LP_ENCODING_13BIT_INT {
            // 13bit int
            let second_byte = reader.read_u8()?;
            uval = (u64::from(first_byte & 0x1f) << 8) | u64::from(second_byte); // 5bit + 8bit, 0x1f is 00011111
            negstart = (1_u64) << 12;
            negmax = 8191; // uint13_max
            let _ = reader.read_bytes(Self::lp_encode_backlen(2)).await?;
        } else if (first_byte & LP_ENCODING_16BIT_INT_MASK) == LP_ENCODING_16BIT_INT {
            // 16bit int
            uval = reader.read_u16::<LittleEndian>()? as u64;
            negstart = (1_u64) << 15;
            negmax = u16::MAX as u64;
            let _ = reader.read_bytes(Self::lp_encode_backlen(2)).await?; // encode: 1byte, int: 2
        } else if (first_byte & LP_ENCODING_24BIT_INT_MASK) == LP_ENCODING_24BIT_INT {
            // 24bit int
            uval = reader.read_u24::<LittleEndian>()? as u64;
            negstart = (1_u64) << 23;
            negmax = (u32::MAX >> 8) as u64; // uint24_max
            let _ = reader.read_bytes(Self::lp_encode_backlen(1 + 3)).await?; // encode: 1byte, int: 3byte
        } else if (first_byte & LP_ENCODING_32BIT_INT_MASK) == LP_ENCODING_32BIT_INT {
            // 32bit int
            uval = reader.read_u32::<LittleEndian>()? as u64;
            negstart = (1_u64) << 31;
            negmax = u32::MAX as u64; // uint32_max
            let _ = reader.read_bytes(Self::lp_encode_backlen(1 + 4)).await?; // encode: 1byte, int: 4byte
        } else if (first_byte & LP_ENCODING_64BIT_INT_MASK) == LP_ENCODING_64BIT_INT {
            // 64bit int
            uval = reader.read_u64::<LittleEndian>()?;
            negstart = (1_u64) << 63;
            negmax = u64::MAX; // uint64_max
            let _ = reader.read_bytes(Self::lp_encode_backlen(1 + 8)).await?; // encode: 1byte, int: 8byte
        } else if (first_byte & LP_ENCODING_12BIT_STR_MASK) == LP_ENCODING_12BIT_STR {
            // 12bit length str
            let second_byte = reader.read_u8()?;
            let length = (((first_byte as usize) & 0x0f) << 8) + second_byte as usize; // 4bit + 8bit
            let ele = reader.read_bytes(length).await?;
            let _ = reader
                .read_bytes(Self::lp_encode_backlen(2 + length))
                .await?; // encode: 2byte, str: length
            return Ok(RedisString::from(ele));
        } else if (first_byte & LP_ENCODING_32BIT_STR_MASK) == LP_ENCODING_32BIT_STR {
            // 32bit length str
            let length = reader.read_u32::<LittleEndian>()? as usize;
            let ele = reader.read_bytes(length).await?;
            let _ = reader
                .read_bytes(Self::lp_encode_backlen(5 + length))
                .await?; // encode: 1byte, length: 4byte, str: length
            return Ok(RedisString::from(ele));
        } else {
            // redis use this value, don't know why
            // uval = 12345678900000000 + uint64(fireByte)
            // negstart = math.MaxUint64
            // negmax = 0
            bail! {DtError::redis_rdb(format!(
                "unknown encoding: {}",
                first_byte
            ))}
        }

        // We reach this code path only for integer encodings.
        // Convert the unsigned value to the signed one using two's complement
        // rule.
        if uval >= negstart {
            // This three steps conversion should avoid undefined behaviors
            // in the unsigned -> signed conversion.
            uval = negmax - uval;
            val = uval as i64;
            val = -val - 1;
        } else {
            val = uval as i64;
        }
        Ok(RedisString::from(val.to_string()))
    }

    /// Return length(bytes) for encoding backlen in Redis protocol
    fn lp_encode_backlen(len: usize) -> usize {
        if len <= 127 {
            1
        } else if len < 16383 {
            2
        } else if len < 2097151 {
            3
        } else if len < 268435455 {
            4
        } else {
            5
        }
    }
}
