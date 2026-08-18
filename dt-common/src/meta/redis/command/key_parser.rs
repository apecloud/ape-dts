use std::collections::{HashMap, HashSet};

use anyhow::{bail, Context};

use crate::{
    config::config_enums::DbType,
    error::{DtError, DtOptionExt},
};

use super::{cmd_constants::CmdConstants, cmd_meta::CmdMeta};

pub struct KeyParser {
    pub container_cmds: HashSet<String>,
    pub cmd_metas: HashMap<String, CmdMeta>,
}

impl KeyParser {
    pub fn new() -> anyhow::Result<Self> {
        let containers: HashSet<String> = serde_json::from_str(CmdConstants::CONTAINER_COMMANDS)
            .context(DtError::DatabaseInvariant(
                DbType::Redis,
                "the embedded Redis command catalog is invalid".to_string(),
            ))?;
        let metas: Vec<CmdMeta> = serde_json::from_str(CmdConstants::COMMAND_METAS).context(
            DtError::DatabaseInvariant(
                DbType::Redis,
                "the embedded Redis command catalog is invalid".to_string(),
            ),
        )?;

        let mut cmd_metas = HashMap::new();
        for cmd in metas {
            cmd_metas.insert(cmd.name.clone(), cmd);
        }

        Ok(Self {
            container_cmds: containers,
            cmd_metas,
        })
    }

    pub fn parse_key_from_argv(
        &self,
        argv: &[String],
    ) -> anyhow::Result<(String, String, Vec<String>, Vec<usize>)> {
        // refer: https://github.com/tair-opensource/RedisShake/blob/v4/internal/commands/keys.go
        let mut cmd_name = argv
            .first()
            .or_dt_error(DtError::RedisCmdError("empty Redis command".to_string()))?
            .to_uppercase();
        if self.container_cmds.contains(&cmd_name) {
            let subcommand = argv.get(1).or_dt_error(DtError::RedisCmdError(format!(
                "missing subcommand for command: {cmd_name}"
            )))?;
            cmd_name = format!("{}-{}", cmd_name, subcommand.to_uppercase());
        }

        let cmd = match self.cmd_metas.get(&cmd_name) {
            Some(cmd) => cmd,
            None => {
                bail!(DtError::RedisCmdError(format!(
                    "unknown command: {cmd_name}"
                )))
            }
        };

        let arg_cout = argv.len() as i32;
        let group = cmd.group.clone();

        let mut keys = vec![];
        let mut keys_indexes = vec![];
        for spec in &cmd.key_spec {
            let begin: i32;
            match spec.begin_search_type.as_str() {
                // The index type of begin_search indicates that input keys appear at a constant index.
                // It is a map under the spec key with a single key:
                // index: the 0-based index from which the client should start extracting key names.
                "index" => begin = spec.begin_search_index,

                // The keyword type of begin_search means a literal token precedes key name arguments.
                // It is a map under the spec with two keys:
                "keyword" => {
                    // startfrom: an index to the arguments array from which the client should begin searching.
                    // This can be a negative value, which means the search should start from the end of the arguments' array,
                    // in reverse order. For example, -2's meaning is to search reverse from the penultimate argument.
                    let (mut idx, step) = if spec.begin_search_start_from > 0 {
                        (spec.begin_search_start_from, 1)
                    } else {
                        (arg_cout + spec.begin_search_start_from, -1)
                    };

                    loop {
                        if idx <= 0 || idx >= arg_cout {
                            bail!(DtError::RedisCmdError(format!(
                                "keyword not found: {cmd_name}"
                            )))
                        }
                        if argv[idx as usize].to_uppercase() == spec.begin_search_keyword {
                            begin = idx + 1;
                            break;
                        }
                        idx += step;
                    }
                }

                _ => {
                    bail! {DtError::RedisCmdError(format!(
                        "unsupported begin search type: {}",
                        spec.begin_search_type
                    ))}
                }
            }

            match spec.find_keys_type.as_str() {
                "range" => {
                    // lastkey: the index, relative to begin_search, of the last key argument.
                    // This can be a negative value, in which case it isn't relative.
                    // For example, -1 indicates to keep extracting keys until the last argument,
                    // -2 until one before the last, and so on.
                    let last_key_idx = if spec.find_keys_range_last_key >= 0 {
                        begin + spec.find_keys_range_last_key
                    } else {
                        arg_cout + spec.find_keys_range_last_key
                    };

                    // limit: if lastkey is has the value of -1, we use the limit to stop the search by a factor.
                    // 0 and 1 mean no limit. 2 means half of the remaining arguments, 3 means a third, and so on.
                    let mut limit_count = i32::MAX;
                    if spec.find_keys_range_limit >= 2 {
                        limit_count = (arg_cout - begin) / (spec.find_keys_range_limit);
                    }

                    // keystep: the number of arguments that should be skipped,
                    // after finding a key, to find the next one.
                    for idx in (begin..=last_key_idx).step_by(spec.find_keys_range_key_step) {
                        let key =
                            argv.get(idx as usize)
                                .or_dt_error(DtError::RedisCmdError(format!(
                                    "key index {idx} is out of range for command: {cmd_name}"
                                )))?;
                        keys.push(key.clone());
                        keys_indexes.push(idx as usize + 1);
                        limit_count -= 1;
                        if limit_count <= 0 {
                            break;
                        }
                    }
                }

                "keynum" => {
                    // keynumidx: the index, relative to begin_search, of the argument containing the number of keys.
                    let keynum_idx = begin + spec.find_keys_keynum_index;
                    if keynum_idx < 0 || keynum_idx >= arg_cout {
                        bail! {DtError::RedisCmdError(format!(
                            "wrong keynumidx: {}",
                            keynum_idx
                        ))}
                    }

                    let key_count = argv[keynum_idx as usize].parse::<usize>().context(
                        DtError::RedisCmdError(format!("invalid key count for command {cmd_name}")),
                    )?;
                    // firstkey: the index, relative to begin_search, of the first key.
                    // This is usually the next argument after keynumidx, and its value, in this case, is greater by one.
                    for idx in (begin + spec.find_keys_keynum_first_key..)
                        .step_by(spec.find_keys_keynum_key_step)
                        .take(key_count)
                    {
                        let key =
                            argv.get(idx as usize)
                                .or_dt_error(DtError::RedisCmdError(format!(
                                    "key index {idx} is out of range for command: {cmd_name}"
                                )))?;
                        keys.push(key.clone());
                        keys_indexes.push(idx as usize + 1);
                    }
                }

                _ => {
                    bail! {DtError::RedisCmdError(format!(
                        "unsupported find keys type: {}",
                        spec.find_keys_type
                    ))}
                }
            }
        }

        Ok((cmd_name, group, keys, keys_indexes))
    }

    pub fn calc_slot(key_bytes: &[u8]) -> u16 {
        // refer: https://redis.io/docs/reference/key-specs/
        // refer: https://redis.io/commands/cluster-keyslot/
        let mut hash_tag = None;

        // command examples:
        // > CLUSTER KEYSLOT somekey
        // (integer) 11058
        // > CLUSTER KEYSLOT foo{hash_tag}
        // (integer) 2515
        // > CLUSTER KEYSLOT bar{hash_tag}
        // (integer) 2515
        for i in 0..key_bytes.len() {
            // '{'
            if key_bytes[i] == 123 {
                for k in i..key_bytes.len() {
                    // '}'
                    if key_bytes[k] == 125 {
                        hash_tag = Some(&key_bytes[i + 1..k]);
                        break;
                    }
                }
            }

            if hash_tag.is_some() {
                break;
            }
        }

        // refer: https://redis.io/docs/management/scaling/
        // There are 16384 hash slots in Redis Cluster,
        // and to compute what is the hash slot of a given key,
        // we simply take the CRC16 of the key modulo 16384.
        if let Some(tag) = hash_tag {
            if !tag.is_empty() {
                return KeyParser::crc16(tag) & 0x3FFF;
            }
        }
        KeyParser::crc16(key_bytes) & 0x3FFF
    }

    pub fn crc16(key: &[u8]) -> u16 {
        let mut crc: u16 = 0;
        for &n in key {
            crc =
                (crc << 8) ^ CmdConstants::CRC16_TABLE[((crc >> 8) ^ (n as u16)) as usize & 0x00FF];
        }
        crc
    }
}

#[cfg(test)]
mod tests {

    use std::vec;

    use super::*;

    #[test]
    fn test_calc_slot() {
        let keys = [
            "somekey",
            "中文",
            "set_key_3_  😀",
            "foo{hash_tag}",
            "bar{hash_tag}",
            "aaaaa{hash_tag}aaaaa",
            "中文{hash_tag}set_key_3_  😀",
            "set_key_3_  😀{hash_tag}中文",
        ];
        let expected_slots = [11058, 13257, 16210, 2515, 2515, 2515, 2515, 2515];

        for i in 0..keys.len() {
            let slot = KeyParser::calc_slot(keys[i].as_bytes());
            assert_eq!(slot, expected_slots[i]);
        }
    }

    #[test]
    fn test_parse_key_from_argv_1() {
        let (cmds, expected_cmd_names, expected_keys_vec) = mock_parse_key_test_data();

        let key_parser = KeyParser::new().unwrap();

        for i in 0..cmds.len() {
            let argv: Vec<String> = cmds[i].split(" ").map(|arg| arg.to_string()).collect();
            let (cmd_name, _group, keys, _key_indexes) =
                key_parser.parse_key_from_argv(&argv).unwrap();

            assert_eq!(cmd_name, expected_cmd_names[i].to_string());
            assert_eq!(keys, expected_keys_vec[i]);
        }
    }

    #[test]
    fn test_parse_key_from_argv_2() {
        let cmd_argv_vec = [
            vec!["SET", "set_key_3_  😀", "val_2_  😀"],
            vec![
                "XADD",
                "stream_key_2  中文😀",
                "*",
                "field_1",
                "val_1",
                "field_2_中文",
                "val_2_中文",
                "field_3_  😀",
                "val_3_  😀",
            ],
        ];
        let expected_cmd_names = ["SET", "XADD"];
        let expected_keys_vec = [vec!["set_key_3_  😀"], vec!["stream_key_2  中文😀"]];

        let key_parser = KeyParser::new().unwrap();
        for i in 0..cmd_argv_vec.len() {
            let argv: Vec<String> = cmd_argv_vec[i].iter().map(|arg| arg.to_string()).collect();
            let (cmd_name, _group, keys, _key_indexes) =
                key_parser.parse_key_from_argv(&argv).unwrap();

            let expected_keys: Vec<String> = expected_keys_vec[i]
                .iter()
                .map(|key| key.to_string())
                .collect();

            assert_eq!(cmd_name, expected_cmd_names[i].to_string());
            assert_eq!(keys, expected_keys);
        }
    }

    fn mock_parse_key_test_data() -> (Vec<String>, Vec<String>, Vec<Vec<String>>) {
        let cmds_str = r#"
        -- APPEND
        APPEND 1-1 append_0
        
        -- BITFIELD
        -- SET
        BITFIELD 2-1 SET i8 #0 100 SET i8 #1 200
        -- INCRBY
        BITFIELD 2-2 incrby i5 100 1
        BITFIELD 2-3 incrby i5 100 1 GET u4 0
        -- OVERFLOW
        BITFIELD 2-4 incrby u2 100 1 OVERFLOW SAT incrby u2 102 1
        BITFIELD 2-4 incrby u2 100 1 OVERFLOW SAT incrby u2 102 1
        BITFIELD 2-4 incrby u2 100 1 OVERFLOW SAT incrby u2 102 1
        BITFIELD 2-4 incrby u2 100 1 OVERFLOW SAT incrby u2 102 1
        BITFIELD 2-4 OVERFLOW FAIL incrby u2 102 1
        
        -- BITOP
        -- AND 
        BITOP AND 3-3 3-1 3-2
        -- OR
        BITOP OR 3-4 3-1 3-2
        -- XOR
        BITOP XOR 3-5 3-1 3-2
        -- NOT
        BITOP NOT 3-6 3-1
        
        -- BLMOVE -- version: 6.2.0
        BLMOVE 4-1 4-2 LEFT LEFT 0
        
        -- BLMPOP -- version: 7.0.0
        -- BLMPOP timeout numkeys key [key ...] <LEFT | RIGHT> [COUNT count]
        BLMPOP 0 2 5-1 5-2 LEFT COUNT 3
        
        -- BLPOP
        BLPOP 6-1 0
        
        -- BRPOP
        BRPOP 7-1 0
        
        -- BRPOPLPUSH
        BRPOPLPUSH 8-1 8-2 0
        
        -- BZMPOP
        BZMPOP 1 2 9-1 9-2 MIN
        
        -- BZPOPMAX
        BZPOPMAX 10-1 10-2 0
        
        -- BZPOPMIN
        BZPOPMIN 11-1 11-2 0
        
        -- COPY
        COPY 12-1 12-2
        
        -- DECR
        DECR 13-1
        
        -- DECRBY
        DECRBY 14-1 3
        
        -- EXPIRE
        EXPIRE 15-1 1
        EXPIRE 15-1 1 XX
        EXPIRE 15-1 1 NX
        
        -- EXPIREAT
        EXPIREAT 16-1 1
        
        -- GEOADD
        GEOADD 17-1 13.361389 38.115556 "Palermo" 15.087269 37.502669 "Catania"
        
        -- GETDEL
        GETDEL 18-1
        
        -- GETEX
        GETEX 19-1 EX 1
        
        -- GETSET
        GETSET 20-1 "World"
        
        -- HSET
        HSET 21-1 field1 "hello" field2 "world"
        
        -- HINCRBY
        HINCRBY 22-1 field 1
        
        -- HINCRBYFLOAT
        HINCRBYFLOAT 23-1 field_1 0.1
        
        -- HMSET
        HMSET 24-1 field1 "Hello" field2 "World"
        
        -- HSET
        HSET 24-1 field2 "Hi" field3 "World"
        
        -- HSETNX
        HSETNX 25-1 field "Hello"
        
        -- INCR
        INCR 26-1
        
        -- INCRBY
        INCRBY 27-1 5
        
        -- INCRBYFLOAT
        INCRBYFLOAT 28-1 0.1
        
        -- LINSERT
        LINSERT 29-1 BEFORE "World" "There"
        
        -- LMOVE
        LMOVE 30-1 30-2 RIGHT LEFT
        
        -- LMPOP
        LMPOP 1 31-1 LEFT
        
        -- LPOP
        LPOP 32-1
        LPOP 32-1 2
        
        -- LPUSH
        LPUSH 33-1 "world"
        
        -- LPUSHX
        LPUSHX 34-1 "Hello"
        
        -- LREM
        LREM 35-1 -2 "hello"
        
        -- LSET
        LSET 36-1 0 "four"
        
        -- LTRIM
        LTRIM 37-1 1 -1
        
        -- MOVE
        MOVE 38-1 1
        
        -- MSET
        MSET 39-1 "Hello" 39-2 "World"
        
        -- MSETNX
        MSETNX 40-1 "Hello" 40-2 "there"
        
        -- PERSIST
        PERSIST 41-1
        
        -- PEXPIRE
        PEXPIRE 42-1 1500000000
        PEXPIRE 42-2 1000 XX
        PEXPIRE 42-3 1000 NX
        
        -- PEXPIREAT
        PEXPIREAT 43-1 1555555555005
        
        -- PFADD
        PFADD 44-1 a b c d e f g
        
        -- PFMERGE
        PFMERGE 45-3 45-1 45-2
        
        -- PSETEX (deprecated)
        PSETEX 46-1 1000 "Hello"
        
        -- RENAME
        RENAME 47-1 47-2
        
        -- RENAMENX
        RENAMENX 48-1 48-2
        
        -- RPOP
        RPOP 49-1
        RPOP 49-1 2
        
        -- RPOPLPUSH (deprecated)
        RPOPLPUSH 50-1 50-2
        
        -- RPUSH
        RPUSH 51-1 "hello"
        
        -- RPUSHX
        RPUSHX 52-1 "World"
        
        -- SADD
        SADD 53-1 "Hello"
        
        -- SDIFFSTORE
        SDIFFSTORE 54-3 54-1 54-2
        
        -- SETBIT
        SETBIT 55-1 7 1
        
        -- SETEX
        SETEX 56-1 1 "Hello"
        
        -- SETNX
        SETNX 57-1 "Hello"
        
        -- SETRANGE
        SETRANGE 58-1 6 "Redis"
        
        -- SINTERSTORE
        SINTERSTORE 59-3 59-1 59-2
        
        -- SMOVE
        SMOVE 60-1 60-2 "two"
        
        -- SPOP
        SPOP 61-1
        
        -- SREM
        SREM 62-1 "one"
        
        -- SUNIONSTORE
        SUNIONSTORE 63-3 63-1 63-2
        
        -- SWAPDB
        SWAPDB 0 1
        
        -- UNLINK
        UNLINK 64-1 64-2 64-3
        
        -- XACK
        -- XACK mystream1 mygroup 1526569495631-0
        
        -- XADD
        XADD 65-1 1526919030474-55 message "Hello,"
        XADD 65-1 1526919030474-* message " World!"
        XADD 65-1 * name Sara surname OConnor
        XADD 65-1 * field1 value1 field2 value2 field3 value3
        
        -- XDEL
        XDEL 66-1 1538561700640-0
        
        -- XTRIM
        XTRIM 67-1 MAXLEN 1000
        
        -- ZADD
        ZADD 68-1 1 "one"
        
        -- ZDIFFSTORE
        ZDIFFSTORE 69-3 2 69-1 69-2
        
        -- ZINCRBY
        ZINCRBY 70-1 2 "one"
        
        -- ZINTERSTORE
        ZINTERSTORE 71-3 2 71-1 71-2 WEIGHTS 2 3
        
        -- ZMPOP
        ZMPOP 1 72-1 MIN
        
        -- ZPOPMAX
        ZPOPMAX 73-1
        
        -- ZPOPMIN
        ZPOPMIN 74-1
        
        -- ZRANGESTORE
        ZRANGESTORE 75-2 75-1 2 -1
        
        -- ZREM
        ZREM 76-1 "two"
        
        -- ZREMRANGEBYLEX
        ZREMRANGEBYLEX 77-1 [alpha [omega
        
        -- ZREMRANGEBYRANK
        ZREMRANGEBYRANK 78-1 0 1
        
        -- ZREMRANGEBYSCORE
        ZREMRANGEBYSCORE 79-1 -inf (2
        
        -- ZUNIONSTORE
        -- ZUNIONSTORE 80-1 2 80-2 zset2 WEIGHTS 2 3
        -- ZUNIONSTORE out 2 80-1 80-2 WEIGHTS 2 3"#;

        let expected_cmd_names_str = r#"[
            "APPEND",
            "BITFIELD",
            "BITFIELD",
            "BITFIELD",
            "BITFIELD",
            "BITFIELD",
            "BITFIELD",
            "BITFIELD",
            "BITFIELD",
            "BITOP",
            "BITOP",
            "BITOP",
            "BITOP",
            "BLMOVE",
            "BLMPOP",
            "BLPOP",
            "BRPOP",
            "BRPOPLPUSH",
            "BZMPOP",
            "BZPOPMAX",
            "BZPOPMIN",
            "COPY",
            "DECR",
            "DECRBY",
            "EXPIRE",
            "EXPIRE",
            "EXPIRE",
            "EXPIREAT",
            "GEOADD",
            "GETDEL",
            "GETEX",
            "GETSET",
            "HSET",
            "HINCRBY",
            "HINCRBYFLOAT",
            "HMSET",
            "HSET",
            "HSETNX",
            "INCR",
            "INCRBY",
            "INCRBYFLOAT",
            "LINSERT",
            "LMOVE",
            "LMPOP",
            "LPOP",
            "LPOP",
            "LPUSH",
            "LPUSHX",
            "LREM",
            "LSET",
            "LTRIM",
            "MOVE",
            "MSET",
            "MSETNX",
            "PERSIST",
            "PEXPIRE",
            "PEXPIRE",
            "PEXPIRE",
            "PEXPIREAT",
            "PFADD",
            "PFMERGE",
            "PSETEX",
            "RENAME",
            "RENAMENX",
            "RPOP",
            "RPOP",
            "RPOPLPUSH",
            "RPUSH",
            "RPUSHX",
            "SADD",
            "SDIFFSTORE",
            "SETBIT",
            "SETEX",
            "SETNX",
            "SETRANGE",
            "SINTERSTORE",
            "SMOVE",
            "SPOP",
            "SREM",
            "SUNIONSTORE",
            "SWAPDB",
            "UNLINK",
            "XADD",
            "XADD",
            "XADD",
            "XADD",
            "XDEL",
            "XTRIM",
            "ZADD",
            "ZDIFFSTORE",
            "ZINCRBY",
            "ZINTERSTORE",
            "ZMPOP",
            "ZPOPMAX",
            "ZPOPMIN",
            "ZRANGESTORE",
            "ZREM",
            "ZREMRANGEBYLEX",
            "ZREMRANGEBYRANK",
            "ZREMRANGEBYSCORE"
        ]"#;

        let expected_keys_vec_str = r#"[
            [
                "1-1"
            ],
            [
                "2-1"
            ],
            [
                "2-2"
            ],
            [
                "2-3"
            ],
            [
                "2-4"
            ],
            [
                "2-4"
            ],
            [
                "2-4"
            ],
            [
                "2-4"
            ],
            [
                "2-4"
            ],
            [
                "3-3",
                "3-1",
                "3-2"
            ],
            [
                "3-4",
                "3-1",
                "3-2"
            ],
            [
                "3-5",
                "3-1",
                "3-2"
            ],
            [
                "3-6",
                "3-1"
            ],
            [
                "4-1",
                "4-2"
            ],
            [
                "5-1",
                "5-2"
            ],
            [
                "6-1"
            ],
            [
                "7-1"
            ],
            [
                "8-1",
                "8-2"
            ],
            [
                "9-1",
                "9-2"
            ],
            [
                "10-1",
                "10-2"
            ],
            [
                "11-1",
                "11-2"
            ],
            [
                "12-1",
                "12-2"
            ],
            [
                "13-1"
            ],
            [
                "14-1"
            ],
            [
                "15-1"
            ],
            [
                "15-1"
            ],
            [
                "15-1"
            ],
            [
                "16-1"
            ],
            [
                "17-1"
            ],
            [
                "18-1"
            ],
            [
                "19-1"
            ],
            [
                "20-1"
            ],
            [
                "21-1"
            ],
            [
                "22-1"
            ],
            [
                "23-1"
            ],
            [
                "24-1"
            ],
            [
                "24-1"
            ],
            [
                "25-1"
            ],
            [
                "26-1"
            ],
            [
                "27-1"
            ],
            [
                "28-1"
            ],
            [
                "29-1"
            ],
            [
                "30-1",
                "30-2"
            ],
            [
                "31-1"
            ],
            [
                "32-1"
            ],
            [
                "32-1"
            ],
            [
                "33-1"
            ],
            [
                "34-1"
            ],
            [
                "35-1"
            ],
            [
                "36-1"
            ],
            [
                "37-1"
            ],
            [
                "38-1"
            ],
            [
                "39-1",
                "39-2"
            ],
            [
                "40-1",
                "40-2"
            ],
            [
                "41-1"
            ],
            [
                "42-1"
            ],
            [
                "42-2"
            ],
            [
                "42-3"
            ],
            [
                "43-1"
            ],
            [
                "44-1"
            ],
            [
                "45-3",
                "45-1",
                "45-2"
            ],
            [
                "46-1"
            ],
            [
                "47-1",
                "47-2"
            ],
            [
                "48-1",
                "48-2"
            ],
            [
                "49-1"
            ],
            [
                "49-1"
            ],
            [
                "50-1",
                "50-2"
            ],
            [
                "51-1"
            ],
            [
                "52-1"
            ],
            [
                "53-1"
            ],
            [
                "54-3",
                "54-1",
                "54-2"
            ],
            [
                "55-1"
            ],
            [
                "56-1"
            ],
            [
                "57-1"
            ],
            [
                "58-1"
            ],
            [
                "59-3",
                "59-1",
                "59-2"
            ],
            [
                "60-1",
                "60-2"
            ],
            [
                "61-1"
            ],
            [
                "62-1"
            ],
            [
                "63-3",
                "63-1",
                "63-2"
            ],
            [
        
            ],
            [
                "64-1",
                "64-2",
                "64-3"
            ],
            [
                "65-1"
            ],
            [
                "65-1"
            ],
            [
                "65-1"
            ],
            [
                "65-1"
            ],
            [
                "66-1"
            ],
            [
                "67-1"
            ],
            [
                "68-1"
            ],
            [
                "69-3",
                "69-1",
                "69-2"
            ],
            [
                "70-1"
            ],
            [
                "71-3",
                "71-1",
                "71-2"
            ],
            [
                "72-1"
            ],
            [
                "73-1"
            ],
            [
                "74-1"
            ],
            [
                "75-2",
                "75-1"
            ],
            [
                "76-1"
            ],
            [
                "77-1"
            ],
            [
                "78-1"
            ],
            [
                "79-1"
            ]
        ]"#;

        // cmds
        let lines: Vec<String> = cmds_str.split("\n").map(|i| i.trim().to_string()).collect();
        let mut cmds = Vec::new();
        for line in lines {
            if !line.is_empty() && !line.starts_with("--") {
                cmds.push(line);
            }
        }

        // expected cmd names
        let expected_cmd_names: Vec<String> = serde_json::from_str(expected_cmd_names_str).unwrap();

        let expected_keys_vec: Vec<Vec<String>> =
            serde_json::from_str(expected_keys_vec_str).unwrap();

        (cmds, expected_cmd_names, expected_keys_vec)
    }
}
