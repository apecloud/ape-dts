use std::collections::HashMap;

use crate::error_boundary::extractor_error::{rdb_error, redis_rdb_source};
use anyhow::bail;
use byteorder::{BigEndian, ByteOrder};
use dt_common::meta::redis::redis_object::{RedisCmd, RedisString, StreamObject};

use crate::extractor::redis::rdb::reader::rdb_reader::RdbReader;

pub struct StreamParser {}

impl StreamParser {
    pub async fn load_from_buffer(
        reader: &mut RdbReader<'_>,
        master_key: RedisString,
        type_byte: u8,
    ) -> anyhow::Result<StreamObject> {
        let mut obj = StreamObject::new();
        obj.key = master_key.clone();

        // 1. length(number of listpack), k1, v1, k2, v2, ..., number, ms, seq

        // Load the number of Listpack.
        let n_list_pack = reader.read_length().await?;
        for _ in 0..n_list_pack {
            // Load key
            // key is streamId, like: 1612181627287-0
            let key = reader.read_string().await?;
            if key.as_bytes().len() != 16 {
                bail! {rdb_error(format!(
                    "invalid stream ID byte length: {}",
                    key.as_bytes().len()
                ))}
            }
            let master_ms = BigEndian::read_i64(&key.as_bytes()[..8]); // ms
            let master_seq = BigEndian::read_i64(&key.as_bytes()[8..]);

            // value is a listpack
            let elements = reader.read_list_pack().await?;
            let mut inx = 0usize;

            // The front of stream listpack is master entry
            // Parse the master entry
            let mut count = Self::next_integer(&mut inx, &elements)?; // count
            let mut deleted = Self::next_integer(&mut inx, &elements)?; // deleted
            let num_fields = Self::next_integer(&mut inx, &elements)? as usize; // num-fields

            let fields = elements
                .get(3..3 + num_fields)
                .ok_or_else(|| rdb_error("stream field definitions exceed the listpack length"))?; // fields
            inx = 3 + num_fields;

            // master entry end by zero
            let last_entry =
                String::try_from(Self::next(&mut inx, &elements)?.clone()).map_err(|error| {
                    redis_rdb_source("stream listpack marker is not valid UTF-8", error)
                })?;
            if last_entry != "0" {
                bail! {rdb_error(format!(
                    "master entry not ends by zero. lastEntry=[{}]",
                    last_entry
                ))}
            }

            // Parse entries
            while count != 0 || deleted != 0 {
                let flags = Self::next_integer(&mut inx, &elements)?; // [is_same_fields|is_deleted]
                let entry_ms = Self::next_integer(&mut inx, &elements)?;
                let entry_seq = Self::next_integer(&mut inx, &elements)?;
                let value = &format!("{}-{}", entry_ms + master_ms, entry_seq + master_seq);

                let mut cmd = RedisCmd::new();
                cmd.add_str_arg("xadd");
                cmd.add_redis_arg(&master_key);
                cmd.add_str_arg(value);

                if flags & 2 == 2 {
                    // same fields, get field from master entry.
                    for field in fields.iter().take(num_fields) {
                        cmd.add_redis_arg(field);
                        cmd.add_redis_arg(Self::next(&mut inx, &elements)?);
                    }
                } else {
                    // get field by lp.Next()
                    let num = Self::next_integer(&mut inx, &elements)? as usize;
                    let values = elements.get(inx..inx + num * 2).ok_or_else(|| {
                        rdb_error("stream entry fields exceed the listpack length")
                    })?;
                    for ele in values {
                        cmd.add_redis_arg(ele);
                    }
                    inx += num * 2;
                }

                Self::next(&mut inx, &elements)?; // lp_count

                if flags & 1 == 1 {
                    // is_deleted
                    deleted -= 1;
                } else {
                    count -= 1;
                    obj.cmds.push(cmd);
                }
            }
        }

        // Load total number of items inside the stream.
        reader.read_length().await?;
        // Load the last entry ID.
        let last_ms = reader.read_length().await?;
        let last_seq = reader.read_length().await?;
        let last_id = format!("{}-{}", last_ms, last_seq);
        if n_list_pack == 0 {
            // Use the XADD MAXLEN 0 trick to generate an empty stream if
            // the key we are serializing is an empty string, which is possible
            // for the Stream type.
            let mut cmd = RedisCmd::new();
            cmd.add_str_arg("XADD");
            cmd.add_redis_arg(&master_key);
            cmd.add_str_arg("MAXLEN");
            cmd.add_str_arg("0");
            cmd.add_str_arg(&last_id);
            cmd.add_str_arg("x");
            cmd.add_str_arg("y");
            obj.cmds.push(cmd);
        }

        // Append XSETID after XADD, make sure lastid is correct,
        // in case of XDEL lastid.
        let mut cmd = RedisCmd::new();
        cmd.add_str_arg("XSETID");
        cmd.add_redis_arg(&master_key);
        cmd.add_str_arg(&last_id);
        obj.cmds.push(cmd);

        if type_byte >= super::RDB_TYPE_STREAM_LISTPACKS_2 {
            // Load the first entry ID.
            let _ = reader.read_length().await?; // first_ms
            let _ = reader.read_length().await?; // first_seq

            /* Load the maximal deleted entry ID. */
            let _ = reader.read_length().await?; // max_deleted_ms
            let _ = reader.read_length().await?; // max_deleted_seq

            /* Load the offset. */
            let _ = reader.read_length().await?; // offset
        }

        // 2. nConsumerGroup, groupName, ms, seq, PEL, Consumers

        // Load the number of groups.
        let n_consumer_group = reader.read_length().await?;
        for _i in 0..n_consumer_group {
            // Load groupName
            let group_name = reader.read_string().await?;

            /* Load the last ID */
            let last_ms = reader.read_length().await?;
            let last_seq = reader.read_length().await?;
            let last_id = format!("{}-{}", last_ms, last_seq);

            /* Create Group */
            let mut cmd = RedisCmd::new();
            cmd.add_str_arg("XGROUP");
            cmd.add_str_arg("CREATE");
            cmd.add_redis_arg(&master_key);
            cmd.add_redis_arg(&group_name);
            cmd.add_str_arg(&last_id);
            obj.cmds.push(cmd);

            /* Load group offset. */
            if type_byte >= super::RDB_TYPE_STREAM_LISTPACKS_2 {
                reader.read_length().await?; // offset
            }

            /* Load the global PEL */
            let n_pel = reader.read_length().await?;
            let mut map_id_to_time = HashMap::new();
            let mut map_id_to_count = HashMap::new();

            for _ in 0..n_pel {
                // Load streamId
                let ms = reader.read_be_u64().await?;
                let seq = reader.read_be_u64().await?;
                let stream_id = format!("{}-{}", ms, seq);

                // Load deliveryTime
                let delivery_time = reader.read_u64().await?.to_string();

                // Load deliveryCount
                let delivery_count = reader.read_length().await?.to_string();

                // Save deliveryTime and deliveryCount
                map_id_to_time.insert(stream_id.clone(), delivery_time);
                map_id_to_count.insert(stream_id, delivery_count);
            }

            // Generate XCLAIMs for each consumer that happens to
            // have pending entries. Empty consumers are discarded.
            let n_consumer = reader.read_length().await?;
            for _i in 0..n_consumer {
                /* Load consumerName */
                let consumer_name = reader.read_string().await?;

                /* Load lastSeenTime */
                let _ = reader.read_u64().await?;

                if type_byte >= super::RDB_TYPE_STREAM_LISTPACKS_3 {
                    // consumer->active_time = rdbLoadMillisecondTime(rdb,RDB_VERSION);
                    let _ = reader.read_u64().await;
                }

                /* Consumer PEL */
                let n_pel = reader.read_length().await?;
                for _i in 0..n_pel {
                    // Load streamId
                    let ms = reader.read_be_u64().await?;
                    let seq = reader.read_be_u64().await?;
                    let stream_id = format!("{}-{}", ms, seq);

                    /* Send */
                    let mut cmd = RedisCmd::new();
                    cmd.add_str_arg("XCLAIM");
                    cmd.add_redis_arg(&master_key);
                    cmd.add_redis_arg(&group_name);
                    cmd.add_redis_arg(&consumer_name);
                    cmd.add_str_arg("0");
                    cmd.add_str_arg(&stream_id);
                    cmd.add_str_arg("TIME");
                    let Some(delivery_time) = map_id_to_time.get(&stream_id) else {
                        bail! {rdb_error(format!(
                            "pending stream entry {stream_id} has no delivery time"
                        ))}
                    };
                    cmd.add_str_arg(delivery_time);
                    cmd.add_str_arg("RETRYCOUNT");
                    let Some(delivery_count) = map_id_to_count.get(&stream_id) else {
                        bail! {rdb_error(format!(
                            "pending stream entry {stream_id} has no delivery count"
                        ))}
                    };
                    cmd.add_str_arg(delivery_count);
                    cmd.add_str_arg("JUSTID");
                    cmd.add_str_arg("FORCE");
                    obj.cmds.push(cmd);
                }
            }
        }

        Ok(obj)
    }

    fn next_integer(inx: &mut usize, elements: &[RedisString]) -> anyhow::Result<i64> {
        let ele = elements
            .get(*inx)
            .ok_or_else(|| rdb_error("unexpected end of stream listpack"))?;
        *inx += 1;
        let value = String::try_from(ele.clone()).map_err(|error| {
            redis_rdb_source("stream listpack integer is not valid UTF-8", error)
        })?;
        value
            .parse::<i64>()
            .map_err(|error| redis_rdb_source("invalid integer in stream listpack", error))
    }

    fn next<'a>(inx: &mut usize, elements: &'a [RedisString]) -> anyhow::Result<&'a RedisString> {
        let ele = elements
            .get(*inx)
            .ok_or_else(|| rdb_error("unexpected end of stream listpack"))?;
        *inx += 1;
        Ok(ele)
    }
}
