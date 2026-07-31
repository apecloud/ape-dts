use crate::meta::redis::redis_object::RedisCmd;

/// redis resp protocol data type
pub const RESP_STATUS: u8 = b'+'; // +<string>\r\n
pub const RESP_ERROR: u8 = b'-'; // -<string>\r\n
pub const RESP_STRING: u8 = b'$'; // $<length>\r\n<bytes>\r\n
pub const RESP_INT: u8 = b':'; // :<number>\r\n
pub const RESP_NIL: u8 = b'_'; // _\r\n
pub const RESP_FLOAT: u8 = b','; // ,<floating-point-number>\r\n (golang float)
pub const RESP_BOOL: u8 = b'#'; // true: #t\r\n false: #f\r\n
pub const RESP_BLOB_ERROR: u8 = b'!'; // !<length>\r\n<bytes>\r\n
pub const RESP_VERBATIM: u8 = b'='; // =<length>\r\nFORMAT:<bytes>\r\n
pub const RESP_BIG_INT: u8 = b'('; // (<big number>\r\n
pub const RESP_ARRAY: u8 = b'*'; // *<len>\r\n... (same as resp2)
pub const RESP_MAP: u8 = b'%'; // %<len>\r\n(key)\r\n(value)\r\n... (golang map)
pub const RESP_SET: u8 = b'~'; // ~<len>\r\n... (same as Array)
pub const RESP_ATTR: u8 = b'|'; // |<len>\r\n(key)\r\n(value)\r\n... + command reply
pub const RESP_PUSH: u8 = b'>'; // ><len>\r\n... (same as Array)

pub struct CmdEncoder {}

impl CmdEncoder {
    pub fn encode(cmd: &RedisCmd) -> Vec<u8> {
        let mut buf = Vec::new();

        buf.push(RESP_ARRAY);
        // write array length
        Self::write_length(&mut buf, cmd.args.len());

        for arg in cmd.args.iter() {
            Self::write_arg(&mut buf, arg);
        }
        buf
    }

    pub fn write_arg(buf: &mut Vec<u8>, arg: &[u8]) {
        buf.push(RESP_STRING);
        // write arg length
        Self::write_length(buf, arg.len());
        // write arg data
        buf.extend_from_slice(arg);
        // write crlf
        Self::write_crlf(buf);
    }

    fn write_length(buf: &mut Vec<u8>, len: usize) {
        buf.extend_from_slice(len.to_string().as_bytes());
        Self::write_crlf(buf);
    }

    fn write_crlf(buf: &mut Vec<u8>) {
        buf.extend_from_slice(b"\r\n");
    }
}
