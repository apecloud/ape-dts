#[allow(clippy::module_inception)]
pub mod entry_parser;
pub mod hash_parser;
pub mod list_parser;
pub mod module2_parser;
pub mod set_parser;
pub mod stream_parser;
pub mod string_parser;
pub mod zset_parser;

const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST: u8 = 1;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_ZSET: u8 = 3;
const RDB_TYPE_HASH: u8 = 4;
const RDB_TYPE_ZSET_2: u8 = 5;
const RDB_TYPE_MODULE: u8 = 6;
const RDB_TYPE_MODULE_2: u8 = 7;

const RDB_TYPE_HASH_ZIPMAP: u8 = 9;
const RDB_TYPE_LIST_ZIPLIST: u8 = 10;
const RDB_TYPE_SET_INTSET: u8 = 11;
const RDB_TYPE_ZSET_ZIPLIST: u8 = 12;
const RDB_TYPE_HASH_ZIPLIST: u8 = 13;
const RDB_TYPE_LIST_QUICKLIST: u8 = 14;
const RDB_TYPE_STREAM_LISTPACKS: u8 = 15;
const RDB_TYPE_HASH_LISTPACK: u8 = 16;
const RDB_TYPE_ZSET_LISTPACK: u8 = 17;
const RDB_TYPE_LIST_QUICKLIST_2: u8 = 18;
const RDB_TYPE_STREAM_LISTPACKS_2: u8 = 19;
const RDB_TYPE_SET_LISTPACK: u8 = 20;
const RDB_TYPE_STREAM_LISTPACKS_3: u8 = 21;

// https://github.com/redis/redis/pull/13391
const RDB_TYPE_HASH_METADATA_PRE_GA: u8 = 22;
const RDB_TYPE_HASH_LISTPACK_EX_PRE_GA: u8 = 23;
const RDB_TYPE_HASH_METADATA: u8 = 24;
const RDB_TYPE_HASH_LISTPACK_EX: u8 = 25;

// Stream with IDMP (Idempotent Message Producer) support.
const RDB_TYPE_STREAM_LISTPACKS_4: u8 = 26;
