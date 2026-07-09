use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ZkStat {
    pub version: i32,
    pub cversion: i32,
    pub aversion: i32,
    pub mtime: i64,
    pub ctime: i64,
    pub czxid: i64,
    pub mzxid: i64,
    pub ephemeral_owner: i64,
}
