use serde::{Deserialize, Serialize};

use super::{zk_event_type::ZkEventType, zk_stat::ZkStat};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZkEntry {
    pub path: String,
    pub data: Option<Vec<u8>>,
    pub stat: ZkStat,
    pub ephemeral: bool,
    pub event_type: ZkEventType,
    pub source_id: String,
}

impl ZkEntry {
    pub fn get_data_size(&self) -> u64 {
        self.path.len() as u64 + self.data.as_ref().map_or(0, |d| d.len() as u64)
    }
}
