use async_trait::async_trait;

use dt_common::config::config_enums::ConflictPolicyEnum;
use dt_common::meta::dt_data::DtItem;

use crate::sinker::base_sinker::BaseSinker;
use crate::Sinker;

pub struct ZkSinker {
    pub url: String,
    pub batch_size: usize,
    pub create_if_not_exists: bool,
    pub sync_ephemeral_as_persistent: bool,
    pub conflict_policy: ConflictPolicyEnum,
    pub base_sinker: BaseSinker,
}

#[async_trait]
impl Sinker for ZkSinker {
    async fn sink_raw(&mut self, _data: Vec<DtItem>, _batch: bool) -> anyhow::Result<()> {
        todo!("ZkSinker::sink_raw not yet implemented")
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
