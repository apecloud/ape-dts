use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use dt_common::meta::syncer::Syncer;
use dt_common::zk_filter::ZkFilter;

use crate::extractor::base_extractor::{BaseExtractor, ExtractState};
use crate::extractor::resumer::recovery::Recovery;
use crate::Extractor;

pub struct ZkExtractor {
    pub url: String,
    pub watch_paths: Vec<String>,
    pub scan_interval_secs: u64,
    pub include_ephemeral: bool,
    pub heartbeat_interval_secs: u64,
    pub filter: ZkFilter,
    pub base_extractor: BaseExtractor,
    pub extract_state: ExtractState,
    pub syncer: Arc<Mutex<Syncer>>,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

#[async_trait]
impl Extractor for ZkExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        todo!("ZkExtractor::extract not yet implemented")
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
