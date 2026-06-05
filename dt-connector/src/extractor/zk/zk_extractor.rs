use async_trait::async_trait;

use dt_common::zk_filter::ZkFilter;

use crate::extractor::base_extractor::BaseExtractor;
use crate::extractor::extractor_monitor::ExtractorMonitor;
use crate::Extractor;

pub struct ZkExtractor {
    pub url: String,
    pub watch_paths: Vec<String>,
    pub scan_interval_secs: u64,
    pub include_ephemeral: bool,
    pub heartbeat_interval_secs: u64,
    pub filter: ZkFilter,
    pub base_extractor: BaseExtractor,
    pub monitor: ExtractorMonitor,
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
