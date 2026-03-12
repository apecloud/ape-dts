use crate::config::limiter_config::CapacityLimiterConfig;

use super::config_enums::PipelineType;

#[derive(Clone)]
pub struct PipelineConfig {
    pub pipeline_type: PipelineType,
    pub capacity_limiter: CapacityLimiterConfig,
    pub checkpoint_interval_secs: u64,
    pub batch_sink_interval_secs: u64,
    pub counter_time_window_secs: u64,
    pub counter_max_sub_count: u64,
    // Deprecated: used when pipeline_type == http_server
    pub http_host: String,
    pub http_port: u64,
    pub with_field_defs: bool,
}
