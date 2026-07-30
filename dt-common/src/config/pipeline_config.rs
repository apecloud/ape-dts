use crate::config::limiter_config::CapacityLimiterConfig;

#[derive(Clone)]
pub struct PipelineConfig {
    pub capacity_limiter: CapacityLimiterConfig,
    pub checkpoint_interval_secs: u64,
    pub batch_sink_interval_secs: u64,
    pub counter_time_window_secs: u64,
    pub counter_max_sub_count: u64,
}
