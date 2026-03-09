use super::config_enums::PipelineType;

#[derive(Clone)]
pub struct PipelineConfig {
    pub pipeline_type: PipelineType,
    pub buffer_size: usize,
    pub buffer_memory_mb: usize,
    // TODO: alpha feature, need more test
    pub max_rps: u64,
    pub max_bps: u64,
    pub checkpoint_interval_secs: u64,
    pub batch_sink_interval_secs: u64,
    pub counter_time_window_secs: u64,
    pub counter_max_sub_count: u64,
    // Deprecated: used when pipeline_type == http_server
    pub http_host: String,
    pub http_port: u64,
    pub with_field_defs: bool,
}
