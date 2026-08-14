use strum::EnumString;

use crate::config::limiter_config::CapacityLimiterConfig;

#[derive(Clone)]
pub struct PipelineConfig {
    pub pipeline_type: PipelineType,
    pub capacity_limiter: CapacityLimiterConfig,
    pub checkpoint_interval_secs: u64,
    pub batch_sink_interval_secs: u64,
    pub counter_time_window_secs: u64,
    pub counter_max_sub_count: u64,
}

#[derive(Clone, Debug, Default, EnumString, PartialEq, Eq)]
pub enum PipelineType {
    #[default]
    #[strum(serialize = "basic")]
    Basic,
    #[strum(serialize = "dependency")]
    Dependency,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::PipelineType;

    #[test]
    fn pipeline_type_parses_config_values() {
        assert_eq!(
            PipelineType::from_str("basic").unwrap(),
            PipelineType::Basic
        );
        assert_eq!(
            PipelineType::from_str("dependency").unwrap(),
            PipelineType::Dependency
        );
    }
}
