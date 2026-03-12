#[derive(Clone, Debug, Hash, Default)]
pub struct RateLimiterConfig {
    pub max_mbps: u32,
    pub max_rps: u32,
}

#[derive(Clone, Debug, Hash, Default)]
pub struct CapacityLimiterConfig {
    pub buffer_size: usize,
    pub buffer_memory_mb: usize,
}
