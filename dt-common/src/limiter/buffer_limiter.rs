use crate::{
    config::pipeline_config::PipelineConfig,
    limiter::limiter::{Limiter, UnitType},
    meta::dt_data::DtItem,
};

pub struct BufferLimiter {
    limiters: Vec<Box<dyn Limiter + Send + Sync>>,
}

impl BufferLimiter {
    pub fn from_config(config: &PipelineConfig) -> Option<Self> {
        let mut limiters: Vec<Box<dyn Limiter + Send + Sync>> = Vec::new();
        if config.max_rps > 0 {
            limiters.push(Box::new(crate::limiter::rate_limiter::RateLimiter::new(
                config.max_rps as u32,
                UnitType::Records,
            )));
        }
        if config.max_bps > 0 {
            limiters.push(Box::new(crate::limiter::rate_limiter::RateLimiter::new(
                config.max_bps as u32,
                UnitType::Bytes,
            )));
        }
        if config.buffer_size > 0 {
            limiters.push(Box::new(
                crate::limiter::capacity_limiter::CapacityLimiter::new(
                    config.buffer_size as u32,
                    UnitType::Records,
                ),
            ));
        }
        if config.buffer_memory_mb > 0 {
            let capacity_bytes = (config.buffer_memory_mb as u64) * 1024 * 1024;
            limiters.push(Box::new(
                crate::limiter::capacity_limiter::CapacityLimiter::new(
                    capacity_bytes as u32,
                    UnitType::Bytes,
                ),
            ));
        }
        if limiters.is_empty() {
            None
        } else {
            Some(Self { limiters })
        }
    }

    pub async fn acquire(&self, dt_item: &DtItem) -> anyhow::Result<()> {
        for limiter in &self.limiters {
            match limiter.get_unit_type().await {
                UnitType::Bytes => {
                    let size = dt_item.dt_data.get_data_size() as u32;
                    limiter.acquire(size).await?;
                }
                UnitType::Records => {
                    limiter.acquire(1).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn release(&self, dt_item: &DtItem) {
        for limiter in &self.limiters {
            match limiter.get_unit_type().await {
                UnitType::Bytes => {
                    let size = dt_item.dt_data.get_data_size() as u32;
                    limiter.release(size).await;
                }
                UnitType::Records => {
                    limiter.release(1).await;
                }
            }
        }
    }
}
