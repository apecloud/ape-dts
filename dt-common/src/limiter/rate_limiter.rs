use async_trait::async_trait;
use governor;

use crate::{
    limiter::limiter::{Limiter, UnitType},
    log_error,
};

pub struct RateLimiter {
    limiter: governor::DefaultDirectRateLimiter,
    capacity: u32,
    unit_type: UnitType,
}

impl RateLimiter {
    pub fn new(mut rate: u32, unit_type: UnitType) -> Self {
        if rate == 0 {
            rate = u32::MAX;
            log_error!(
                "Rate limiter is set to 0, which means no limit. Using max u32 value as the rate."
            );
        }
        let quota = governor::Quota::per_second(std::num::NonZeroU32::new(rate).unwrap());
        let limiter = governor::RateLimiter::direct(quota);
        Self {
            limiter,
            capacity: rate,
            unit_type,
        }
    }
}

#[async_trait]
impl Limiter for RateLimiter {
    async fn acquire(&self, n: u32) -> anyhow::Result<()> {
        if n > 0 {
            let num = std::num::NonZeroU32::new(n)
                .ok_or_else(|| anyhow::anyhow!("n must be greater than 0"))?;
            match self.limiter.until_n_ready(num).await {
                Ok(_) => {}
                Err(e) => {
                    log_error!("Failed to acquire from rate limiter: {}", e);
                    return Err(anyhow::anyhow!(
                        "`{}` exceeds max capacity `{}` of the rate limiter: {}",
                        n,
                        self.capacity,
                        e
                    ));
                }
            }
        }
        Ok(())
    }

    async fn release(&self, _n: u32) {}

    async fn get_unit_type(&self) -> UnitType {
        self.unit_type.clone()
    }
}
