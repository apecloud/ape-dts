use async_trait::async_trait;
use governor;

use crate::limiter::limiter::{Limiter, UnitType};

pub struct RateLimiter {
    limiter: governor::DefaultDirectRateLimiter,
    unit_type: UnitType,
}

impl RateLimiter {
    pub fn new(rate: u32, unit_type: UnitType) -> Self {
        let quota = governor::Quota::per_second(std::num::NonZeroU32::new(rate).unwrap());
        let limiter = governor::RateLimiter::direct(quota);
        Self { limiter, unit_type }
    }
}

#[async_trait]
impl Limiter for RateLimiter {
    async fn acquire(&self, n: u32) -> anyhow::Result<()> {
        let _ = self
            .limiter
            .until_n_ready(std::num::NonZeroU32::new(n).unwrap())
            .await;
        Ok(())
    }

    async fn release(&self, _n: u32) {}

    async fn get_unit_type(&self) -> UnitType {
        self.unit_type.clone()
    }
}
