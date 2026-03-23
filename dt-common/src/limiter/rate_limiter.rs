use async_trait::async_trait;
use governor;

use crate::{
    limiter::{Limiter, UnitType},
    log_error, log_warn,
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
        let num = if let Some(num) = std::num::NonZeroU32::new(n) {
            num
        } else {
            log_warn!("Trying to acquire 0 from rate limiter, which means no acquire. Ignoring.");
            return Ok(());
        };
        match self.limiter.until_n_ready(num).await {
            Ok(_) => {}
            Err(e) => {
                let error_msg = format!(
                    "`{}` exceeds max capacity `{}` of the rate limiter: {}",
                    n, self.capacity, e
                );
                log_error!("{}", error_msg);
                return Err(anyhow::anyhow!(error_msg));
            }
        }
        Ok(())
    }

    async fn release(&self, _n: u32) {}

    async fn get_unit_type(&self) -> UnitType {
        self.unit_type.clone()
    }
}
