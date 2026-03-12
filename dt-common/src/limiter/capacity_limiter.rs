use async_trait::async_trait;
use tokio::sync::Semaphore;

use crate::limiter::limiter::{Limiter, UnitType};

pub struct CapacityLimiter {
    semaphore: Semaphore,
    unit_type: UnitType,
}

impl CapacityLimiter {
    pub fn new(capacity: usize, unit_type: UnitType) -> Self {
        Self {
            semaphore: Semaphore::new(capacity),
            unit_type,
        }
    }
}

#[async_trait]
impl Limiter for CapacityLimiter {
    async fn acquire(&self, n: u32) -> anyhow::Result<()> {
        let permit = self.semaphore.acquire_many(n).await?;
        permit.forget();
        Ok(())
    }

    async fn release(&self, n: u32) {
        self.semaphore.add_permits(n as usize);
    }

    async fn get_unit_type(&self) -> UnitType {
        self.unit_type.clone()
    }
}
