use async_trait::async_trait;
use tokio::sync::Semaphore;

use crate::limiter::limiter::{Limiter, UnitType};

pub struct CapacityLimiter {
    semaphore: Semaphore,
    unit_type: UnitType,
}

impl CapacityLimiter {
    pub fn new(capacity: u32, unit_type: UnitType) -> Self {
        Self {
            semaphore: Semaphore::new(capacity as usize),
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

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::Duration;

    use tokio::sync::Barrier;

    use crate::limiter::limiter::Limiter;

    use super::CapacityLimiter;
    use super::UnitType;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn semaphore_limiter_limits_concurrent_tasks() {
        let limiter = Arc::new(CapacityLimiter::new(2, UnitType::Records));
        let barrier = Arc::new(Barrier::new(5));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let limiter = limiter.clone();
            let barrier = barrier.clone();
            let in_flight = in_flight.clone();
            let max_in_flight = max_in_flight.clone();

            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                limiter.acquire(1).await.unwrap();

                let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                loop {
                    let observed = max_in_flight.load(Ordering::SeqCst);
                    if current <= observed {
                        break;
                    }
                    if max_in_flight
                        .compare_exchange(observed, current, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        break;
                    }
                }

                tokio::time::sleep(Duration::from_millis(100)).await;

                in_flight.fetch_sub(1, Ordering::SeqCst);
                limiter.release(1).await;
            }));
        }

        barrier.wait().await;
        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(max_in_flight.load(Ordering::SeqCst), 2);
    }
}
