use async_trait::async_trait;

pub mod buffer_limiter;
pub mod capacity_limiter;
pub mod rate_limiter;

#[derive(Clone)]
pub enum UnitType {
    Bytes,
    Records,
}

#[async_trait]
pub trait Limiter {
    async fn acquire(&self, n: u32) -> anyhow::Result<()>;
    async fn release(&self, n: u32);
    async fn get_unit_type(&self) -> UnitType;
}
