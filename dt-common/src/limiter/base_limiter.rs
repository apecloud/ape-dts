use async_trait::async_trait;

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
