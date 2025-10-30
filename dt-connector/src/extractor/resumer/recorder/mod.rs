use async_trait::async_trait;

use dt_common::meta::position::Position;

pub mod to_database;

#[async_trait]
pub trait Recorder {
    async fn record_position(&self, position: &Position) -> anyhow::Result<()>;
}
