pub mod base_pipeline;
pub mod checker_pipeline;
pub mod component;
pub mod dependency_pipeline;

use async_trait::async_trait;

#[async_trait]
pub trait Pipeline {
    async fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
