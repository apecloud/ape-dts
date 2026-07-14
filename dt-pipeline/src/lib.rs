pub mod base_pipeline;
pub mod lua_processor;

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
