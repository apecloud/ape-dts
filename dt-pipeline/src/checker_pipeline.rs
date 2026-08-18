use async_trait::async_trait;
use dt_common::{
    log_warn,
    meta::{position::Position, row_data::RowData, struct_meta::struct_data::StructData},
};
use dt_connector::{
    checker::CheckerHandle, sinker::busy_tracking_sinker::BusyTrackingSinker, Sinker,
};

use crate::{base_pipeline::BasePipeline, Pipeline};

pub struct CheckerPipeline {
    inner: BasePipeline,
}

impl CheckerPipeline {
    pub fn new(mut inner: BasePipeline, checker: CheckerHandle) -> Self {
        let metrics = inner.monitor.sinker_worker_metrics();
        let check_sinker = CheckerSinker { checker };
        let sinker_with_wrap =
            BusyTrackingSinker::new(Box::new(check_sinker), metrics.register_worker());
        inner.sinkers = vec![std::sync::Arc::new(async_mutex::Mutex::new(
            Box::new(sinker_with_wrap) as Box<dyn Sinker + Send>,
        ))];
        Self { inner }
    }
}

#[async_trait]
impl Pipeline for CheckerPipeline {
    async fn start(&mut self) -> anyhow::Result<()> {
        self.inner.start().await
    }

    async fn stop(&mut self) -> anyhow::Result<()> {
        self.inner.stop().await
    }
}

struct CheckerSinker {
    checker: CheckerHandle,
}

#[async_trait]
impl Sinker for CheckerSinker {
    async fn sink_dml(&mut self, data: Vec<RowData>, batch: bool) -> anyhow::Result<()> {
        if let Err(err) = self.checker.check_dml(data, batch).await {
            log_warn!("standalone checker check_dml failed: {}", err);
        }
        Ok(())
    }

    async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        self.checker.check_struct(data).await
    }

    async fn handle_control_item(
        &mut self,
        item: &dt_common::meta::dt_data::DtItem,
    ) -> anyhow::Result<()> {
        self.checker.handle_control_item(item).await
    }

    async fn close_with_position(&mut self, position: Option<&Position>) -> anyhow::Result<()> {
        if let Err(err) = self.checker.close_with_position(position).await {
            log_warn!("standalone checker close failed: {}", err);
        }
        Ok(())
    }
}
