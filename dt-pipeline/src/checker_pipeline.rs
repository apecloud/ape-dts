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
    pub async fn new(mut inner: BasePipeline, checker: CheckerHandle) -> Self {
        let metrics = inner.monitor.sinker_worker_metrics();
        let raw = checker.is_raw().await;
        let mut sinkers = Vec::new();
        for id in checker.sink_ids().await {
            let check_sinker = CheckerSinker {
                checker: checker.clone(),
                raw,
                id,
            };
            let wrapped =
                BusyTrackingSinker::new(Box::new(check_sinker), metrics.register_worker());
            sinkers.push(std::sync::Arc::new(async_mutex::Mutex::new(
                Box::new(wrapped) as Box<dyn Sinker + Send>,
            )));
        }
        inner.sinkers = sinkers;
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
    raw: bool,
    id: String,
}

#[async_trait]
impl Sinker for CheckerSinker {
    fn get_id(&self) -> String {
        self.id.clone()
    }
    async fn sink_raw(
        &mut self,
        data: Vec<dt_common::meta::dt_data::DtItem>,
        _batch: bool,
    ) -> anyhow::Result<()> {
        self.checker.check_raw(data).await
    }

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
        if self.raw {
            return self.checker.close_with_position(position).await;
        }
        if let Err(err) = self.checker.close_with_position(position).await {
            log_warn!("standalone checker close failed: {}", err);
        }
        Ok(())
    }
}
