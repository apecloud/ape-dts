use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use async_trait::async_trait;
use dt_common::meta::{
    dcl_meta::dcl_data::DclData, ddl_meta::ddl_data::DdlData, dt_data::DtItem, dt_queue::DtQueue,
    position::Position, row_data::RowData, struct_meta::struct_data::StructData,
};
use dt_connector::{checker::CheckerHandle, Sinker};

use crate::{DataSize, Parallelizer};

pub struct CheckedParallelizer {
    inner: Box<dyn Parallelizer + Send + Sync>,
    checker: CheckerHandle,
    fail_on_checker_error: bool,
    last_checker_ok: AtomicBool,
}

impl CheckedParallelizer {
    pub fn new(
        inner: Box<dyn Parallelizer + Send + Sync>,
        checker: CheckerHandle,
        fail_on_checker_error: bool,
    ) -> Self {
        Self {
            inner,
            checker,
            fail_on_checker_error,
            last_checker_ok: AtomicBool::new(true),
        }
    }

    fn handle_checker_result(&self, result: anyhow::Result<()>) -> anyhow::Result<()> {
        match result {
            Ok(()) => {
                self.last_checker_ok.store(true, Ordering::Release);
                Ok(())
            }
            Err(err) => {
                self.last_checker_ok.store(false, Ordering::Release);
                log::warn!("checker sidecar failed: {}", err);
                if self.fail_on_checker_error {
                    Err(err)
                } else {
                    Ok(())
                }
            }
        }
    }
}

#[async_trait]
impl Parallelizer for CheckedParallelizer {
    fn get_name(&self) -> String {
        self.inner.get_name()
    }

    async fn drain(&mut self, buffer: &DtQueue) -> anyhow::Result<Vec<DtItem>> {
        self.inner.drain(buffer).await
    }

    async fn sink_ddl(
        &mut self,
        data: Vec<DdlData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        self.inner.sink_ddl(data, sinkers).await
    }

    async fn sink_dcl(
        &mut self,
        data: Vec<DclData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        self.inner.sink_dcl(data, sinkers).await
    }

    async fn sink_raw(
        &mut self,
        data: Vec<DtItem>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        self.inner.sink_raw(data, sinkers).await
    }

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        let check_data = Arc::new(data.clone());
        let data_size = self.inner.sink_dml(data, sinkers).await?;
        if !check_data.is_empty() {
            self.handle_checker_result(self.checker.check_rows_sync(check_data).await)?;
        } else {
            self.last_checker_ok.store(true, Ordering::Release);
        }
        Ok(data_size)
    }

    async fn sink_struct(
        &mut self,
        data: Vec<StructData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        let check_data = data.clone();
        let data_size = self.inner.sink_struct(data, sinkers).await?;
        if !check_data.is_empty() {
            self.handle_checker_result(self.checker.check_struct(check_data).await)?;
        } else {
            self.last_checker_ok.store(true, Ordering::Release);
        }
        Ok(data_size)
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.checker.close().await?;
        self.inner.close().await
    }

    async fn close_with_position(&mut self, position: Option<&Position>) -> anyhow::Result<()> {
        self.checker.close_with_position(position).await?;
        self.inner.close().await
    }

    async fn record_checkpoint(&self, position: &Position) -> anyhow::Result<()> {
        self.checker.record_checkpoint(position).await
    }

    fn has_checker(&self) -> bool {
        true
    }

    fn checker_last_ok(&self) -> bool {
        self.last_checker_ok.load(Ordering::Acquire)
    }
}
