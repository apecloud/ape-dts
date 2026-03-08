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

use crate::{merge_parallelizer::MergeParallelizer, DataSize, Parallelizer};

enum CheckedInner {
    Generic(Box<dyn Parallelizer + Send + Sync>),
    Merge(MergeParallelizer),
}

impl CheckedInner {
    fn as_parallelizer(&self) -> &(dyn Parallelizer + Send + Sync) {
        match self {
            Self::Generic(inner) => inner.as_ref(),
            Self::Merge(inner) => inner,
        }
    }

    fn as_parallelizer_mut(&mut self) -> &mut (dyn Parallelizer + Send + Sync) {
        match self {
            Self::Generic(inner) => inner.as_mut(),
            Self::Merge(inner) => inner,
        }
    }

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<(DataSize, Vec<RowData>)> {
        match self {
            Self::Generic(inner) => inner
                .sink_dml(data, sinkers)
                .await
                .map(|size| (size, Vec::new())),
            Self::Merge(inner) => inner.sink_dml_checked(data, sinkers).await,
        }
    }
}

pub struct CheckedParallelizer {
    inner: CheckedInner,
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
            inner: CheckedInner::Generic(inner),
            checker,
            fail_on_checker_error,
            last_checker_ok: AtomicBool::new(true),
        }
    }

    pub fn new_merge(
        inner: MergeParallelizer,
        checker: CheckerHandle,
        fail_on_checker_error: bool,
    ) -> Self {
        Self {
            inner: CheckedInner::Merge(inner),
            checker,
            fail_on_checker_error,
            last_checker_ok: AtomicBool::new(true),
        }
    }

    fn handle_checker_result(&self, result: anyhow::Result<()>) -> anyhow::Result<()> {
        match result {
            Ok(()) => Ok(()),
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
        self.inner.as_parallelizer().get_name()
    }

    async fn drain(&mut self, buffer: &DtQueue) -> anyhow::Result<Vec<DtItem>> {
        self.inner.as_parallelizer_mut().drain(buffer).await
    }

    async fn sink_ddl(
        &mut self,
        data: Vec<DdlData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        self.inner
            .as_parallelizer_mut()
            .sink_ddl(data, sinkers)
            .await
    }

    async fn sink_dcl(
        &mut self,
        data: Vec<DclData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        self.inner
            .as_parallelizer_mut()
            .sink_dcl(data, sinkers)
            .await
    }

    async fn sink_raw(
        &mut self,
        data: Vec<DtItem>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        self.inner
            .as_parallelizer_mut()
            .sink_raw(data, sinkers)
            .await
    }

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        let (data_size, check_data) = self.inner.sink_dml(data, sinkers).await?;
        if !check_data.is_empty() {
            self.handle_checker_result(self.checker.check_rows_sync(Arc::new(check_data)).await)?;
        }
        Ok(data_size)
    }

    async fn sink_struct(
        &mut self,
        data: Vec<StructData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        let check_data = data.clone();
        let data_size = self
            .inner
            .as_parallelizer_mut()
            .sink_struct(data, sinkers)
            .await?;
        if !check_data.is_empty() {
            self.handle_checker_result(self.checker.check_struct(check_data).await)?;
        }
        Ok(data_size)
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.checker.close().await?;
        self.inner.as_parallelizer_mut().close().await
    }

    async fn close_with_position(&mut self, position: Option<&Position>) -> anyhow::Result<()> {
        let checker_position = if self.checker_last_ok() {
            position
        } else {
            None
        };
        self.checker.close_with_position(checker_position).await?;
        self.inner.as_parallelizer_mut().close().await
    }

    async fn record_checkpoint(&self, position: &Position) -> anyhow::Result<()> {
        if !self.checker_last_ok() {
            anyhow::bail!("skip checker checkpoint because previous checker run failed")
        }
        self.checker.record_checkpoint(position).await?;
        self.last_checker_ok.store(true, Ordering::Release);
        Ok(())
    }

    fn has_checker(&self) -> bool {
        true
    }

    fn checker_last_ok(&self) -> bool {
        self.last_checker_ok.load(Ordering::Acquire)
    }
}
