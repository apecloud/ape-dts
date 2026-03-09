use async_trait::async_trait;
use dt_common::meta::{
    dcl_meta::dcl_data::DclData, ddl_meta::ddl_data::DdlData, dt_data::DtItem, row_data::RowData,
    struct_meta::struct_data::StructData,
};

use crate::{checker::DataCheckerHandle, Sinker};

macro_rules! delegate_inner {
    ($self:ident, $method:ident($($arg:expr),*)) => {
        $self.inner.$method($($arg),*).await
    };
}

#[async_trait]
pub trait CheckedSinkTarget: Sinker {
    async fn sink_dml_borrowed(&mut self, data: &mut [RowData], batch: bool) -> anyhow::Result<()>;
}

pub struct CheckedSinker<S> {
    inner: S,
    checker: DataCheckerHandle,
    fail_on_checker_error: bool,
}

impl<S> CheckedSinker<S> {
    pub fn new(inner: S, checker: DataCheckerHandle, fail_on_checker_error: bool) -> Self {
        Self {
            inner,
            checker,
            fail_on_checker_error,
        }
    }
}

pub fn wrap_checked_dml_sinker<S: CheckedSinkTarget + Send + 'static>(
    sinker: S,
    checker: Option<DataCheckerHandle>,
    fail_on_checker_error: bool,
) -> Box<dyn Sinker + Send> {
    if let Some(checker) = checker {
        Box::new(CheckedSinker::new(sinker, checker, fail_on_checker_error))
    } else {
        Box::new(sinker)
    }
}

#[async_trait]
impl<S: CheckedSinkTarget + Send> Sinker for CheckedSinker<S> {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, batch: bool) -> anyhow::Result<()> {
        self.inner.sink_dml_borrowed(&mut data, batch).await?;
        match self.checker.check_sync(data).await {
            Ok(()) => Ok(()),
            Err(err) if self.fail_on_checker_error => Err(err),
            Err(err) => {
                log::warn!("checker sidecar failed: {}", err);
                Ok(())
            }
        }
    }

    async fn sink_ddl(&mut self, data: Vec<DdlData>, batch: bool) -> anyhow::Result<()> {
        delegate_inner!(self, sink_ddl(data, batch))
    }

    async fn sink_dcl(&mut self, data: Vec<DclData>, batch: bool) -> anyhow::Result<()> {
        delegate_inner!(self, sink_dcl(data, batch))
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        delegate_inner!(self, close())
    }

    async fn sink_raw(&mut self, data: Vec<DtItem>, batch: bool) -> anyhow::Result<()> {
        delegate_inner!(self, sink_raw(data, batch))
    }

    async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        delegate_inner!(self, sink_struct(data))
    }

    async fn refresh_meta(&mut self, data: Vec<DdlData>) -> anyhow::Result<()> {
        delegate_inner!(self, refresh_meta(data))
    }

    fn get_id(&self) -> String {
        self.inner.get_id()
    }
}
