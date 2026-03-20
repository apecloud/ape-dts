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
}

impl<S> CheckedSinker<S> {
    pub fn new(inner: S, checker: DataCheckerHandle) -> Self {
        Self { inner, checker }
    }
}

pub fn wrap_checked_dml_sinker<S: CheckedSinkTarget + Send + 'static>(
    sinker: S,
    checker: Option<DataCheckerHandle>,
) -> Box<dyn Sinker + Send> {
    if let Some(checker) = checker {
        Box::new(CheckedSinker::new(sinker, checker))
    } else {
        Box::new(sinker)
    }
}

#[async_trait]
impl<S: CheckedSinkTarget + Send> Sinker for CheckedSinker<S> {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, batch: bool) -> anyhow::Result<()> {
        self.inner.sink_dml_borrowed(&mut data, batch).await?;
        self.checker.enqueue_check(data).await?;
        Ok(())
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
