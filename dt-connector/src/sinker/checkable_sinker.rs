use async_trait::async_trait;
use dt_common::log_warn;
use dt_common::meta::{
    dcl_meta::dcl_data::DclData, ddl_meta::ddl_data::DdlData, dt_data::DtItem, row_data::RowData,
    struct_meta::struct_data::StructData,
};

use dt_common::meta::position::Position;

use crate::{checker::CheckerHandle, Sinker};

macro_rules! delegate_inner {
    ($self:ident, $method:ident($($arg:expr),*)) => {
        $self.inner.$method($($arg),*).await
    };
}

#[async_trait]
pub trait CheckableSink: Sinker {
    async fn sink_dml_borrowed(&mut self, data: &mut [RowData], batch: bool) -> anyhow::Result<()>;

    fn prepare_check_data(&self, data: Vec<RowData>) -> Vec<RowData> {
        data
    }
}

pub struct SinkerWithChecker<S> {
    inner: S,
    checker: CheckerHandle,
    lifecycle_owner: bool,
}

impl<S> SinkerWithChecker<S> {
    pub fn new(inner: S, checker: CheckerHandle, lifecycle_owner: bool) -> Self {
        Self {
            inner,
            checker,
            lifecycle_owner,
        }
    }
}

pub fn wrap_sinker_with_checker<S: CheckableSink + Send + 'static>(
    sinker: S,
    checker: Option<CheckerHandle>,
    lifecycle_owner: bool,
) -> Box<dyn Sinker + Send> {
    if let Some(checker) = checker {
        Box::new(SinkerWithChecker::new(sinker, checker, lifecycle_owner))
    } else {
        Box::new(sinker)
    }
}

#[async_trait]
impl<S: CheckableSink + Send> Sinker for SinkerWithChecker<S> {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, batch: bool) -> anyhow::Result<()> {
        self.inner.sink_dml_borrowed(&mut data, batch).await?;
        let data = self.inner.prepare_check_data(data);
        if let Err(err) = self.checker.check_dml(data, batch).await {
            log_warn!("checker check_dml failed: {}", err);
        }
        Ok(())
    }

    async fn sink_ddl(&mut self, data: Vec<DdlData>, batch: bool) -> anyhow::Result<()> {
        let checker_data = data.clone();
        self.inner.sink_ddl(data, batch).await?;
        if let Err(err) = self.checker.refresh_meta(checker_data).await {
            log_warn!("checker refresh_meta failed: {}", err);
        }
        Ok(())
    }

    async fn sink_dcl(&mut self, data: Vec<DclData>, batch: bool) -> anyhow::Result<()> {
        delegate_inner!(self, sink_dcl(data, batch))
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        delegate_inner!(self, close())
    }

    async fn close_with_position(&mut self, position: Option<&Position>) -> anyhow::Result<()> {
        self.inner.close_with_position(position).await?;
        if self.lifecycle_owner {
            if let Err(err) = self.checker.close_with_position(position).await {
                log_warn!("checker close failed: {}", err);
            }
        }
        Ok(())
    }

    async fn record_checkpoint(&mut self, position: &Position) -> anyhow::Result<()> {
        self.inner.record_checkpoint(position).await?;
        if self.lifecycle_owner {
            self.checker.record_checkpoint(position).await?;
        }
        Ok(())
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

    async fn handle_control_item(&mut self, item: &DtItem) -> anyhow::Result<()> {
        self.inner.handle_control_item(item).await?;
        if self.lifecycle_owner {
            self.checker.handle_control_item(item).await?;
        }
        Ok(())
    }

    fn get_id(&self) -> String {
        self.inner.get_id()
    }
}
