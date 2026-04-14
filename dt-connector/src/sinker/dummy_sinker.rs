use async_trait::async_trait;
use dt_common::meta::row_data::RowData;

use crate::{sinker::checkable_sinker::CheckableSink, Sinker};

pub struct DummySinker {}

#[async_trait]
impl Sinker for DummySinker {}

#[async_trait]
impl CheckableSink for DummySinker {
    async fn sink_dml_borrowed(
        &mut self,
        _data: &mut [RowData],
        _batch: bool,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
