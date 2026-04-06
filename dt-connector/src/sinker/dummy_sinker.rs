use async_trait::async_trait;

use crate::{sinker::check_sinker::CheckWrappedSink, Sinker};
use dt_common::meta::row_data::RowData;

pub struct DummySinker {}

#[async_trait]
impl Sinker for DummySinker {}

#[async_trait]
impl CheckWrappedSink for DummySinker {
    async fn sink_dml_borrowed(
        &mut self,
        _data: &mut [RowData],
        _batch: bool,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
