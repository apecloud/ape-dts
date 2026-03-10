use async_trait::async_trait;

use crate::{sinker::checked_sinker::CheckedSinkTarget, Sinker};
use dt_common::meta::row_data::RowData;

pub struct DummySinker {}

#[async_trait]
impl Sinker for DummySinker {}

#[async_trait]
impl CheckedSinkTarget for DummySinker {
    async fn sink_dml_borrowed(
        &mut self,
        _data: &mut [RowData],
        _batch: bool,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
