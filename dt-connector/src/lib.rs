#![allow(clippy::manual_range_contains)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::comparison_chain)]

pub mod checker;
pub mod conn_util;
pub mod data_marker;
pub mod extractor;
pub mod meta_fetcher;
pub mod rdb_query_builder;
pub mod rdb_router;
pub mod sinker;

use async_trait::async_trait;
use checker::check_log::CheckLog;
use dt_common::meta::{
    dcl_meta::dcl_data::DclData, ddl_meta::ddl_data::DdlData, dt_data::DtItem, row_data::RowData,
    struct_meta::struct_data::StructData,
};
use std::sync::Arc;

#[async_trait]
pub trait Sinker {
    async fn sink_dml(&mut self, mut _data: Vec<Arc<RowData>>, _batch: bool) -> anyhow::Result<()> {
        Ok(())
    }

    async fn sink_ddl(&mut self, mut _data: Vec<DdlData>, _batch: bool) -> anyhow::Result<()> {
        Ok(())
    }

    async fn sink_dcl(&mut self, mut _data: Vec<DclData>, _batch: bool) -> anyhow::Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn sink_raw(&mut self, mut _data: Vec<DtItem>, _batch: bool) -> anyhow::Result<()> {
        Ok(())
    }

    async fn sink_struct(&mut self, mut _data: Vec<StructData>) -> anyhow::Result<()> {
        Ok(())
    }

    async fn refresh_meta(&mut self, _data: Vec<DdlData>) -> anyhow::Result<()> {
        Ok(())
    }

    fn get_id(&self) -> String {
        String::new()
    }
}

#[async_trait]
pub trait Extractor {
    async fn extract(&mut self) -> anyhow::Result<()>;

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait BatchCheckExtractor {
    async fn batch_extract(&mut self, check_logs: &[CheckLog]) -> anyhow::Result<()>;
}
