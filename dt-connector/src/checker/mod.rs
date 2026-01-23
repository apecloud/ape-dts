pub mod base_checker;
pub mod check_log;
pub mod log_reader;
pub mod mongo_checker;
pub mod mysql_checker;
pub mod pg_checker;
pub mod struct_checker;

use std::sync::Arc;

use dt_common::meta::{row_data::RowData, struct_meta::struct_data::StructData};

pub use base_checker::{
    has_null_key, CheckContext, Checker, CheckerTbMeta, DataCheckerHandle, FetchResult,
};
pub use mongo_checker::MongoChecker as MongoCheckerHandle;
pub use mysql_checker::MysqlChecker as MysqlCheckerHandle;
pub use pg_checker::PgChecker as PgCheckerHandle;
pub use struct_checker::StructCheckerHandle;

pub enum CheckerHandle {
    Data(DataCheckerHandle),
    Struct(StructCheckerHandle),
}

impl CheckerHandle {
    pub async fn check_rows(&self, data: Vec<Arc<RowData>>) -> anyhow::Result<()> {
        match self {
            CheckerHandle::Data(handle) => handle.check(data).await,
            CheckerHandle::Struct(_) => Ok(()),
        }
    }

    pub async fn check_struct(&self, data: Vec<StructData>) -> anyhow::Result<()> {
        match self {
            CheckerHandle::Struct(handle) => handle.check_struct(data).await,
            CheckerHandle::Data(_) => Ok(()),
        }
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        match self {
            CheckerHandle::Data(handle) => handle.close().await,
            CheckerHandle::Struct(handle) => handle.close().await,
        }
    }
}
