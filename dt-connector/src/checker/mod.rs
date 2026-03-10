pub mod base_checker;
pub mod check_log;
pub mod log_reader;
pub mod mongo_checker;
pub mod mysql_checker;
pub mod pg_checker;
pub mod state_store;
pub mod struct_checker;

pub use base_checker::{
    has_null_key, CheckContext, Checker, CheckerTbMeta, DataCheckerHandle, FetchResult,
};
pub use mongo_checker::MongoChecker as MongoCheckerHandle;
pub use mysql_checker::MysqlChecker as MysqlCheckerHandle;
pub use pg_checker::PgChecker as PgCheckerHandle;
pub use state_store::{
    CheckerCheckpointBundle, CheckerStateRow, CheckerStateStore, CheckpointManifest,
};
pub use struct_checker::StructCheckerHandle;

pub enum CheckerHandle {
    Data(DataCheckerHandle),
    Struct(StructCheckerHandle),
}

impl CheckerHandle {
    pub async fn check_struct(
        &self,
        data: Vec<dt_common::meta::struct_meta::struct_data::StructData>,
    ) -> anyhow::Result<()> {
        match self {
            CheckerHandle::Data(_) => Ok(()),
            CheckerHandle::Struct(handle) => handle.check_struct(data).await,
        }
    }

    pub async fn close_with_position(
        &mut self,
        position: Option<&dt_common::meta::position::Position>,
    ) -> anyhow::Result<()> {
        match self {
            CheckerHandle::Data(handle) => handle.close_with_position(position).await,
            CheckerHandle::Struct(handle) => handle.close().await,
        }
    }

    pub async fn record_checkpoint(
        &self,
        position: &dt_common::meta::position::Position,
    ) -> anyhow::Result<()> {
        match self {
            CheckerHandle::Data(handle) => handle.record_checkpoint(position).await,
            CheckerHandle::Struct(_) => Ok(()),
        }
    }
}
