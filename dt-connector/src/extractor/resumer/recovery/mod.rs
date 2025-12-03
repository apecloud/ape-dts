use async_trait::async_trait;

use dashmap::DashMap;
use dt_common::meta::position::Position;

pub mod from_database;
pub mod from_log;

pub struct RecoverySnapshotCache {
    current_tb_positions: DashMap<DbTb, Position>,
    checkpoint_tb_positions: DashMap<DbTb, Position>,
    finished_tbs: DashMap<DbTb, bool>,
}

type DbTb = (String, String);

#[async_trait]
pub trait Recovery {
    async fn check_snapshot_finished(&self, schema: &str, tb: &str) -> bool;

    async fn get_snapshot_resume_position(
        &self,
        schema: &str,
        tb: &str,
        checkpoint: bool,
    ) -> Option<Position>;

    async fn get_cdc_resume_position(&self) -> Option<Position>;
}
