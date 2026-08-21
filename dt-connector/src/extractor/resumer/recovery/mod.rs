use async_trait::async_trait;
use dashmap::DashMap;
use dt_common::meta::position::Position;

pub mod from_database;
pub mod from_log;

pub struct RecoverySnapshotCache {
    current_tb_positions: DashMap<DbSchemaTb, Position>,
    checkpoint_tb_positions: DashMap<DbSchemaTb, Position>,
    finished_tbs: DashMap<DbSchemaTb, bool>,
}

type DbSchemaTb = (String, String, String);

#[async_trait]
pub trait Recovery {
    async fn check_snapshot_finished(&self, db: &str, schema: &str, tb: &str) -> bool;

    async fn get_snapshot_resume_position(
        &self,
        db: &str,
        schema: &str,
        tb: &str,
        checkpoint: bool,
    ) -> Option<Position>;

    async fn get_cdc_resume_position(&self) -> Option<Position>;

    async fn get_cdc_resume_positions(&self) -> Vec<Position> {
        self.get_cdc_resume_position().await.into_iter().collect()
    }
}
