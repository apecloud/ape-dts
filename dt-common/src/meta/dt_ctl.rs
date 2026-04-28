use serde::{Deserialize, Serialize};

use crate::meta::position::Position;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DtCtl {
    SnapshotExtractFinished {
        task_id: String,
        schema: String,
        tb: String,
        finish_position: Position,
    },
}

impl DtCtl {
    pub fn task_id(&self) -> &str {
        match self {
            DtCtl::SnapshotExtractFinished { task_id, .. } => task_id,
        }
    }
}
