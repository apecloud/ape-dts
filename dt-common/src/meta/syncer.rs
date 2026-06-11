use std::collections::HashMap;

use crate::meta::position::Position;

#[derive(Default)]
pub struct Syncer {
    pub received_position: Position,
    pub committed_position: Position,
    pub committed_positions: HashMap<String, Position>,
}
