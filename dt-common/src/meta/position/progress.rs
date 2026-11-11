use std::collections::HashMap;

use super::{
    aggregator::{default::DefaultPositionAggregator, PositionAggregator},
    Position,
};

pub struct Progress {
    pub received_position: Position,
    pub committed_position: Position,
    pub committed_positions: HashMap<String, Position>,

    aggregator: Box<dyn PositionAggregator>,
}

impl Default for Progress {
    fn default() -> Self {
        Self::new(Box::new(DefaultPositionAggregator))
    }
}

impl Progress {
    pub fn new(aggregator: Box<dyn PositionAggregator>) -> Self {
        Self {
            received_position: Position::None,
            committed_position: Position::None,
            committed_positions: HashMap::new(),
            aggregator,
        }
    }

    pub fn update(
        &mut self,
        received: Option<Position>,
        commits: impl IntoIterator<Item = (String, Position)>,
    ) {
        for (key, position) in commits {
            if matches!(position, Position::None) {
                continue;
            }
            let previous = self.committed_positions.entry(key).or_default();
            *previous = self.aggregator.merge(previous, position);
        }
        if let Some(position) = received {
            self.received_position = self
                .aggregator
                .aggregate_received(&self.committed_positions, &position);
        }
    }

    pub fn aggregate_committed_position(&self) -> Position {
        self.aggregator
            .aggregate_committed(&self.committed_positions, &self.received_position)
    }
}
