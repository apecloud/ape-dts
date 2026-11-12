use std::collections::HashMap;

use crate::{
    config::config_enums::DbType,
    meta::position::{aggregator::redis::RedisPositionAggregator, Position},
};

pub mod default;
pub mod redis;

pub trait PositionAggregator: Send + Sync {
    fn merge(&self, _previous: &Position, incoming: Position) -> Position {
        incoming
    }

    fn aggregate_received(
        &self,
        _positions: &HashMap<String, Position>,
        received: &Position,
    ) -> Position {
        received.clone()
    }

    fn aggregate_committed(
        &self,
        positions: &HashMap<String, Position>,
        received: &Position,
    ) -> Position;
}

pub fn position_aggregator(db_type: &DbType) -> Box<dyn PositionAggregator> {
    match db_type {
        DbType::Redis => Box::new(RedisPositionAggregator),
        _ => Box::new(default::DefaultPositionAggregator),
    }
}
