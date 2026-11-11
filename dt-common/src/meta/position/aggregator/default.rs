use std::collections::HashMap;

use super::{Position, PositionAggregator};

pub struct DefaultPositionAggregator;

impl PositionAggregator for DefaultPositionAggregator {
    fn aggregate_committed(
        &self,
        positions: &HashMap<String, Position>,
        received: &Position,
    ) -> Position {
        positions
            .iter()
            .filter(|(_, position)| !matches!(position, Position::None))
            .max_by(|left, right| left.0.cmp(right.0))
            .map(|(_, position)| position)
            .unwrap_or(received)
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::position::Progress;

    fn pg_position(lsn: &str, timestamp: &str) -> Position {
        Position::PgCdc {
            lsn: lsn.into(),
            timestamp: timestamp.into(),
        }
    }

    #[test]
    fn default_assembler_preserves_received_and_transaction_commit_positions() {
        let mut progress = Progress::default();
        assert_eq!(progress.aggregate_committed_position(), Position::None);
        let committed = pg_position("0/10", "2026-11-11 12:00:00.000");
        let received = pg_position("0/20", "2026-11-11 14:00:00.000");
        progress.update(
            Some(received.clone()),
            [("default_key".into(), committed.clone())],
        );
        assert_eq!(progress.received_position, received);
        assert_eq!(progress.aggregate_committed_position(), committed);
        let next = pg_position("0/30", "2026-11-11 15:00:00.000");
        progress.update(Some(next.clone()), [("default_key".into(), next.clone())]);
        assert_eq!(progress.aggregate_committed_position(), next);
    }

    #[test]
    fn default_assembler_keeps_partition_positions_and_ignores_empty_updates() {
        let mut progress = Progress::default();
        let first = Position::Kafka {
            topic: "topic".into(),
            partition: 0,
            offset: 10,
        };
        let second = Position::Kafka {
            topic: "topic".into(),
            partition: 1,
            offset: 20,
        };
        progress.update(Some(first.clone()), [("topic-0".into(), first.clone())]);
        progress.update(Some(second.clone()), [("topic-1".into(), second.clone())]);
        progress.update(None, [("topic-0".into(), Position::None)]);
        assert_eq!(progress.committed_positions["topic-0"], first);
        assert_eq!(progress.committed_positions["topic-1"], second);
        assert_eq!(progress.aggregate_committed_position(), second);
    }

    #[test]
    fn default_assembler_uses_received_before_the_first_commit() {
        let mut progress = Progress::default();
        let received = pg_position("0/20", "2026-11-11 14:00:00.000");
        progress.update(Some(received.clone()), []);
        assert_eq!(progress.received_position, received);
        assert_eq!(progress.aggregate_committed_position(), received);
    }
}
