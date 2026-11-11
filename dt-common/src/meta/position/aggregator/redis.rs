use std::collections::HashMap;

use super::PositionAggregator;
use crate::meta::position::{parse_timestamp, Position};

const NODE_KEY_PREFIX: &str = "redis-node-";

pub struct RedisPositionAggregator;

impl RedisPositionAggregator {
    pub fn node_key(node_id: &str) -> String {
        format!("{NODE_KEY_PREFIX}{node_id}")
    }

    pub fn register_nodes<'a>(
        positions: &mut HashMap<String, Position>,
        node_ids: impl IntoIterator<Item = &'a str>,
    ) {
        for node_id in node_ids {
            positions.entry(Self::node_key(node_id)).or_default();
        }
    }

    pub fn merge_timestamp(previous: &str, incoming: &mut String) {
        if parse_timestamp(previous) > parse_timestamp(incoming) {
            *incoming = previous.to_string();
        }
    }
}

impl PositionAggregator for RedisPositionAggregator {
    fn merge(&self, previous: &Position, mut incoming: Position) -> Position {
        if let (
            Position::Redis {
                node_id: Some(previous_id),
                timestamp: previous_time,
                ..
            },
            Position::Redis {
                node_id: Some(node_id),
                timestamp,
                ..
            },
        ) = (previous, &mut incoming)
        {
            if previous_id == node_id {
                Self::merge_timestamp(previous_time, timestamp);
            }
        }
        incoming
    }

    fn aggregate_received(
        &self,
        positions: &HashMap<String, Position>,
        received: &Position,
    ) -> Position {
        if matches!(
            received,
            Position::Redis {
                node_id: Some(_),
                ..
            }
        ) {
            return self.aggregate_committed(positions, received);
        }
        received.clone()
    }

    fn aggregate_committed(
        &self,
        positions: &HashMap<String, Position>,
        received: &Position,
    ) -> Position {
        if let Some(position) = positions
            .values()
            .find(|position| matches!(position, Position::Redis { node_id: None, .. }))
            .or_else(|| {
                matches!(received, Position::Redis { node_id: None, .. }).then_some(received)
            })
        {
            return position.clone();
        }
        positions
            .iter()
            .filter(|(key, _)| key.starts_with(NODE_KEY_PREFIX))
            .map(|(key, position)| (position.to_timestamp(), key, position))
            .min_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(right.1)))
            .filter(|(timestamp, _, _)| *timestamp != 0)
            .map(|(_, _, position)| position.clone())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::config_enums::DbType,
        meta::position::{aggregator::position_aggregator, Progress},
    };
    use std::str::FromStr;

    fn progress(node_ids: impl IntoIterator<Item = String>) -> Progress {
        let mut progress = Progress::new(position_aggregator(&DbType::Redis));
        let ids: Vec<_> = node_ids.into_iter().collect();
        RedisPositionAggregator::register_nodes(
            &mut progress.committed_positions,
            ids.iter().map(String::as_str),
        );
        progress
    }

    fn node_position(node_id: &str, offset: u64, timestamp: u64) -> Position {
        Position::Redis {
            node_id: Some(node_id.into()),
            address: Some(format!("{node_id}:6379")),
            repl_id: format!("repl-{node_id}"),
            repl_port: 10008,
            repl_offset: offset,
            now_db_id: 0,
            timestamp: if timestamp == 0 {
                String::new()
            } else {
                Position::format_timestamp_millis(timestamp as i64)
            },
        }
    }

    fn apply(progress: &mut Progress, positions: Vec<Position>) {
        let received = positions.last().cloned();
        progress.update(
            received,
            positions.into_iter().map(|position| {
                let Position::Redis {
                    node_id: Some(id), ..
                } = &position
                else {
                    panic!("expected node position")
                };
                (RedisPositionAggregator::node_key(id), position)
            }),
        );
    }

    #[test]
    fn redis_watermark_keeps_missing_nodes_across_batches() {
        let mut progress = progress((0..12).map(|id| id.to_string()));
        let noon = 1_794_312_000_000;
        let two_pm = noon + 7_200_000;
        apply(
            &mut progress,
            (0..11)
                .map(|id| node_position(&id.to_string(), 10, two_pm))
                .collect(),
        );
        assert_eq!(progress.received_position.to_timestamp(), 0);
        assert_eq!(
            progress.committed_positions["redis-node-11"],
            Position::None
        );
        for (offset, node_id, timestamp, expected) in [
            (11, "11", noon, noon),
            (12, "0", two_pm + 3_600_000, noon),
            (13, "11", noon + 3_600_000, noon + 3_600_000),
            (14, "11", noon, noon + 3_600_000),
            (15, "11", 0, noon + 3_600_000),
        ] {
            apply(
                &mut progress,
                vec![node_position(node_id, offset, timestamp)],
            );
            assert_eq!(progress.received_position.to_timestamp(), expected);
            assert_eq!(
                progress.aggregate_committed_position().to_timestamp(),
                expected
            );
            assert_eq!(progress.committed_positions.len(), 12);
            let Position::Redis { repl_offset, .. } =
                progress.committed_positions[&RedisPositionAggregator::node_key(node_id)]
            else {
                panic!("expected node offset")
            };
            assert_eq!(repl_offset, offset);
        }
    }

    #[test]
    fn redis_watermark_preserves_standalone_timestamp() {
        let mut progress = Progress::new(Box::new(RedisPositionAggregator));
        let position = Position::Redis {
            node_id: None,
            address: None,
            repl_id: "repl".into(),
            repl_port: 10008,
            repl_offset: 10,
            now_db_id: 0,
            timestamp: "2026-11-10 12:00:00.000".to_string(),
        };
        progress.update(
            Some(position.clone()),
            [("default_key".into(), position.clone())],
        );
        assert_eq!(progress.received_position, position);
        assert_eq!(progress.aggregate_committed_position(), position);
    }

    #[test]
    fn redis_watermark_restart_waits_for_each_recovered_stream() {
        let mut previous = progress(["fast".into(), "slow".into()]);
        apply(
            &mut previous,
            vec![
                node_position("fast", 10, 14_000),
                node_position("slow", 20, 12_000),
            ],
        );
        assert_eq!(previous.received_position.to_timestamp(), 12_000);
        let mut restarted = progress(["fast".into(), "slow".into()]);
        for (node, timestamp, expected) in [
            ("fast", 0, 0),
            ("fast", 15_000, 0),
            ("slow", 0, 0),
            ("slow", 11_000, 11_000),
            ("slow", 13_000, 13_000),
        ] {
            apply(&mut restarted, vec![node_position(node, 30, timestamp)]);
            assert_eq!(restarted.received_position.to_timestamp(), expected);
        }
        assert_eq!(
            previous.committed_positions["redis-node-slow"].to_timestamp(),
            12_000
        );
    }

    #[test]
    fn redis_watermark_new_topology_starts_unknown() {
        let mut progress = progress(["retained".into(), "new".into()]);
        apply(&mut progress, vec![node_position("retained", 10, 15_000)]);
        assert_eq!(progress.received_position.to_timestamp(), 0);
        apply(&mut progress, vec![node_position("new", 20, 13_000)]);
        assert_eq!(progress.received_position.to_timestamp(), 13_000);
        assert_eq!(progress.committed_positions.len(), 2);
    }

    #[test]
    fn redis_summary_selects_a_real_node_position() {
        let mut progress = progress(["fast".into(), "slow".into()]);
        let slow = node_position("slow", 200, 12_000);
        apply(
            &mut progress,
            vec![node_position("fast", 100, 14_000), slow.clone()],
        );
        apply(&mut progress, vec![node_position("fast", 101, 15_000)]);
        assert_eq!(progress.received_position, slow);
        assert_eq!(progress.aggregate_committed_position(), slow);
        assert_eq!(
            progress.committed_positions["redis-node-fast"],
            node_position("fast", 101, 15_000)
        );
        let json = progress.received_position.to_string();
        assert!(serde_json::from_str::<serde_json::Value>(&json).unwrap()["timestamp"].is_string());
        assert_eq!(Position::from_str(&json).unwrap(), slow);
    }

    #[test]
    fn redis_committed_assembly_uses_the_map_without_a_received_hint() {
        let slow = node_position("slow", 200, 12_000);
        let positions = HashMap::from([
            (
                RedisPositionAggregator::node_key("fast"),
                node_position("fast", 100, 14_000),
            ),
            (RedisPositionAggregator::node_key("slow"), slow.clone()),
        ]);
        assert_eq!(
            RedisPositionAggregator.aggregate_committed(&positions, &Position::None),
            slow
        );
        let mut unready = positions;
        unready.insert(RedisPositionAggregator::node_key("unknown"), Position::None);
        assert_eq!(
            RedisPositionAggregator.aggregate_committed(&unready, &slow),
            Position::None
        );
    }

    #[test]
    fn redis_standalone_commit_keeps_offset_without_a_heartbeat() {
        let mut position = node_position("standalone", 123, 0);
        if let Position::Redis { node_id, .. } = &mut position {
            *node_id = None;
        }
        let positions = HashMap::from([("default_key".to_string(), position.clone())]);
        assert_eq!(
            RedisPositionAggregator.aggregate_committed(&positions, &Position::None),
            position
        );
        assert_eq!(
            RedisPositionAggregator.aggregate_committed(&HashMap::new(), &position),
            position
        );
    }

    #[test]
    fn redis_summary_uses_a_stable_node_for_equal_timestamps() {
        let mut progress = progress(["a".into(), "b".into()]);
        let first = node_position("a", 100, 12_000);
        apply(
            &mut progress,
            vec![first.clone(), node_position("b", 200, 12_000)],
        );
        assert_eq!(progress.received_position, first);
        assert_eq!(progress.aggregate_committed_position(), first);
    }

    #[test]
    fn redis_position_keeps_legacy_string_serialization() {
        let legacy = r#"{"type":"Redis","repl_id":"repl-1","repl_port":10008,"repl_offset":123,"now_db_id":0,"timestamp":"2026-11-10 12:00:00.000"}"#;
        let position = Position::from_str(legacy).unwrap();
        assert_eq!(position.to_string(), legacy);
        assert!(position.to_timestamp() > 0);
        for (timestamp, expected) in [("", 0), ("invalid", 0), ("1970-01-01 00:00:12.000", 12_000)]
        {
            let mut position = position.clone();
            if let Position::Redis {
                timestamp: value, ..
            } = &mut position
            {
                *value = timestamp.to_string();
            }
            assert_eq!(position.to_timestamp(), expected);
            assert_eq!(
                Position::from_log(&format!(
                    "2026-11-11 14:00:00 | current_position | {position}"
                )),
                position
            );
        }
    }
}
