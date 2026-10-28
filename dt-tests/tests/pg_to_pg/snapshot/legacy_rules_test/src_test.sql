INSERT INTO legacy_allowed.kept VALUES (1, 'schema include'), (2, 'schema include');
INSERT INTO legacy_allowed.blocked VALUES (1, 'table exclude wins'), (2, 'table exclude wins');
INSERT INTO legacy_denied.rescued VALUES (1, 'schema exclude wins'), (2, 'schema exclude wins');
INSERT INTO legacy_selected.kept VALUES (1, 'table include'), (2, 'table include');
INSERT INTO legacy_selected.unselected VALUES (1, 'not included'), (2, 'not included');
INSERT INTO legacy_shard_a.merge_left VALUES (1, 'left shard'), (2, 'left shard');
INSERT INTO legacy_shard_b.merge_right VALUES (1, 'right shard'), (2, 'right shard');
