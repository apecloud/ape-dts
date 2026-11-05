-- The repeated ordinary prefix exceeds batch_size=2 and cannot be a pagination key.
INSERT INTO snapshot_expression_uk.mixed_rows VALUES (1, NULL, 'null_code'), (1, 'code1', 'first'), (1, 'code2', 'second'), (2, NULL, 'next_id');
INSERT INTO snapshot_expression_uk.primary_rows VALUES (1, 'code1', 'first'), (2, 'code2', 'second');
INSERT INTO snapshot_expression_uk.ordinary_rows VALUES (1, 'code1', 'first'), (2, 'code2', 'second');
