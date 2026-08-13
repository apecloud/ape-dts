INSERT INTO parallel_resume_log.integer_rows VALUES
    (1, N'v1'), (2, N'v2'), (3, N'v3'), (4, N'v4'),
    (5, N'v5'), (6, N'v6'), (7, N'v7'), (8, N'v8'),
    (9, N'v9'), (10, N'v10'), (11, N'v11'), (12, N'v12');
INSERT INTO parallel_resume_log.nullable_rows VALUES
    (1, 1, N'v1'), (2, 2, N'v2'), (3, 3, N'v3'),
    (4, 4, N'v4'), (5, 5, N'v5'), (6, 6, N'v6'),
    (7, 10, N'v10'), (8, 20, N'v20'), (9, 100, N'v100'),
    (10, NULL, N'null-1'), (11, NULL, N'null-2');
INSERT INTO parallel_resume_log.[string rows.*] VALUES
    (N'a', 0x01), (N'b', 0x02), (N'c', 0x03), (N'd', 0x04),
    (N'e', NULL), (N'f', 0x06), (N'g', 0x07), (N'中文', 0x08);
INSERT INTO parallel_resume_log.composite_rows VALUES
    (1, 1, N'one-one'), (1, 2, N'one-two'),
    (2, 1, N'two-one'), (2, 2, NULL),
    (3, 1, N'three-one'), (3, 2, N'three-two'),
    (4, 1, N'four-one'), (4, 2, N'four-two');
INSERT INTO parallel_resume_log.binary_rows VALUES
    (0x0001, N'binary-one'), (0x0002, N'binary-two'),
    (0x00FF, N'binary-ff'), (0x0100, N'binary-256'),
    (0x7FFFFFFF, N'binary-max-int'), (0xE4BDA0E5A5BD, N'UTF-8 bytes'),
    (0xFFFFFFFE, N'binary-near-max'), (0xFFFFFFFF, NULL);
INSERT INTO parallel_resume_log.decimal_rows VALUES
    (-999999999999.9999, N'decimal-min'), (-10.5000, N'decimal-negative'),
    (-0.0001, N'decimal-small-negative'), (0, N'zero'),
    (0.0001, N'decimal-small-positive'), (10.5000, N'decimal-positive'),
    (123456789012.3456, N'decimal-large'), (999999999999.9999, NULL);
INSERT INTO parallel_resume_log.date_rows VALUES
    ('0001-01-01', N'date-min'), ('2024-01-01', N'date-one'),
    ('2024-02-29', N'leap-day'), ('2025-01-01', N'date-two'),
    ('2026-08-12', N'date-three'), ('2038-01-19', N'unix-boundary'),
    ('9999-12-30', N'date-near-max'), ('9999-12-31', NULL);
INSERT INTO parallel_resume_log.no_key_rows VALUES
    (1, N'one'), (1, N'one-duplicate-key'), (2, N'two'), (3, NULL),
    (4, N'中文'), (5, N''), (6, N'six'), (7, N'seven');
INSERT INTO parallel_resume_log.unique_rows VALUES
    (1, -2147483648, N'min'), (2, -1, N'negative'), (3, 0, N'zero'),
    (4, 1, N'one'), (5, 2, N'two'), (6, 100, N'hundred'),
    (7, 2147483646, N'near-max'), (8, 2147483647, NULL);
INSERT INTO parallel_resume_log.position_log_rows VALUES
    (1, N'position-one'), (2, N'position-two'), (3, N'position-three'),
    (4, N'position-four'), (5, N'position-five'), (6, N'position-six'),
    (7, N'position-seven'), (8, NULL);
INSERT INTO parallel_resume_log.finished_rows VALUES
    (1, N'skipped-one'), (2, N'skipped-two');
INSERT INTO parallel_resume_log.finished_config_rows VALUES
    (1, N'skipped-config-one'), (2, N'skipped-config-two');
GO
