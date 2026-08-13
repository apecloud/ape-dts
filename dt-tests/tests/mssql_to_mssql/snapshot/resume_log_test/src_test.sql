INSERT INTO resume_log_test.resume_rows VALUES
    (1, N'one'), (2, N'two'), (3, N'three'), (4, N'four'), (5, N'five');
INSERT INTO resume_log_test.config_rows VALUES
    (1, N'one'), (2, N'two'), (3, N'three'), (4, N'four');
INSERT INTO resume_log_test.composite_rows VALUES
    (1, 1, N'one-one'), (1, 2, N'one-two'),
    (2, 1, N'two-one'), (2, 2, N'two-two');
INSERT INTO resume_log_test.binary_key_rows VALUES
    (0x00FF, N'binary-one'), (0x0102, N'binary-two'),
    (0xE4BDA0E5A5BD, N'UTF-8 bytes'), (0xFFFFFFFF, N'binary-max');
INSERT INTO resume_log_test.[resume table.*] VALUES
    (1, N'special-one'), (2, N'special-two'), (3, N'special-three');
INSERT INTO resume_log_test.nullable_composite_unique_rows VALUES
    (1, 1, N'a', N'one-a'), (2, 1, N'b', N'one-b'),
    (3, NULL, N'a', N'null-a'), (4, NULL, N'a', N'null-a-duplicate'),
    (5, 2, NULL, N'two-null'), (6, NULL, NULL, NULL);
INSERT INTO resume_log_test.string_key_rows VALUES
    (N'a', 0x01), (N'b', 0x02), (N'c', 0x03),
    (N'd', 0xE4BDA0E5A5BD), (N'z', NULL);
INSERT INTO resume_log_test.date_key_rows VALUES
    ('0001-01-01', N'date-min'), ('2024-01-01', N'date-one'),
    ('2024-02-29', N'leap-day'), ('2026-08-12', N'today'), ('9999-12-31', NULL);
INSERT INTO resume_log_test.no_key_rows VALUES
    (1, N'one'), (NULL, N'null-key'), (2, NULL), (1, N'one');
INSERT INTO resume_log_test.fresh_rows VALUES
    (-2147483648, N'fresh-min'), (0, N''), (2147483647, NULL);
INSERT INTO resume_log_test.finished_config_rows VALUES
    (1, N'skipped-config'), (2, N'skipped-config-two');
INSERT INTO resume_log_test.finished_log_rows VALUES
    (1, N'skipped-log'), (2, N'skipped-log-two');
GO
