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
INSERT INTO resume_log_test.fresh_rows VALUES
    (1, N'fresh-one'), (2, N'fresh-two');
INSERT INTO resume_log_test.finished_config_rows VALUES
    (1, N'skipped-config'), (2, N'skipped-config-two');
INSERT INTO resume_log_test.finished_log_rows VALUES
    (1, N'skipped-log'), (2, N'skipped-log-two');
GO
