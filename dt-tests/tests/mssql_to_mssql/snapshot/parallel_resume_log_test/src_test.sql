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
INSERT INTO parallel_resume_log.finished_rows VALUES
    (1, N'skipped-one'), (2, N'skipped-two');
GO
