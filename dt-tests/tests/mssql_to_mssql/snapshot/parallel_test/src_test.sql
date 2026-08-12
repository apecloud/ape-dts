INSERT INTO parallel_test.integer_rows VALUES
    (1, N'v1'), (2, N'v2'), (3, N'v3'), (4, N'v4'),
    (7, N'v7'), (9, N'v9'), (10, N'v10'), (11, N'v11'),
    (15, N'v15'), (20, N'v20'), (30, N'v30'), (50, N'v50');
INSERT INTO parallel_test.nullable_rows VALUES
    (1, NULL, N'null-1'), (2, NULL, N'null-2'),
    (3, 1, N'v1'), (4, 2, N'v2'), (5, 8, N'v8'),
    (6, 9, N'v9'), (7, 20, N'v20'), (8, 100, N'v100');
INSERT INTO parallel_test.where_rows VALUES
    (1, 10), (2, 20), (3, 30), (4, 40), (5, 50),
    (6, 60), (7, 70), (8, 80), (9, 90), (10, 100);
GO
