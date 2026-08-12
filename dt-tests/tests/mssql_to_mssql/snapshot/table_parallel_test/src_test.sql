INSERT INTO table_parallel_test.identity_rows (value) VALUES
    (N'i1'), (N'i2'), (N'i3'), (N'i4'), (N'i5');
INSERT INTO table_parallel_test.regular_rows VALUES
    (1, 1.250000), (2, NULL), (3, -3.500000), (4, 999.999999);
INSERT INTO table_parallel_test.string_key_rows VALUES
    (N'a', 0x0102), (N'b', NULL), (N'中文', 0xCAFE);
GO
