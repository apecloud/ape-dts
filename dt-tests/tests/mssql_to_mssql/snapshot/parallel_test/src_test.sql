INSERT INTO [ape_dts].parallel_test.integer_rows VALUES
    (1, 1), (2, 2), (3, 3), (7, 7), (9, 9), (10, 10), (11, 11),
    (12, 12), (14, 14), (16, 16), (17, 17), (18, 18), (19, 19);
INSERT INTO [ape_dts].parallel_test.integer_more_rows VALUES
    (1, 1), (2, 2), (3, 3), (7, 7), (9, 9), (10, 10), (11, 11),
    (12, 12), (14, 14), (16, 16), (17, 17), (18, 18), (19, 19),
    (20, 20), (21, 21), (22, 22), (23, 23), (24, 24), (25, 25),
    (26, 26), (27, 27), (28, 28), (29, 29), (30, 30);
INSERT INTO [ape_dts].parallel_test.string_rows VALUES
    (N'1', 1), (N'2', 2), (N'3', 3), (N'7', 7), (N'9', 9),
    (N'10', 10), (N'11', 11), (N'12', 12), (N'14', 14),
    (N'16', 16), (N'17', 17), (N'18', 18), (N'中文', 19);
INSERT INTO [ape_dts].parallel_test.no_key_rows VALUES
    (1, 1), (2, 2), (3, 3), (7, 7), (9, 9), (10, 10), (11, 11),
    (12, 12), (14, 14), (16, 16), (17, 17), (18, 18), (19, 19);
INSERT INTO [ape_dts].parallel_test.nullable_partition_rows VALUES
    (1, 1, 1), (2, 2, 2), (3, 3, 3), (7, 7, 7), (9, 9, 9),
    (10, 10, 10), (11, 11, 11), (12, 12, 12), (14, 14, 14),
    (16, 16, 16), (17, 17, 17), (18, 18, 18), (19, 19, 19),
    (100, NULL, 100), (200, NULL, 200);
INSERT INTO [ape_dts].parallel_test.unique_rows VALUES
    (1, 1, 1), (2, 2, 2), (3, 3, 3), (7, 7, 7), (9, 9, 9),
    (10, 10, 10), (11, 11, 11), (12, 12, 12), (14, 14, 14),
    (16, 16, 16), (17, 17, 17), (18, 18, 18), (19, 19, 19),
    (100, NULL, 100);
INSERT INTO [ape_dts].parallel_test.all_null_rows VALUES
    (NULL, NULL), (NULL, NULL), (NULL, NULL), (NULL, NULL),
    (NULL, NULL), (NULL, NULL), (NULL, NULL), (NULL, NULL),
    (NULL, NULL), (NULL, NULL), (NULL, NULL), (NULL, NULL);
INSERT INTO [ape_dts].parallel_test.where_condition_1 VALUES
    (1, 1), (2, 2), (3, 3), (4, 4), (5, 5),
    (6, 6), (7, 7), (8, 8), (9, 9), (10, 10);
INSERT INTO [ape_dts].parallel_test.where_condition_2 VALUES
    (1, 1), (2, 2), (3, 3), (4, 4), (5, 5),
    (6, 6), (7, 7), (8, 8), (9, 9), (10, 10);
INSERT INTO [ape_dts].parallel_test.fallback_no_key_rows VALUES
    (1, 1), (1, 1), (1, 1), (1, 1), (1, 1), (1, 1), (1, 1),
    (1, 1), (1, 1), (1, 1), (1, 1), (1, 1), (1, 1), (1, 1),
    (1, 1), (1, 1), (1, 1), (1, 1), (1, 1), (1, 1), (1, 1),
    (1, 1), (1, 1), (1, 1), (1, 1), (1, 1), (1, 1), (1, 1), (1, 1);
INSERT INTO [ape_dts].parallel_test.fallback_primary_rows VALUES
    (1, 1), (2, 2), (3, 3), (7, 7), (9, 9), (10, 10);
INSERT INTO [ape_dts].parallel_test.bigint_rows VALUES
    (-9223372036854775808, N'min'), (-10000000000, N'negative'), (-1, N'minus one'),
    (0, N'zero'), (1, N'one'), (10000000000, N'positive'),
    (9223372036854775806, N'near max'), (9223372036854775807, N'max');
INSERT INTO [ape_dts].parallel_test.decimal_rows VALUES
    (-9999999999999999.9999, N'min'), (-123456789.1234, N'negative'),
    (-0.0001, N'negative fraction'), (0.0000, N'zero'), (0.0001, N'fraction'),
    (123456789.1234, N'positive'), (9999999999999999.9998, N'near max'),
    (9999999999999999.9999, N'max');
INSERT INTO [ape_dts].parallel_test.date_rows VALUES
    ('0001-01-01', N'min'), ('1900-01-01', N'1900'), ('1969-12-31', N'pre epoch'),
    ('1970-01-01', N'epoch'), ('2000-02-29', N'leap'), ('2024-02-29', N'leap 2024'),
    ('9999-12-30', N'near max'), ('9999-12-31', N'max');
INSERT INTO [ape_dts].parallel_test.binary_rows VALUES
    (0x00000000, N'zero'), (0x00000001, N'one'), (0x00000002, N'two'),
    (0x000000FF, N'255'), (0x00000100, N'256'), (0x7FFFFFFF, N'positive'),
    (0xE4BDA0E5A5BD0000, N'UTF-8'), (0xFFFFFFFF, N'max bytes');
INSERT INTO [ape_dts].parallel_test.guid_rows VALUES
    ('00000000-0000-0000-0000-000000000000', N'zero'),
    ('00000000-0000-0000-0000-000000000001', N'one'),
    ('11111111-1111-1111-1111-111111111111', N'ones'),
    ('550e8400-e29b-41d4-a716-446655440000', N'RFC example'),
    ('7fffffff-ffff-ffff-ffff-ffffffffffff', N'middle'),
    ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', N'a'),
    ('ffffffff-ffff-ffff-ffff-fffffffffffe', N'near max'),
    ('ffffffff-ffff-ffff-ffff-ffffffffffff', N'max');
INSERT INTO [ape_dts].parallel_test.datetimeoffset_rows VALUES
    ('0001-01-01T00:00:00+00:00', N'min'),
    ('1900-01-01T00:00:00+00:00', N'1900'),
    ('1969-12-31T23:59:59.9999999+00:00', N'pre epoch'),
    ('1970-01-01T00:00:00+00:00', N'epoch'),
    ('2000-02-29T12:34:56.1234567+08:00', N'leap'),
    ('2024-02-29T23:59:59.9999999-08:00', N'leap 2024'),
    ('9999-12-30T23:59:59.9999999+00:00', N'near max'),
    ('9999-12-31T23:59:59.9999999+00:00', N'max');
INSERT INTO [ape_dts].parallel_test.composite_rows VALUES
    (-1, 1, N'negative tenant'), (0, 0, N'zero'),
    (1, 1, N'one-one'), (1, 2, N'one-two'),
    (2, 1, N'two-one'), (2, 2, NULL),
    (2147483647, 1, N'max tenant'), (2147483647, 2, N'last');
GO
