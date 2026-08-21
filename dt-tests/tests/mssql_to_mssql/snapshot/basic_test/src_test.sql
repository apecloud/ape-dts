INSERT INTO [ape_dts].basic_test.basic_types
    (enabled, amount, name, note, payload, event_date, event_time, external_id) VALUES
    (1, 12.3400, N'basic', NULL, 0x000102FEFF, '2024-01-02',
        '2024-01-02T03:04:05.1234567', '11111111-1111-1111-1111-111111111111'),
    (0, -98.7654, N'Unicode 测试', N'quote '' value', 0xCAFE, '2024-02-29',
        '2024-02-29T23:59:59.9999999', '22222222-2222-2222-2222-222222222222'),
    (1, 0.0000, N'empty optionals', NULL, NULL, '2000-01-01',
        '2000-01-01T00:00:00', '33333333-3333-3333-3333-333333333333');

INSERT INTO [ape_dts].basic_test.no_pk_no_uk VALUES
    (1, -1, 123456, 1234567890123, 123456.1234, 3.25, 4.5, 1,
        '2022-01-02', '03:04:05.1234567', '2022-01-02T03:04:05.1234567',
        N'row one 中文', 0x0123456789ABCDEF, 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'),
    (255, 32767, 2147483647, 9223372036854775807, -654321.4321, -3.5, -4.75, 0,
        '9999-12-31', '23:59:59.9999999', '9999-12-31T23:59:59.9999999',
        N'row two 😀', 0xFEDCBA9876543210, 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'),
    (0, -32768, -2147483648, -9223372036854775808, 0, 0, 0, 0,
        '0001-01-01', '00:00:00', '0001-01-01T00:00:00', N'', 0x,
        '00000000-0000-0000-0000-000000000000'),
    (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL),
    (1, -1, 123456, 1234567890123, 123456.1234, 3.25, 4.5, 1,
        '2022-01-02', '03:04:05.1234567', '2022-01-02T03:04:05.1234567',
        N'row one 中文', 0x0123456789ABCDEF, 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa');

INSERT INTO [ape_dts].basic_test.one_pk_no_uk VALUES
    (1, 10, 1234.5000, N'one', 0x01, '2024-01-01T01:02:03.1234567'),
    (2, -20, -9999.9999, N'二', 0xCAFE, '2024-02-29T23:59:59.9999999'),
    (3, NULL, NULL, NULL, NULL, NULL),
    (4, 0, 0.0000, N'', 0x, '0001-01-01T00:00:00'),
    (5, 2147483647, 9999999999999999.9999, N'max', 0xFFFFFFFF, '9999-12-31T23:59:59.9999999'),
    (6, -2147483648, -9999999999999999.9999, N'min', 0x00, '1753-01-01T00:00:00');

INSERT INTO [ape_dts].basic_test.no_pk_one_uk VALUES
    (1, N'code-1', 1.1000, N'one'),
    (2, N'code-2', -2.2000, N'二'),
    (NULL, N'code-null-row', NULL, NULL),
    (4, N'', 0.0000, N''),
    (5, N'code-😀', 5.5000, N'emoji 😀'),
    (6, N'code-quote''', 6.6000, N'quote '' value');

INSERT INTO [ape_dts].basic_test.no_pk_multi_uk VALUES
    (1, N'code-1', '10000000-0000-0000-0000-000000000001', 1, 1, 0x01),
    (2, N'code-2', '10000000-0000-0000-0000-000000000002', 1, 2, 0x02),
    (3, N'code-3', '10000000-0000-0000-0000-000000000003', 2, 1, NULL),
    (NULL, N'code-4', '10000000-0000-0000-0000-000000000004', 2, 2, 0x),
    (-5, N'中文-5', '10000000-0000-0000-0000-000000000005', -1, 1, 0xCAFE),
    (6, N'code-6', '10000000-0000-0000-0000-000000000006', 3, 1, 0xFFFFFFFF);

INSERT INTO [ape_dts].basic_test.one_pk_multi_uk VALUES
    (1, N'pk-code-1', '20000000-0000-0000-0000-000000000001', 1, 1, N'one'),
    (2, N'pk-code-2', '20000000-0000-0000-0000-000000000002', 1, 2, N'two'),
    (3, N'pk-code-3', '20000000-0000-0000-0000-000000000003', 2, 1, NULL),
    (4, N'pk-code-4', '20000000-0000-0000-0000-000000000004', 2, 2, N''),
    (5, N'主键-5', '20000000-0000-0000-0000-000000000005', 3, 1, N'中文'),
    (6, N'pk-code-6', '20000000-0000-0000-0000-000000000006', 3, 2, N'last');

INSERT INTO [ape_dts].basic_test.numeric_table VALUES
    (1, 1, 255, -32768, -2147483648, -9223372036854775808,
        -123.25, 1.23456789012345E100, -214748.3648, 123456789012.3456,
        -1234567890123456789012345678901234.5678, 12345678901234.123456),
    (2, 0, 0, 32767, 2147483647, 9223372036854775807,
        123.25, -1.5E-100, 214748.3647, -123456789012.3456,
        1234567890123456789012345678901234.5678, -99999999999999.999999),
    (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO [ape_dts].basic_test.date_time_table VALUES
    (1, '0001-01-01', '00:00:00', '1900-01-01T00:00:00',
        '1753-01-01T00:00:00.000', '0001-01-01T00:00:00',
        '0001-01-01T00:00:00+00:00'),
    (2, '9999-12-31', '23:59:59.9999999', '2079-06-06T23:59:00',
        '9999-12-31T23:59:59.997', '9999-12-31T23:59:59.9999999',
        '9999-12-31T23:59:59.9999999+00:00'),
    (3, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO [ape_dts].basic_test.string_binary_table VALUES
    (1, 'char', 'plain '' varchar', N'nchar', N'Unicode 中文 日本語 대한민국 😀',
        N'line 1' + NCHAR(10) + N'line 2', 0x000102030405FEFF, 0xDEADBEEF,
        N'<root attr="value"><child>中文</child></root>',
        N'{"string":"中文","number":1.25,"bool":true,"array":[1,null]}'),
    (2, '', '', N'', N'', N'', 0x, 0x, N'<empty />', N'{}'),
    (3, 'trail', 'trailing spaces   ', N'固定', N'quote '' and "',
        REPLICATE(CONVERT(nvarchar(max), N'large-'), 100), 0xCAFE, 0x0000FF,
        N'<items><item id="1"/><item id="2"/></items>', N'[1,2,3]'),
    (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO [ape_dts].basic_test.[col has special character]
    ([p:k], [select], [col,2], [col.3], [col with space], [col]]5]) VALUES
    (1, N'reserved', N'comma', N'dot', N'space', N'bracket'),
    (2, N'中文', NULL, N'', N'quote '' value', N']'),
    (3, NULL, NULL, NULL, NULL, NULL);

INSERT INTO [ape_dts].basic_test.ignore_cols_1 VALUES (1, 1, 11, 111), (2, 2, 22, 222);
INSERT INTO [ape_dts].basic_test.ignore_cols_2 VALUES (1, 1, 11, 111), (2, 2, 22, 222);
INSERT INTO [ape_dts].[Upper_Case_DB].[Upper_Case_TB] VALUES
    (1, 1, 1, 1, 1), (2, 2, 2, 2, NULL);

INSERT INTO [ape_dts].basic_test.where_condition_1 VALUES
    (1, 1), (2, 2), (3, 3), (4, 4), (5, 5),
    (6, 6), (7, 7), (8, 8), (9, 9), (10, 10);
INSERT INTO [ape_dts].basic_test.where_condition_2 VALUES
    (1, 1), (2, 2), (3, 3), (4, 4), (5, 5),
    (6, 6), (7, 7), (8, 8), (9, 9), (10, 10);
INSERT INTO [ape_dts].basic_test.where_condition_3 VALUES
    (1, 1), (2, 2), (3, 3), (4, 4), (5, 5),
    (6, 6), (7, 7), (8, 8), (9, 9), (10, 10);

INSERT INTO [ape_dts].basic_test.fk_parent VALUES (1, 101, N'parent-1'), (2, 102, N'parent-2');
INSERT INTO [ape_dts].basic_test.fk_child VALUES (1, 101, N'child-1'), (2, 102, N'child-2');

INSERT INTO [ape_dts].basic_test.composite_pk_table VALUES
    (1, N'1', 1), (2, N'2', 2), (3, N'3', 3), (4, N'4', 4), (5, N'5', 5),
    (6, N'6', 6), (7, N'7', 7), (8, N'8', 8), (9, N'9', 9), (10, N'10', 10);
INSERT INTO [ape_dts].basic_test.composite_unique_key_table VALUES
    (1, N'1', 1), (2, N'2', 2), (3, N'3', 3), (4, N'4', 4), (5, N'5', 5),
    (6, N'6', 6), (7, N'7', 7), (8, N'8', 8), (9, N'9', 9), (10, N'10', 10);
INSERT INTO [ape_dts].basic_test.nullable_composite_unique_key_table VALUES
    (1, N'1', 1), (2, N'2', 2), (3, N'3', 3), (4, N'4', 4), (5, N'5', 5),
    (6, N'6', 6), (7, NULL, 7), (8, N'8', NULL), (9, NULL, NULL);
INSERT INTO [ape_dts].basic_test.multi_primary_and_single_unique_table VALUES
    (1, N'1', 1, N'u1', 1), (2, N'2', 2, N'u2', 2),
    (3, N'3', 3, N'u3', 3), (4, N'4', 4, N'u4', 4),
    (5, N'5', 5, N'u5', 5), (6, N'6', 6, N'u6', 6),
    (7, N'7', 7, N'u7', 7), (8, N'8', 8, N'u8', 8),
    (9, N'9', 9, N'u9', 9), (10, N'10', 10, N'u10', 10);
INSERT INTO [ape_dts].basic_test.all_pks VALUES
    (1, 2, 3), (4, 5, 6), (7, 8, 9), (10, 11, 12);

INSERT INTO [ape_dts].basic_test.tbl_1 VALUES
    (1, 'code1', N'name1'), (2, 'code2', N'name2'), (3, 'code3', NULL);
INSERT INTO [ape_dts].basic_test.tbl_2 VALUES
    ('code1', N'name1'), ('code1', N'name2'), (NULL, N'name3');
INSERT INTO [ape_dts].basic_test.tbl_3 VALUES
    (1, 'code1', N'name1'), (2, 'code2', N'name2'), (3, 'code3', NULL);
INSERT INTO [ape_dts].basic_test.tbl_4 VALUES
    ('code1', N'name1'), ('code2', N'name2'), ('code3', N'name3');
INSERT INTO [ape_dts].basic_test.tbl_5 VALUES
    ('code1', N'name1'), ('code1', N'name1'), (NULL, NULL);

SET IDENTITY_INSERT [ape_dts].basic_test.server_generated_cols ON;
INSERT INTO [ape_dts].basic_test.server_generated_cols (id, base_value) VALUES
    (10, 5), (42, -7), (100, 0);
SET IDENTITY_INSERT [ape_dts].basic_test.server_generated_cols OFF;

INSERT INTO [ape_dts].basic_test.timestamp_alias_cols (id, value) VALUES
    (1, N'first'), (2, N'中文'), (3, N'last');
GO
