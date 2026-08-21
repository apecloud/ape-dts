INSERT INTO [ape_dts].key_structure.no_key_rows VALUES
    (1, N'one'), (NULL, N'null-key'), (2, NULL), (NULL, NULL), (1, N'one');
INSERT INTO [ape_dts].key_structure.single_primary_rows VALUES
    (-9223372036854775808, N'min', 0x00),
    (-1, N'negative', 0xDEADBEEF),
    (0, N'', 0x),
    (1, N'中文', 0xE4BDA0E5A5BD),
    (9223372036854775807, NULL, NULL);
INSERT INTO [ape_dts].key_structure.composite_primary_rows VALUES
    (-2147483648, -2147483648, N'min'),
    (1, 1, N'a'), (1, 2, NULL), (2, 1, N'b'), (2, 2, N'中文'),
    (2147483647, 2147483647, N'max');
INSERT INTO [ape_dts].key_structure.unique_rows VALUES
    (NULL, '11111111-1111-1111-1111-111111111111', N'alt-1', 10),
    (2, '22222222-2222-2222-2222-222222222222', N'alt-2', NULL),
    (3, '33333333-3333-3333-3333-333333333333', N'alt-3', -30),
    (4, 'ffffffff-ffff-ffff-ffff-ffffffffffff', N'中文-key', 2147483647),
    (5, '00000000-0000-0000-0000-000000000000', N'', -2147483648);
INSERT INTO [ape_dts].key_structure.primary_and_unique_rows VALUES
    (1, 1, '11111111-aaaa-bbbb-cccc-111111111111', N'alt-1', N'one'),
    (1, 2, '22222222-aaaa-bbbb-cccc-222222222222', NULL, N'null alternate'),
    (2, 1, '33333333-aaaa-bbbb-cccc-333333333333', N'alt-3', NULL),
    (2, 2, '44444444-aaaa-bbbb-cccc-444444444444', NULL, N'中文'),
    (2147483647, 2147483647, 'ffffffff-aaaa-bbbb-cccc-ffffffffffff', N'', N'max');
INSERT INTO [ape_dts].key_structure.composite_unique_rows VALUES
    (1, N'us-east', -2147483648, N'min'),
    (2, N'us-east', 0, N'zero'),
    (3, N'us-east', 2147483647, N'max'),
    (4, N'中国', 1, N'中文'),
    (5, N'', 1, NULL);
INSERT INTO [ape_dts].key_structure.nullable_composite_unique_rows VALUES
    (1, 1, N'a', N'complete'),
    (2, NULL, N'a', N'null-a-one'),
    (3, NULL, N'a', N'null-a-two'),
    (4, 1, NULL, N'null-b-one'),
    (5, 1, NULL, N'null-b-two'),
    (6, NULL, NULL, NULL),
    (7, -2147483648, N'中文', N'boundary');
INSERT INTO [ape_dts].key_structure.all_primary_rows VALUES
    (-2147483648, N'', 0x00),
    (0, N'a', 0x0001),
    (0, N'a', 0x0002),
    (1, N'中文', 0xE4BDA0E5A5BD),
    (2147483647, N'z', 0xFFFFFFFFFFFFFFFF);
INSERT INTO [ape_dts].key_structure.default_rows (id, nullable_value) VALUES
    (-2147483648, NULL), (1, N'explicit nullable');
INSERT INTO [ape_dts].key_structure.default_rows VALUES
    (2, N'explicit text', 0, N'value'),
    (2147483647, N'中文 default', -2147483648, N'boundary');
GO
