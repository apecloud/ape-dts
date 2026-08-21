INSERT INTO [ape_dts].type_coverage.all_supported_types VALUES (
    1, 1, 255, -32768, -2147483648, -9223372036854775808,
    -123.25, 1.23456789012345E100, -214748.3648, 123456789012.3456,
    -1234567890123456789012345678901234.5678, 12345678901234.123456,
    'char', 'plain '' varchar', N'nchar', N'Unicode 中文 日本語 대한민국 😀',
    0x000102030405FEFF, 0xDEADBEEF, 'legacy text', N'legacy 中文', 0xCAFE0123,
    '550e8400-e29b-41d4-a716-446655440000',
    N'<root attr="value"><child>中文</child></root>',
    '0001-01-01', '23:59:59.9999999', '2024-02-29T23:59:00',
    '1753-01-01T00:00:00.000', '9999-12-31T23:59:59.9999999',
    '2026-08-12T12:34:56.1234567+08:00',
    N'{"string":"中文","number":1.25,"bool":true,"array":[1,null]}'
);
INSERT INTO [ape_dts].type_coverage.all_supported_types (
    id, bit_value, tinyint_value, smallint_value, int_value, bigint_value,
    real_value, float_value, smallmoney_value, money_value, decimal_value,
    numeric_value, char_value, varchar_value, nchar_value, nvarchar_value,
    binary_value, varbinary_value, text_value, ntext_value, image_value,
    uuid_value, xml_value, date_value, time_value, smalldatetime_value,
    datetime_value, datetime2_value, datetimeoffset_value, json_value
) VALUES (
    2, 0, 0, 32767, 2147483647, 9223372036854775807,
    0.0, -1.5E-100, 214748.3647, -123456789012.3456,
    0.0000, -99999999999999.999999,
    '', '', N'', N'', 0x, 0x, '', N'', 0x,
    '00000000-0000-0000-0000-000000000000', N'<empty />',
    '9999-12-31', '00:00:00', '1900-01-01T00:00:00',
    '9999-12-31T23:59:59.997', '0001-01-01T00:00:00',
    '2026-08-12T04:34:56.1234567+00:00', N'{}'
);
INSERT INTO [ape_dts].type_coverage.all_supported_types (id) VALUES (3);
INSERT INTO [ape_dts].type_coverage.all_supported_types VALUES (
    4, 1, 42, -123, 123456, -123456789012345,
    12.5, -98765.4321, 123.4567, -123456789.1234,
    1234567890123456789012345678901234.5678, 12345.678901,
    'fixed', 'quotes '' and symbols !@#', N'宽字符', N'Mixed 中文 日本語',
    0x4142434445464748, 0x000102030405FEFF, 'legacy plain text', N'legacy Unicode 中文',
    0x0123456789ABCDEF, '12345678-1234-5678-90ab-1234567890ab',
    N'<root><child id="4">text &amp; 中文</child></root>',
    '2024-02-29', '04:05:06.1234567', '2024-02-29T04:05:00',
    '2024-02-29T04:05:06.123', '2024-02-29T04:05:06.1234567',
    '2024-02-29T12:05:06.1234567+08:00',
    N'{"id":4,"string":"中文","nested":{"ok":true},"array":[1,2,3]}'
);
GO
