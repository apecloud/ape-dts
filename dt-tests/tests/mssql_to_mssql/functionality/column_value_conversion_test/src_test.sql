USE [ape_dts_col_value_conversion_source];
GO

INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[bit_value] VALUES
    (1, 0), (2, 1), (3, 1), (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[tinyint_value] VALUES
    (1, 0), (2, 255), (3, 128), (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[smallint_value] VALUES
    (1, -32768), (2, 32767), (3, 0), (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[int_value] VALUES
    (1, -2147483648), (2, 2147483647), (3, 0), (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[bigint_value] VALUES
    (1, CONVERT(bigint, '-9223372036854775808')),
    (2, CONVERT(bigint, '9223372036854775807')),
    (3, 0),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[real_value] VALUES
    (1, CONVERT(real, '-3.402823466E+38')),
    (2, CONVERT(real, '3.402823466E+38')),
    (3, CONVERT(real, '1.175494351E-38')),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[float_value] VALUES
    (1, CONVERT(float, '-1.7976931348623157E+308')),
    (2, CONVERT(float, '1.7976931348623157E+308')),
    (3, CONVERT(float, '2.2250738585072014E-308')),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[smallmoney_value] VALUES
    (1, -214748.3648), (2, 214748.3647), (3, 0.0001), (4, NULL);
-- Tiberius maps money to f64, so use endpoint-adjacent values that retain four decimal places.
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[money_value] VALUES
    (1, CONVERT(money, '-922337203685477.5000')),
    (2, CONVERT(money, '922337203685477.5000')),
    (3, 0.0001),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[decimal_value] VALUES
    (1, CONVERT(decimal(38, 0), '-99999999999999999999999999999999999999')),
    (2, CONVERT(decimal(38, 0), '99999999999999999999999999999999999999')),
    (3, 0),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[numeric_value] VALUES
    (1, CONVERT(numeric(38, 37), '-9.9999999999999999999999999999999999999')),
    (2, CONVERT(numeric(38, 37), '9.9999999999999999999999999999999999999')),
    (3, CONVERT(numeric(38, 37), '0.0000000000000000000000000000000000001')),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[char_latin1_value] VALUES
    (1, ''), (2, '0123456789ABCDEF'), (3, N'España français'), (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[char_chinese_value] VALUES
    (1, ''), (2, N'中文边界'), (3, N'数据A'), (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[varchar_latin1_value] VALUES
    (1, ''),
    (2, '0123456789ABCDEF0123456789ABCDEF'),
    (3, N'España français'),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[varchar_chinese_value] VALUES
    (1, ''), (2, REPLICATE(N'中', 16)), (3, N'中文数据传输'), (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[nchar_value] VALUES
    (1, N''), (2, N'中文边界测试'), (3, N'中文😀'), (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[nvarchar_value] VALUES
    (1, N''),
    (2, REPLICATE(N'中', 32)),
    (3, N'简体中文-繁體中文-😀'),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[text_latin1_value] VALUES
    (1, ''),
    (2, REPLICATE(CONVERT(varchar(max), 'z'), 1024)),
    (3, N'España français'),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[text_chinese_value] VALUES
    (1, ''),
    (2, REPLICATE(CONVERT(varchar(max), N'中') COLLATE Chinese_PRC_CI_AS, 512)),
    (3, N'中文数据传输'),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[ntext_value] VALUES
    (1, N''),
    (2, REPLICATE(CONVERT(nvarchar(max), N'中'), 512)),
    (3, N'中文-日本語-대한민국-😀'),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[binary_value] VALUES
    (1, 0x0000000000000000),
    (2, 0xFFFFFFFFFFFFFFFF),
    (3, 0x000102030405FEFF),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[varbinary_value] VALUES
    (1, 0x),
    (2, 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF),
    (3, 0x000102030405FEFF),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[image_value] VALUES
    (1, 0x),
    (2, CONVERT(varbinary(max), REPLICATE(CONVERT(varchar(max), 'x'), 1024))),
    (3, 0x000102030405FEFF),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[uuid_value] VALUES
    (1, '00000000-0000-0000-0000-000000000000'),
    (2, 'ffffffff-ffff-ffff-ffff-ffffffffffff'),
    (3, '550e8400-e29b-41d4-a716-446655440000'),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[xml_value] VALUES
    (1, CONVERT(xml, N'<a/>')),
    (2, CONVERT(xml, N'<根 属性="边界">中文😀</根>')),
    (3, CONVERT(xml, N'<root><child>text &amp; value</child></root>')),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[date_value] VALUES
    (1, CONVERT(date, '00010101')),
    (2, CONVERT(date, '99991231')),
    (3, CONVERT(date, '20240229')),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[time_value] VALUES
    (1, CONVERT(time(7), '00:00:00')),
    (2, CONVERT(time(7), '23:59:59.9999999')),
    (3, CONVERT(time(7), '12:34:56.1234567')),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[smalldatetime_value] VALUES
    (1, CONVERT(smalldatetime, '19000101 00:00:00')),
    (2, CONVERT(smalldatetime, '20790606 23:59:00')),
    (3, CONVERT(smalldatetime, '20240814 12:34:00')),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[datetime_value] VALUES
    (1, CONVERT(datetime, '17530101 00:00:00.000')),
    (2, CONVERT(datetime, '99991231 23:59:59.997')),
    (3, CONVERT(datetime, '20240814 12:34:56.123')),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[datetime2_value] VALUES
    (1, CONVERT(datetime2(7), '0001-01-01 00:00:00')),
    (2, CONVERT(datetime2(7), '9999-12-31 23:59:59.9999999')),
    (3, CONVERT(datetime2(7), '2024-08-14 12:34:56.1234567')),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[offset_value] VALUES
    (1, CONVERT(datetimeoffset(7), '0001-01-01T00:00:00+00:00')),
    (2, CONVERT(datetimeoffset(7), '9999-12-31T23:59:59.9999999+00:00')),
    (3, CONVERT(datetimeoffset(7), '2024-08-14T12:34:56.1234567+14:00')),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[rowversion_value] ([case_id]) VALUES
    (1), (2), (3), (4);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[geometry_value] VALUES
    (1, geometry::STGeomFromText(N'POINT (1.25 -2.5 3.5 4.5)', 4326)),
    (2, geometry::STGeomFromText(N'LINESTRING (0 0, 3 4, -5 6)', 0)),
    (3, geometry::STGeomFromText(N'POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))', 3857)),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[geography_value] VALUES
    (1, geography::STGeomFromText(N'POINT (-122.360 47.656 3.5 4.5)', 4326)),
    (2, geography::STGeomFromText(N'LINESTRING (-122.360 47.656, -122.343 47.656)', 4326)),
    (3, geography::STGeomFromText(N'POINT (0 0)', 4326)),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[hierarchyid_value] VALUES
    (1, hierarchyid::Parse(N'/1/2/')),
    (2, hierarchyid::Parse(N'/1/2.5/3/')),
    (3, hierarchyid::Parse(N'/9/')),
    (4, NULL);
INSERT INTO [ape_dts_col_value_conversion_source].[dbo].[bigvariant_value] VALUES
    (1, dbo.BigVariantFromString(N'BigVariant Unicode 中文')),
    (2, dbo.BigVariantFromBinary(0x000102030405FEFF)),
    (3, dbo.BigVariantFromVariant(CONVERT(sql_variant, CONVERT(decimal(20, 6), '-12345678901234.123456')))),
    (4, NULL);
GO
