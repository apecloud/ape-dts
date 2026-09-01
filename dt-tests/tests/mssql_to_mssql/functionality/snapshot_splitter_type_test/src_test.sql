INSERT INTO [ape_dts_snapshot_splitter_component_test].[even_split].[tinyint_value] ([id], [value])
SELECT value_id, CONVERT(tinyint, value_id) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[even_split].[smallint_value] ([id], [value])
SELECT value_id, CONVERT(smallint, value_id) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[even_split].[int_value] ([id], [value])
SELECT value_id, CONVERT(int, value_id) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[even_split].[bigint_value] ([id], [value])
SELECT value_id, CONVERT(bigint, value_id) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);

INSERT INTO [ape_dts_snapshot_splitter_component_test].[uneven_split].[real_value] ([id], [value])
SELECT value_id, CONVERT(real, value_id) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[uneven_split].[float_value] ([id], [value])
SELECT value_id, CONVERT(float, value_id) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[uneven_split].[smallmoney_value] ([id], [value])
SELECT value_id, CONVERT(smallmoney, value_id) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[uneven_split].[money_value] ([id], [value])
SELECT value_id, CONVERT(money, value_id) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[uneven_split].[decimal_value] ([id], [value])
SELECT value_id, CONVERT(decimal(18, 4), value_id) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[uneven_split].[numeric_value] ([id], [value])
SELECT value_id, CONVERT(numeric(20, 6), value_id) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[uneven_split].[char_value] ([id], [value])
SELECT value_id, CONVERT(char(8), CONCAT('c', value_id)) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[uneven_split].[varchar_value] ([id], [value])
SELECT value_id, CONVERT(varchar(20), CONCAT('v', value_id)) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[uneven_split].[nchar_value] ([id], [value])
SELECT value_id, CONVERT(nchar(8), CONCAT(N'nc', value_id)) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[uneven_split].[nvarchar_value] ([id], [value])
SELECT value_id, CONVERT(nvarchar(20), CONCAT(N'nv', value_id)) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[uneven_split].[binary_value] ([id], [value])
SELECT value_id, CONVERT(binary(8), value_id) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[uneven_split].[varbinary_value] ([id], [value])
SELECT value_id, CONVERT(varbinary(8), value_id) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[uneven_split].[uuid_value] ([id], [value])
SELECT value_id,
       CONVERT(uniqueidentifier, CONCAT('00000000-0000-0000-0000-', RIGHT('000000000000' + CONVERT(varchar(12), value_id), 12)))
FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[uneven_split].[date_value] ([id], [value])
SELECT value_id, DATEFROMPARTS(2024, 1, value_id) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[uneven_split].[time_value] ([id], [value])
SELECT value_id, TIMEFROMPARTS(value_id, 0, 0, 0, 7) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[uneven_split].[smalldatetime_value] ([id], [value])
SELECT value_id, DATEADD(day, value_id, CONVERT(smalldatetime, '20240101', 112)) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[uneven_split].[datetime_value] ([id], [value])
SELECT value_id, DATEADD(day, value_id, CONVERT(datetime, '20240101', 112)) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[uneven_split].[datetime2_value] ([id], [value])
SELECT value_id, DATEADD(day, value_id, CONVERT(datetime2, '20240101', 112)) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[uneven_split].[datetimeoffset_value] ([id], [value])
SELECT value_id, TODATETIMEOFFSET(DATEADD(day, value_id, CONVERT(datetime2, '20240101', 112)), '+08:00')
FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[uneven_split].[rowversion_value] ([id])
SELECT value_id FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[uneven_split].[timestamp_value] ([id])
SELECT value_id FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);

INSERT INTO [ape_dts_snapshot_splitter_component_test].[full_table].[bit_value] ([id], [value])
SELECT value_id, CONVERT(bit, value_id % 2) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[full_table].[text_value] ([id], [value])
SELECT value_id, CONVERT(varchar(20), CONCAT('text', value_id)) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[full_table].[ntext_value] ([id], [value])
SELECT value_id, CONVERT(nvarchar(20), CONCAT(N'ntext', value_id)) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[full_table].[image_value] ([id], [value])
SELECT value_id, CONVERT(varbinary(8), value_id) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
INSERT INTO [ape_dts_snapshot_splitter_component_test].[full_table].[xml_value] ([id], [value])
SELECT value_id, CONVERT(xml, CONCAT('<value>', value_id, '</value>')) FROM (VALUES (1), (2), (3), (4), (5)) AS test_values(value_id);
GO
