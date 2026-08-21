USE [ape_dts];
GO
DROP TABLE IF EXISTS [ape_dts].big_packet_test.large_values;
IF SCHEMA_ID(N'big_packet_test') IS NULL EXEC(N'CREATE SCHEMA big_packet_test');
CREATE TABLE [ape_dts].big_packet_test.large_values (
    id int NOT NULL PRIMARY KEY,
    text_payload nvarchar(max) NOT NULL,
    binary_payload varbinary(max) NOT NULL
);
GO
