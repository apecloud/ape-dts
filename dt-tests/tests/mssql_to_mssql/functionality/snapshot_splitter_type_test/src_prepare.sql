USE [master];

IF DB_ID(N'ape_dts_snapshot_splitter_component_test') IS NOT NULL
BEGIN
    ALTER DATABASE [ape_dts_snapshot_splitter_component_test]
        SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [ape_dts_snapshot_splitter_component_test];
END;

CREATE DATABASE [ape_dts_snapshot_splitter_component_test];
GO

USE [ape_dts_snapshot_splitter_component_test];

EXEC(N'CREATE SCHEMA [even_split]');
EXEC(N'CREATE SCHEMA [uneven_split]');
EXEC(N'CREATE SCHEMA [full_table]');

CREATE TABLE [ape_dts_snapshot_splitter_component_test].[even_split].[tinyint_value]
    ([id] int NOT NULL PRIMARY KEY, [value] tinyint NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[even_split].[smallint_value]
    ([id] int NOT NULL PRIMARY KEY, [value] smallint NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[even_split].[int_value]
    ([id] int NOT NULL PRIMARY KEY, [value] int NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[even_split].[bigint_value]
    ([id] int NOT NULL PRIMARY KEY, [value] bigint NOT NULL);

CREATE TABLE [ape_dts_snapshot_splitter_component_test].[uneven_split].[real_value]
    ([id] int NOT NULL PRIMARY KEY, [value] real NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[uneven_split].[float_value]
    ([id] int NOT NULL PRIMARY KEY, [value] float NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[uneven_split].[smallmoney_value]
    ([id] int NOT NULL PRIMARY KEY, [value] smallmoney NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[uneven_split].[money_value]
    ([id] int NOT NULL PRIMARY KEY, [value] money NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[uneven_split].[decimal_value]
    ([id] int NOT NULL PRIMARY KEY, [value] decimal(18, 4) NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[uneven_split].[numeric_value]
    ([id] int NOT NULL PRIMARY KEY, [value] numeric(20, 6) NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[uneven_split].[char_value]
    ([id] int NOT NULL PRIMARY KEY, [value] char(8) NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[uneven_split].[varchar_value]
    ([id] int NOT NULL PRIMARY KEY, [value] varchar(20) NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[uneven_split].[nchar_value]
    ([id] int NOT NULL PRIMARY KEY, [value] nchar(8) NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[uneven_split].[nvarchar_value]
    ([id] int NOT NULL PRIMARY KEY, [value] nvarchar(20) NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[uneven_split].[binary_value]
    ([id] int NOT NULL PRIMARY KEY, [value] binary(8) NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[uneven_split].[varbinary_value]
    ([id] int NOT NULL PRIMARY KEY, [value] varbinary(8) NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[uneven_split].[uuid_value]
    ([id] int NOT NULL PRIMARY KEY, [value] uniqueidentifier NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[uneven_split].[date_value]
    ([id] int NOT NULL PRIMARY KEY, [value] date NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[uneven_split].[time_value]
    ([id] int NOT NULL PRIMARY KEY, [value] time(7) NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[uneven_split].[smalldatetime_value]
    ([id] int NOT NULL PRIMARY KEY, [value] smalldatetime NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[uneven_split].[datetime_value]
    ([id] int NOT NULL PRIMARY KEY, [value] datetime NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[uneven_split].[datetime2_value]
    ([id] int NOT NULL PRIMARY KEY, [value] datetime2(7) NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[uneven_split].[datetimeoffset_value]
    ([id] int NOT NULL PRIMARY KEY, [value] datetimeoffset(7) NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[uneven_split].[rowversion_value]
    ([id] int NOT NULL PRIMARY KEY, [value] rowversion NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[uneven_split].[timestamp_value]
    ([id] int NOT NULL PRIMARY KEY, [value] timestamp NOT NULL);

CREATE TABLE [ape_dts_snapshot_splitter_component_test].[full_table].[bit_value]
    ([id] int NOT NULL PRIMARY KEY, [value] bit NOT NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[full_table].[text_value]
    ([id] int NOT NULL PRIMARY KEY, [value] text NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[full_table].[ntext_value]
    ([id] int NOT NULL PRIMARY KEY, [value] ntext NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[full_table].[image_value]
    ([id] int NOT NULL PRIMARY KEY, [value] image NULL);
CREATE TABLE [ape_dts_snapshot_splitter_component_test].[full_table].[xml_value]
    ([id] int NOT NULL PRIMARY KEY, [value] xml NULL);
GO
