USE [master];

IF DB_ID(N'ape_dts_snapshot_extractor_component_test') IS NOT NULL
BEGIN
    ALTER DATABASE [ape_dts_snapshot_extractor_component_test]
        SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [ape_dts_snapshot_extractor_component_test];
END;

CREATE DATABASE [ape_dts_snapshot_extractor_component_test];
GO

USE [ape_dts_snapshot_extractor_component_test];

EXEC(N'CREATE SCHEMA [invalid_order_columns]');
EXEC(N'CREATE SCHEMA [snapshot_extractor_test]');

CREATE TABLE [ape_dts_snapshot_extractor_component_test].[invalid_order_columns].[generated_always_order_rows] (
    [id] int NOT NULL PRIMARY KEY,
    [valid_from] datetime2 GENERATED ALWAYS AS ROW START NOT NULL
        DEFAULT SYSUTCDATETIME(),
    [valid_to] datetime2 GENERATED ALWAYS AS ROW END NOT NULL
        DEFAULT CONVERT(datetime2, '9999-12-31 23:59:59.9999999'),
    PERIOD FOR SYSTEM_TIME ([valid_from], [valid_to])
);

INSERT INTO [ape_dts_snapshot_extractor_component_test].[invalid_order_columns].[generated_always_order_rows]
    ([id]) VALUES (1);

CREATE TABLE [ape_dts_snapshot_extractor_component_test].[invalid_order_columns].[rowversion_order_rows] (
    [id] int NOT NULL PRIMARY KEY,
    [rowversion_value] rowversion NOT NULL
);

INSERT INTO [ape_dts_snapshot_extractor_component_test].[invalid_order_columns].[rowversion_order_rows]
    ([id]) VALUES (1);

CREATE TABLE [ape_dts_snapshot_extractor_component_test].[invalid_order_columns].[computed_order_rows] (
    [base_value] int NOT NULL,
    [computed_value] AS ([base_value] * 2) PERSISTED
);

CREATE UNIQUE INDEX [uk_computed_order_rows]
    ON [ape_dts_snapshot_extractor_component_test].[invalid_order_columns].[computed_order_rows]
    ([computed_value]);

INSERT INTO [ape_dts_snapshot_extractor_component_test].[invalid_order_columns].[computed_order_rows]
    ([base_value]) VALUES (1);

CREATE TABLE [ape_dts_snapshot_extractor_component_test].[invalid_order_columns].[timestamp_order_rows] (
    [id] int NOT NULL PRIMARY KEY,
    [timestamp_value] timestamp NOT NULL
);

INSERT INTO [ape_dts_snapshot_extractor_component_test].[invalid_order_columns].[timestamp_order_rows]
    ([id]) VALUES (1);

CREATE TABLE [ape_dts_snapshot_extractor_component_test].[snapshot_extractor_test].[snapshot_rows] (
    [id] int NOT NULL PRIMARY KEY,
    [split_key] int NULL,
    [name] nvarchar(20) NOT NULL
);

INSERT INTO [ape_dts_snapshot_extractor_component_test].[snapshot_extractor_test].[snapshot_rows]
    ([id], [split_key], [name]) VALUES
    (1, NULL, N'a'),
    (2, NULL, N'b'),
    (3, 10, N'c'),
    (4, 20, N'd'),
    (5, 30, N'e'),
    (6, 40, N'f'),
    (7, 50, N'g');
GO
