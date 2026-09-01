USE [master];

IF DB_ID(N'ape_dts_sinker_component_test') IS NOT NULL
BEGIN
    ALTER DATABASE [ape_dts_sinker_component_test]
        SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [ape_dts_sinker_component_test];
END;

CREATE DATABASE [ape_dts_sinker_component_test];
GO

USE [ape_dts_sinker_component_test];

CREATE TABLE [ape_dts_sinker_component_test].[dbo].[transaction_rows] (
    id int IDENTITY(1, 1) NOT NULL PRIMARY KEY,
    code nvarchar(20) NOT NULL UNIQUE,
    computed_code AS UPPER(code),
    valid_from datetime2 GENERATED ALWAYS AS ROW START NOT NULL
        DEFAULT SYSUTCDATETIME(),
    valid_to datetime2 GENERATED ALWAYS AS ROW END NOT NULL
        DEFAULT CONVERT(datetime2, '9999-12-31 23:59:59.9999999'),
    version rowversion NOT NULL,
    PERIOD FOR SYSTEM_TIME (valid_from, valid_to)
);

CREATE TABLE [ape_dts_sinker_component_test].[dbo].[bulk_rows] (
    id int NOT NULL PRIMARY KEY,
    code nvarchar(20) NOT NULL,
    happened_at datetime2(7) NOT NULL
);

CREATE TABLE [ape_dts_sinker_component_test].[dbo].[parameter_rows] (
    id int NOT NULL PRIMARY KEY,
    datetime_value datetime NOT NULL,
    smalldatetime_value smalldatetime NOT NULL
);
GO
