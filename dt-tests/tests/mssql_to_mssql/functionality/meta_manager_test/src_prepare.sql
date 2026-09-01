USE [master];

IF DB_ID(N'ape_dts_meta_manager_component_test') IS NOT NULL
BEGIN
    ALTER DATABASE [ape_dts_meta_manager_component_test]
        SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [ape_dts_meta_manager_component_test];
END;

CREATE DATABASE [ape_dts_meta_manager_component_test];
GO

USE [ape_dts_meta_manager_component_test];

EXEC(N'CREATE SCHEMA [meta_manager_test]');

CREATE TABLE [ape_dts_meta_manager_component_test].[meta_manager_test].[catalog_types] (
    [tenant_id] int NOT NULL,
    [id] bigint IDENTITY(1, 1) NOT NULL,
    [optional_name] nvarchar(100) NULL,
    [score] float(24) NOT NULL,
    [alias_name] sysname NOT NULL,
    [computed_value] AS ([tenant_id] + 1),
    [valid_from] datetime2 GENERATED ALWAYS AS ROW START NOT NULL
        DEFAULT SYSUTCDATETIME(),
    [valid_to] datetime2 GENERATED ALWAYS AS ROW END NOT NULL
        DEFAULT CONVERT(datetime2, '9999-12-31 23:59:59.9999999'),
    [version] rowversion NOT NULL,
    CONSTRAINT [pk_ape_dts_meta_manager] PRIMARY KEY ([tenant_id], [id]),
    CONSTRAINT [uq_ape_dts_meta_manager_name] UNIQUE ([optional_name]),
    PERIOD FOR SYSTEM_TIME ([valid_from], [valid_to])
);
GO
