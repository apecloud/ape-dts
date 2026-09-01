USE [master];

IF DB_ID(N'ape_dts_connection_pool_component_test') IS NOT NULL
BEGIN
    ALTER DATABASE [ape_dts_connection_pool_component_test]
        SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [ape_dts_connection_pool_component_test];
END;

CREATE DATABASE [ape_dts_connection_pool_component_test];
GO

CREATE TABLE [ape_dts_connection_pool_component_test].[dbo].[cross_task]
    ([task_id] int NOT NULL PRIMARY KEY, [session_id] int NOT NULL);
CREATE TABLE [ape_dts_connection_pool_component_test].[dbo].[transaction_test]
    ([id] int NOT NULL PRIMARY KEY);
CREATE TABLE [ape_dts_connection_pool_component_test].[dbo].[poisoned_transaction]
    ([id] int NOT NULL PRIMARY KEY);
CREATE TABLE [ape_dts_connection_pool_component_test].[dbo].[table_sink_identity]
    ([id] int IDENTITY(1, 1) NOT NULL PRIMARY KEY);
CREATE TABLE [ape_dts_connection_pool_component_test].[dbo].[table_sink_regular]
    ([id] int NOT NULL PRIMARY KEY);
GO
