USE [ape_dts];
GO
DROP TABLE IF EXISTS [ape_dts].[tls_test].[accounts];
IF SCHEMA_ID(N'tls_test') IS NULL EXEC(N'CREATE SCHEMA tls_test');
GO
CREATE TABLE [ape_dts].[tls_test].[accounts] (id INT NOT NULL PRIMARY KEY, value VARCHAR(100) NOT NULL);
