USE [ape_dts];
GO
DROP TABLE IF EXISTS [ape_dts].resume_test.resume_rows;
DROP TABLE IF EXISTS [ape_dts].resume_test.composite_rows;
DROP TABLE IF EXISTS [ape_dts].resume_test.binary_key_rows;
DROP TABLE IF EXISTS [ape_dts].resume_test.[resume table.*];
DROP TABLE IF EXISTS [ape_dts].resume_test.nullable_composite_unique_rows;
DROP TABLE IF EXISTS [ape_dts].resume_test.string_key_rows;
DROP TABLE IF EXISTS [ape_dts].resume_test.fresh_rows;
DROP TABLE IF EXISTS [ape_dts].resume_test.finished_rows;
DROP TABLE IF EXISTS [ape_dts].resume_test.finished_rows_2;
IF SCHEMA_ID(N'resume_test') IS NULL EXEC(N'CREATE SCHEMA resume_test');
CREATE TABLE [ape_dts].resume_test.resume_rows (id int NOT NULL PRIMARY KEY, value nvarchar(50) NULL);
CREATE TABLE [ape_dts].resume_test.composite_rows (
    tenant_id int NOT NULL, row_id int NOT NULL, value nvarchar(50) NULL,
    PRIMARY KEY (tenant_id, row_id)
);
CREATE TABLE [ape_dts].resume_test.binary_key_rows (binary_id varbinary(32) NOT NULL PRIMARY KEY, value nvarchar(50) NULL);
CREATE TABLE [ape_dts].resume_test.[resume table.*] ([p.k] int NOT NULL PRIMARY KEY, [value.*] nvarchar(50) NULL);
CREATE TABLE [ape_dts].resume_test.nullable_composite_unique_rows (
    row_id int NOT NULL, uk1 int NULL, uk2 nvarchar(20) NULL, value nvarchar(50) NULL
);
CREATE UNIQUE INDEX uk_nullable_composite
    ON [ape_dts].resume_test.nullable_composite_unique_rows (uk1, uk2)
    WHERE uk1 IS NOT NULL AND uk2 IS NOT NULL;
CREATE TABLE [ape_dts].resume_test.string_key_rows (code nvarchar(30) NOT NULL PRIMARY KEY, value varbinary(30) NULL);
CREATE TABLE [ape_dts].resume_test.fresh_rows (id int NOT NULL PRIMARY KEY, value nvarchar(50) NULL);
CREATE TABLE [ape_dts].resume_test.finished_rows (id int NOT NULL PRIMARY KEY, value nvarchar(50) NULL);
CREATE TABLE [ape_dts].resume_test.finished_rows_2 (id int NOT NULL PRIMARY KEY, value nvarchar(50) NULL);
GO
