USE [ape_dts];
GO
DROP TABLE IF EXISTS [ape_dts].resume_log_test.resume_rows;
DROP TABLE IF EXISTS [ape_dts].resume_log_test.config_rows;
DROP TABLE IF EXISTS [ape_dts].resume_log_test.composite_rows;
DROP TABLE IF EXISTS [ape_dts].resume_log_test.binary_key_rows;
DROP TABLE IF EXISTS [ape_dts].resume_log_test.[resume table.*];
DROP TABLE IF EXISTS [ape_dts].resume_log_test.nullable_composite_unique_rows;
DROP TABLE IF EXISTS [ape_dts].resume_log_test.string_key_rows;
DROP TABLE IF EXISTS [ape_dts].resume_log_test.date_key_rows;
DROP TABLE IF EXISTS [ape_dts].resume_log_test.no_key_rows;
DROP TABLE IF EXISTS [ape_dts].resume_log_test.fresh_rows;
DROP TABLE IF EXISTS [ape_dts].resume_log_test.finished_config_rows;
DROP TABLE IF EXISTS [ape_dts].resume_log_test.finished_log_rows;
IF SCHEMA_ID(N'resume_log_test') IS NULL EXEC(N'CREATE SCHEMA resume_log_test');
CREATE TABLE [ape_dts].resume_log_test.resume_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE [ape_dts].resume_log_test.config_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE [ape_dts].resume_log_test.composite_rows (
    tenant_id int NOT NULL,
    row_id int NOT NULL,
    value nvarchar(30) NOT NULL,
    PRIMARY KEY (tenant_id, row_id)
);
CREATE TABLE [ape_dts].resume_log_test.binary_key_rows (
    binary_id varbinary(32) NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE [ape_dts].resume_log_test.[resume table.*] (
    [p.k] int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE [ape_dts].resume_log_test.nullable_composite_unique_rows (
    row_id int NOT NULL, uk1 int NULL, uk2 nvarchar(20) NULL, value nvarchar(50) NULL
);
CREATE UNIQUE INDEX uk_resume_log_nullable_composite
    ON [ape_dts].resume_log_test.nullable_composite_unique_rows (uk1, uk2)
    WHERE uk1 IS NOT NULL AND uk2 IS NOT NULL;
CREATE TABLE [ape_dts].resume_log_test.string_key_rows (
    code nvarchar(30) NOT NULL PRIMARY KEY, value varbinary(30) NULL
);
CREATE TABLE [ape_dts].resume_log_test.date_key_rows (
    event_date date NOT NULL PRIMARY KEY, value nvarchar(50) NULL
);
CREATE TABLE [ape_dts].resume_log_test.no_key_rows (
    id int NULL, value nvarchar(50) NULL
);
CREATE TABLE [ape_dts].resume_log_test.fresh_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NULL
);
CREATE TABLE [ape_dts].resume_log_test.finished_config_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE [ape_dts].resume_log_test.finished_log_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
GO
