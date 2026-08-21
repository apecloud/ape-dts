USE [ape_dts];
GO
DROP TABLE IF EXISTS [ape_dts].parallel_resume_db.integer_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_resume_db.nullable_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_resume_db.[string rows.*];
DROP TABLE IF EXISTS [ape_dts].parallel_resume_db.composite_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_resume_db.binary_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_resume_db.decimal_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_resume_db.date_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_resume_db.no_key_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_resume_db.unique_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_resume_db.finished_rows;
IF SCHEMA_ID(N'parallel_resume_db') IS NULL EXEC(N'CREATE SCHEMA parallel_resume_db');
CREATE TABLE [ape_dts].parallel_resume_db.integer_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE [ape_dts].parallel_resume_db.nullable_rows (
    row_id int NOT NULL PRIMARY KEY,
    split_key int NULL,
    value nvarchar(30) NOT NULL
);
CREATE TABLE [ape_dts].parallel_resume_db.[string rows.*] (
    code nvarchar(30) NOT NULL PRIMARY KEY,
    value varbinary(20) NULL
);
CREATE TABLE [ape_dts].parallel_resume_db.composite_rows (
    tenant_id int NOT NULL, row_id int NOT NULL, value nvarchar(30) NULL,
    PRIMARY KEY (tenant_id, row_id)
);
CREATE TABLE [ape_dts].parallel_resume_db.binary_rows (
    binary_id varbinary(16) NOT NULL PRIMARY KEY, value nvarchar(30) NULL
);
CREATE TABLE [ape_dts].parallel_resume_db.decimal_rows (
    decimal_id decimal(20, 4) NOT NULL PRIMARY KEY, value nvarchar(30) NULL
);
CREATE TABLE [ape_dts].parallel_resume_db.date_rows (
    event_date date NOT NULL PRIMARY KEY, value nvarchar(30) NULL
);
CREATE TABLE [ape_dts].parallel_resume_db.no_key_rows (id int NOT NULL, value nvarchar(30) NULL);
CREATE TABLE [ape_dts].parallel_resume_db.unique_rows (
    row_id int NOT NULL, code int NOT NULL UNIQUE, value nvarchar(30) NULL
);
CREATE TABLE [ape_dts].parallel_resume_db.finished_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
GO
