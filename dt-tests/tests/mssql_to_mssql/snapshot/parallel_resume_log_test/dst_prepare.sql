USE [ape_dts];
GO
DROP TABLE IF EXISTS [ape_dts].parallel_resume_log.integer_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_resume_log.nullable_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_resume_log.[string rows.*];
DROP TABLE IF EXISTS [ape_dts].parallel_resume_log.composite_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_resume_log.binary_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_resume_log.decimal_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_resume_log.date_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_resume_log.no_key_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_resume_log.unique_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_resume_log.position_log_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_resume_log.finished_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_resume_log.finished_config_rows;
IF SCHEMA_ID(N'parallel_resume_log') IS NULL EXEC(N'CREATE SCHEMA parallel_resume_log');
CREATE TABLE [ape_dts].parallel_resume_log.integer_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE [ape_dts].parallel_resume_log.nullable_rows (
    row_id int NOT NULL PRIMARY KEY,
    split_key int NULL,
    value nvarchar(30) NOT NULL
);
CREATE TABLE [ape_dts].parallel_resume_log.[string rows.*] (
    code nvarchar(30) NOT NULL PRIMARY KEY,
    value varbinary(20) NULL
);
CREATE TABLE [ape_dts].parallel_resume_log.composite_rows (
    tenant_id int NOT NULL, row_id int NOT NULL, value nvarchar(30) NULL,
    PRIMARY KEY (tenant_id, row_id)
);
CREATE TABLE [ape_dts].parallel_resume_log.binary_rows (
    binary_id varbinary(16) NOT NULL PRIMARY KEY, value nvarchar(30) NULL
);
CREATE TABLE [ape_dts].parallel_resume_log.decimal_rows (
    decimal_id decimal(20, 4) NOT NULL PRIMARY KEY, value nvarchar(30) NULL
);
CREATE TABLE [ape_dts].parallel_resume_log.date_rows (
    event_date date NOT NULL PRIMARY KEY, value nvarchar(30) NULL
);
CREATE TABLE [ape_dts].parallel_resume_log.no_key_rows (id int NOT NULL, value nvarchar(30) NULL);
CREATE TABLE [ape_dts].parallel_resume_log.unique_rows (
    row_id int NOT NULL, code int NOT NULL UNIQUE, value nvarchar(30) NULL
);
CREATE TABLE [ape_dts].parallel_resume_log.position_log_rows (
    id int NOT NULL PRIMARY KEY, value nvarchar(30) NULL
);
CREATE TABLE [ape_dts].parallel_resume_log.finished_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE [ape_dts].parallel_resume_log.finished_config_rows (
    id int NOT NULL PRIMARY KEY, value nvarchar(30) NULL
);
INSERT INTO [ape_dts].parallel_resume_log.integer_rows VALUES
    (1, N'v1'), (2, N'v2'), (3, N'v3'), (4, N'v4');
INSERT INTO [ape_dts].parallel_resume_log.nullable_rows VALUES
    (1, 1, N'v1'), (2, 2, N'v2'), (3, 3, N'v3');
INSERT INTO [ape_dts].parallel_resume_log.[string rows.*] VALUES
    (N'a', 0x01), (N'b', 0x02), (N'c', 0x03);
INSERT INTO [ape_dts].parallel_resume_log.composite_rows VALUES
    (1, 1, N'one-one'), (1, 2, N'one-two');
INSERT INTO [ape_dts].parallel_resume_log.binary_rows VALUES
    (0x0001, N'binary-one'), (0x0002, N'binary-two');
INSERT INTO [ape_dts].parallel_resume_log.decimal_rows VALUES
    (-999999999999.9999, N'decimal-min'), (-10.5000, N'decimal-negative');
INSERT INTO [ape_dts].parallel_resume_log.date_rows VALUES
    ('0001-01-01', N'date-min'), ('2024-01-01', N'date-one');
INSERT INTO [ape_dts].parallel_resume_log.position_log_rows VALUES
    (1, N'position-one'), (2, N'position-two'), (3, N'position-three');
GO
