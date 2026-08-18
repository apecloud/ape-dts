DROP TABLE IF EXISTS parallel_resume_log.integer_rows;
DROP TABLE IF EXISTS parallel_resume_log.nullable_rows;
DROP TABLE IF EXISTS parallel_resume_log.[string rows.*];
DROP TABLE IF EXISTS parallel_resume_log.composite_rows;
DROP TABLE IF EXISTS parallel_resume_log.binary_rows;
DROP TABLE IF EXISTS parallel_resume_log.decimal_rows;
DROP TABLE IF EXISTS parallel_resume_log.date_rows;
DROP TABLE IF EXISTS parallel_resume_log.no_key_rows;
DROP TABLE IF EXISTS parallel_resume_log.unique_rows;
DROP TABLE IF EXISTS parallel_resume_log.position_log_rows;
DROP TABLE IF EXISTS parallel_resume_log.finished_rows;
DROP TABLE IF EXISTS parallel_resume_log.finished_config_rows;
IF SCHEMA_ID(N'parallel_resume_log') IS NULL EXEC(N'CREATE SCHEMA parallel_resume_log');
CREATE TABLE parallel_resume_log.integer_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE parallel_resume_log.nullable_rows (
    row_id int NOT NULL PRIMARY KEY,
    split_key int NULL,
    value nvarchar(30) NOT NULL
);
CREATE TABLE parallel_resume_log.[string rows.*] (
    code nvarchar(30) NOT NULL PRIMARY KEY,
    value varbinary(20) NULL
);
CREATE TABLE parallel_resume_log.composite_rows (
    tenant_id int NOT NULL, row_id int NOT NULL, value nvarchar(30) NULL,
    PRIMARY KEY (tenant_id, row_id)
);
CREATE TABLE parallel_resume_log.binary_rows (
    binary_id varbinary(16) NOT NULL PRIMARY KEY, value nvarchar(30) NULL
);
CREATE TABLE parallel_resume_log.decimal_rows (
    decimal_id decimal(20, 4) NOT NULL PRIMARY KEY, value nvarchar(30) NULL
);
CREATE TABLE parallel_resume_log.date_rows (
    event_date date NOT NULL PRIMARY KEY, value nvarchar(30) NULL
);
CREATE TABLE parallel_resume_log.no_key_rows (id int NOT NULL, value nvarchar(30) NULL);
CREATE TABLE parallel_resume_log.unique_rows (
    row_id int NOT NULL, code int NOT NULL UNIQUE, value nvarchar(30) NULL
);
CREATE TABLE parallel_resume_log.position_log_rows (
    id int NOT NULL PRIMARY KEY, value nvarchar(30) NULL
);
CREATE TABLE parallel_resume_log.finished_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE parallel_resume_log.finished_config_rows (
    id int NOT NULL PRIMARY KEY, value nvarchar(30) NULL
);
GO
