DROP TABLE IF EXISTS resume_log_test.resume_rows;
DROP TABLE IF EXISTS resume_log_test.config_rows;
DROP TABLE IF EXISTS resume_log_test.composite_rows;
DROP TABLE IF EXISTS resume_log_test.binary_key_rows;
DROP TABLE IF EXISTS resume_log_test.[resume table.*];
DROP TABLE IF EXISTS resume_log_test.fresh_rows;
DROP TABLE IF EXISTS resume_log_test.finished_config_rows;
DROP TABLE IF EXISTS resume_log_test.finished_log_rows;
IF SCHEMA_ID(N'resume_log_test') IS NULL EXEC(N'CREATE SCHEMA resume_log_test');
CREATE TABLE resume_log_test.resume_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE resume_log_test.config_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE resume_log_test.composite_rows (
    tenant_id int NOT NULL,
    row_id int NOT NULL,
    value nvarchar(30) NOT NULL,
    PRIMARY KEY (tenant_id, row_id)
);
CREATE TABLE resume_log_test.binary_key_rows (
    binary_id varbinary(32) NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE resume_log_test.[resume table.*] (
    [p.k] int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE resume_log_test.fresh_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE resume_log_test.finished_config_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE resume_log_test.finished_log_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
GO
