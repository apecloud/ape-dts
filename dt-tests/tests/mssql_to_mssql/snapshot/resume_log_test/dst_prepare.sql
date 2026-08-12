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
INSERT INTO resume_log_test.resume_rows VALUES
    (1, N'one'), (2, N'two'), (3, N'three');
INSERT INTO resume_log_test.config_rows VALUES
    (1, N'one'), (2, N'two');
INSERT INTO resume_log_test.composite_rows VALUES
    (1, 1, N'one-one'), (1, 2, N'one-two');
INSERT INTO resume_log_test.binary_key_rows VALUES
    (0x00FF, N'binary-one'), (0x0102, N'binary-two');
INSERT INTO resume_log_test.[resume table.*] VALUES (1, N'special-one');
GO
