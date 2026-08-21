USE [ape_dts];
GO
IF DB_ID(N'ape_dts_parallel_resume') IS NULL CREATE DATABASE [ape_dts_parallel_resume];
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
DROP TABLE IF EXISTS [ape_dts_parallel_resume].[dbo].[positions];
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
INSERT INTO [ape_dts].parallel_resume_db.integer_rows VALUES
    (1, N'v1'), (2, N'v2'), (3, N'v3'), (4, N'v4');
INSERT INTO [ape_dts].parallel_resume_db.nullable_rows VALUES
    (1, 1, N'v1'), (2, 2, N'v2'), (3, 3, N'v3');
INSERT INTO [ape_dts].parallel_resume_db.[string rows.*] VALUES
    (N'a', 0x01), (N'b', 0x02), (N'c', 0x03);
INSERT INTO [ape_dts].parallel_resume_db.composite_rows VALUES
    (1, 1, N'one-one'), (1, 2, N'one-two');
INSERT INTO [ape_dts].parallel_resume_db.binary_rows VALUES
    (0x0001, N'binary-one'), (0x0002, N'binary-two');
INSERT INTO [ape_dts].parallel_resume_db.decimal_rows VALUES
    (-999999999999.9999, N'decimal-min'), (-10.5000, N'decimal-negative');
INSERT INTO [ape_dts].parallel_resume_db.date_rows VALUES
    ('0001-01-01', N'date-min'), ('2024-01-01', N'date-one');

CREATE TABLE [ape_dts_parallel_resume].[dbo].[positions] (
    id bigint IDENTITY(1, 1) PRIMARY KEY,
    task_id nvarchar(255) NOT NULL,
    resumer_type nvarchar(100) NOT NULL,
    position_key nvarchar(475) NOT NULL,
    position_data nvarchar(max) NULL,
    created_at datetime2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at datetime2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    UNIQUE (task_id, resumer_type, position_key)
);
INSERT INTO [ape_dts_parallel_resume].[dbo].[positions]
    (task_id, resumer_type, position_key, position_data) VALUES
    (N'mssql_parallel_resume_db_test', N'SnapshotDoing', N'ape_dts-parallel_resume_db-integer_rows',
     N'{"type":"RdbSnapshot","db_type":"mssql","db":"ape_dts","schema":"parallel_resume_db","tb":"integer_rows","order_key":{"single":["id","4"]}}'),
    (N'mssql_parallel_resume_db_test', N'SnapshotDoing', N'ape_dts-parallel_resume_db-nullable_rows',
     N'{"type":"RdbSnapshot","db_type":"mssql","db":"ape_dts","schema":"parallel_resume_db","tb":"nullable_rows","order_key":{"single":["split_key","3"]}}'),
    (N'mssql_parallel_resume_db_test', N'SnapshotDoing', N'ape_dts-parallel_resume_db-string rows.*',
     N'{"type":"RdbSnapshot","db_type":"mssql","db":"ape_dts","schema":"parallel_resume_db","tb":"string rows.*","order_key":{"single":["code","c"]}}'),
    (N'mssql_parallel_resume_db_test', N'SnapshotDoing', N'ape_dts-parallel_resume_db-composite_rows',
     N'{"type":"RdbSnapshot","db_type":"mssql","db":"ape_dts","schema":"parallel_resume_db","tb":"composite_rows","order_key":{"single":["tenant_id","1"]}}'),
    (N'mssql_parallel_resume_db_test', N'SnapshotDoing', N'ape_dts-parallel_resume_db-binary_rows',
     N'{"type":"RdbSnapshot","db_type":"mssql","db":"ape_dts","schema":"parallel_resume_db","tb":"binary_rows","order_key":{"single":["binary_id","0002"]}}'),
    (N'mssql_parallel_resume_db_test', N'SnapshotDoing', N'ape_dts-parallel_resume_db-decimal_rows',
     N'{"type":"RdbSnapshot","db_type":"mssql","db":"ape_dts","schema":"parallel_resume_db","tb":"decimal_rows","order_key":{"single":["decimal_id","-10.5000"]}}'),
    (N'mssql_parallel_resume_db_test', N'SnapshotDoing', N'ape_dts-parallel_resume_db-date_rows',
     N'{"type":"RdbSnapshot","db_type":"mssql","db":"ape_dts","schema":"parallel_resume_db","tb":"date_rows","order_key":{"single":["event_date","2024-01-01"]}}'),
    (N'mssql_parallel_resume_db_test', N'SnapshotFinished', N'ape_dts-parallel_resume_db-finished_rows', NULL);
GO
