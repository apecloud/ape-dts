DROP TABLE IF EXISTS parallel_resume_db.integer_rows;
DROP TABLE IF EXISTS parallel_resume_db.nullable_rows;
DROP TABLE IF EXISTS parallel_resume_db.[string rows.*];
DROP TABLE IF EXISTS parallel_resume_db.finished_rows;
DROP TABLE IF EXISTS ape_dts_parallel_resume.positions;
IF SCHEMA_ID(N'parallel_resume_db') IS NULL EXEC(N'CREATE SCHEMA parallel_resume_db');
IF SCHEMA_ID(N'ape_dts_parallel_resume') IS NULL EXEC(N'CREATE SCHEMA ape_dts_parallel_resume');
CREATE TABLE parallel_resume_db.integer_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE parallel_resume_db.nullable_rows (
    row_id int NOT NULL PRIMARY KEY,
    split_key int NULL,
    value nvarchar(30) NOT NULL
);
CREATE TABLE parallel_resume_db.[string rows.*] (
    code nvarchar(30) NOT NULL PRIMARY KEY,
    value varbinary(20) NULL
);
CREATE TABLE parallel_resume_db.finished_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
INSERT INTO parallel_resume_db.integer_rows VALUES
    (1, N'v1'), (2, N'v2'), (3, N'v3'), (4, N'v4');
INSERT INTO parallel_resume_db.nullable_rows VALUES
    (1, 1, N'v1'), (2, 2, N'v2'), (3, 3, N'v3');
INSERT INTO parallel_resume_db.[string rows.*] VALUES
    (N'a', 0x01), (N'b', 0x02), (N'c', 0x03);

CREATE TABLE ape_dts_parallel_resume.positions (
    id bigint IDENTITY(1, 1) PRIMARY KEY,
    task_id nvarchar(255) NOT NULL,
    resumer_type nvarchar(100) NOT NULL,
    position_key nvarchar(475) NOT NULL,
    position_data nvarchar(max) NULL,
    created_at datetime2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at datetime2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    UNIQUE (task_id, resumer_type, position_key)
);
INSERT INTO ape_dts_parallel_resume.positions
    (task_id, resumer_type, position_key, position_data) VALUES
    (N'mssql_parallel_resume_db_test', N'SnapshotDoing', N'parallel_resume_db-integer_rows',
     N'{"type":"RdbSnapshot","db_type":"mssql","schema":"parallel_resume_db","tb":"integer_rows","order_key":{"single":["id","4"]}}'),
    (N'mssql_parallel_resume_db_test', N'SnapshotDoing', N'parallel_resume_db-nullable_rows',
     N'{"type":"RdbSnapshot","db_type":"mssql","schema":"parallel_resume_db","tb":"nullable_rows","order_key":{"single":["split_key","3"]}}'),
    (N'mssql_parallel_resume_db_test', N'SnapshotDoing', N'parallel_resume_db-string rows.*',
     N'{"type":"RdbSnapshot","db_type":"mssql","schema":"parallel_resume_db","tb":"string rows.*","order_key":{"single":["code","c"]}}'),
    (N'mssql_parallel_resume_db_test', N'SnapshotFinished', N'parallel_resume_db-finished_rows', NULL);
GO
