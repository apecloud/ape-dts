DROP TABLE IF EXISTS resume_test.resume_rows;
DROP TABLE IF EXISTS resume_test.fresh_rows;
DROP TABLE IF EXISTS resume_test.finished_rows;
DROP TABLE IF EXISTS ape_dts_resume_test.positions;
IF SCHEMA_ID(N'resume_test') IS NULL EXEC(N'CREATE SCHEMA resume_test');
IF SCHEMA_ID(N'ape_dts_resume_test') IS NULL EXEC(N'CREATE SCHEMA ape_dts_resume_test');
CREATE TABLE resume_test.resume_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE resume_test.fresh_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE resume_test.finished_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
INSERT INTO resume_test.resume_rows VALUES (1, N'one'), (2, N'two');

CREATE TABLE ape_dts_resume_test.positions (
    id bigint IDENTITY(1, 1) PRIMARY KEY,
    task_id nvarchar(255) NOT NULL,
    resumer_type nvarchar(100) NOT NULL,
    position_key nvarchar(475) NOT NULL,
    position_data nvarchar(max) NULL,
    created_at datetime2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at datetime2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    UNIQUE (task_id, resumer_type, position_key)
);
INSERT INTO ape_dts_resume_test.positions
    (task_id, resumer_type, position_key, position_data) VALUES
    (N'mssql_resume_db_test', N'SnapshotDoing', N'resume_test-resume_rows',
     N'{"type":"RdbSnapshot","db_type":"mssql","schema":"resume_test","tb":"resume_rows","order_key":{"single":["id","2"]}}'),
    (N'mssql_resume_db_test', N'SnapshotFinished', N'resume_test-finished_rows', NULL);
GO
