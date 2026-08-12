DROP TABLE IF EXISTS parallel_resume_log.integer_rows;
DROP TABLE IF EXISTS parallel_resume_log.nullable_rows;
DROP TABLE IF EXISTS parallel_resume_log.[string rows.*];
DROP TABLE IF EXISTS parallel_resume_log.finished_rows;
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
CREATE TABLE parallel_resume_log.finished_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
INSERT INTO parallel_resume_log.integer_rows VALUES
    (1, N'v1'), (2, N'v2'), (3, N'v3'), (4, N'v4');
INSERT INTO parallel_resume_log.nullable_rows VALUES
    (1, 1, N'v1'), (2, 2, N'v2'), (3, 3, N'v3');
INSERT INTO parallel_resume_log.[string rows.*] VALUES
    (N'a', 0x01), (N'b', 0x02), (N'c', 0x03);
GO
