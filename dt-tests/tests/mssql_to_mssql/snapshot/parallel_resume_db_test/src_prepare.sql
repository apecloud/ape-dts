DROP TABLE IF EXISTS parallel_resume_db.integer_rows;
DROP TABLE IF EXISTS parallel_resume_db.nullable_rows;
DROP TABLE IF EXISTS parallel_resume_db.[string rows.*];
DROP TABLE IF EXISTS parallel_resume_db.finished_rows;
IF SCHEMA_ID(N'parallel_resume_db') IS NULL EXEC(N'CREATE SCHEMA parallel_resume_db');
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
GO
