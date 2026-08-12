DROP TABLE IF EXISTS resume_test.resume_rows;
DROP TABLE IF EXISTS resume_test.fresh_rows;
DROP TABLE IF EXISTS resume_test.finished_rows;
IF SCHEMA_ID(N'resume_test') IS NULL EXEC(N'CREATE SCHEMA resume_test');
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
GO
