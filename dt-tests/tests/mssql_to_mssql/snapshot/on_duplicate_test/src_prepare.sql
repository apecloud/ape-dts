DROP TABLE IF EXISTS on_duplicate_test.conflict_rows;
IF SCHEMA_ID(N'on_duplicate_test') IS NULL EXEC(N'CREATE SCHEMA on_duplicate_test');
CREATE TABLE on_duplicate_test.conflict_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
GO
