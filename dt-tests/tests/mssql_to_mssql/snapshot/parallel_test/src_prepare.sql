DROP TABLE IF EXISTS parallel_test.integer_rows;
DROP TABLE IF EXISTS parallel_test.nullable_rows;
DROP TABLE IF EXISTS parallel_test.where_rows;
IF SCHEMA_ID(N'parallel_test') IS NULL EXEC(N'CREATE SCHEMA parallel_test');
CREATE TABLE parallel_test.integer_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE parallel_test.nullable_rows (
    row_id int NOT NULL PRIMARY KEY,
    split_key int NULL,
    value nvarchar(30) NOT NULL
);
CREATE TABLE parallel_test.where_rows (
    id int NOT NULL PRIMARY KEY,
    value int NOT NULL
);
GO
