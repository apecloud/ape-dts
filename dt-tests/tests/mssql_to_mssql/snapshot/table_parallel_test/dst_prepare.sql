DROP TABLE IF EXISTS table_parallel_test.identity_rows;
DROP TABLE IF EXISTS table_parallel_test.regular_rows;
DROP TABLE IF EXISTS table_parallel_test.string_key_rows;
IF SCHEMA_ID(N'table_parallel_test') IS NULL EXEC(N'CREATE SCHEMA table_parallel_test');
CREATE TABLE table_parallel_test.identity_rows (
    id int IDENTITY(1, 1) NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE table_parallel_test.regular_rows (
    id bigint NOT NULL PRIMARY KEY,
    value decimal(20, 6) NULL
);
CREATE TABLE table_parallel_test.string_key_rows (
    code nvarchar(30) NOT NULL PRIMARY KEY,
    value varbinary(32) NULL
);
GO
