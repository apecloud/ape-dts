DROP TABLE IF EXISTS ssl_test.encrypted_rows;
IF SCHEMA_ID(N'ssl_test') IS NULL EXEC(N'CREATE SCHEMA ssl_test');
CREATE TABLE ssl_test.encrypted_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(100) NULL,
    payload varbinary(100) NULL
);
GO
