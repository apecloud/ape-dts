DROP TABLE IF EXISTS on_duplicate_replace.conflict_rows;
DROP TABLE IF EXISTS on_duplicate_replace.unique_rows;
DROP TABLE IF EXISTS on_duplicate_replace.nullable_unique_rows;
DROP TABLE IF EXISTS on_duplicate_replace.key_only_rows;
DROP TABLE IF EXISTS on_duplicate_replace.primary_and_unique_rows;
IF SCHEMA_ID(N'on_duplicate_replace') IS NULL EXEC(N'CREATE SCHEMA on_duplicate_replace');
CREATE TABLE on_duplicate_replace.conflict_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE on_duplicate_replace.unique_rows (
    id int IDENTITY(1, 1) NOT NULL,
    code nvarchar(30) NOT NULL UNIQUE,
    value nvarchar(30) NOT NULL
);
CREATE TABLE on_duplicate_replace.nullable_unique_rows (
    id int NOT NULL,
    code nvarchar(30) NULL UNIQUE,
    value nvarchar(30) NOT NULL
);
CREATE TABLE on_duplicate_replace.key_only_rows (
    id int NOT NULL PRIMARY KEY
);
CREATE TABLE on_duplicate_replace.primary_and_unique_rows (
    id int NOT NULL PRIMARY KEY,
    code nvarchar(30) NOT NULL UNIQUE,
    value nvarchar(30) NULL
);
GO
