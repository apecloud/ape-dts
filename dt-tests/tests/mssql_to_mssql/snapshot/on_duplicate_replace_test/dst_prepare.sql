USE [ape_dts];
GO
DROP TABLE IF EXISTS [ape_dts].on_duplicate_replace.conflict_rows;
DROP TABLE IF EXISTS [ape_dts].on_duplicate_replace.unique_rows;
DROP TABLE IF EXISTS [ape_dts].on_duplicate_replace.nullable_unique_rows;
DROP TABLE IF EXISTS [ape_dts].on_duplicate_replace.key_only_rows;
DROP TABLE IF EXISTS [ape_dts].on_duplicate_replace.primary_and_unique_rows;
IF SCHEMA_ID(N'on_duplicate_replace') IS NULL EXEC(N'CREATE SCHEMA on_duplicate_replace');
CREATE TABLE [ape_dts].on_duplicate_replace.conflict_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE [ape_dts].on_duplicate_replace.unique_rows (
    id int IDENTITY(1, 1) NOT NULL,
    code nvarchar(30) NOT NULL UNIQUE,
    value nvarchar(30) NOT NULL
);
CREATE TABLE [ape_dts].on_duplicate_replace.nullable_unique_rows (
    id int NOT NULL,
    code nvarchar(30) NULL UNIQUE,
    value nvarchar(30) NOT NULL
);
CREATE TABLE [ape_dts].on_duplicate_replace.key_only_rows (
    id int NOT NULL PRIMARY KEY
);
CREATE TABLE [ape_dts].on_duplicate_replace.primary_and_unique_rows (
    id int NOT NULL PRIMARY KEY,
    code nvarchar(30) NOT NULL UNIQUE,
    value nvarchar(30) NULL
);
INSERT INTO [ape_dts].on_duplicate_replace.conflict_rows VALUES
    (1, N'target-existing'), (4, N'target-primary-conflict');
SET IDENTITY_INSERT [ape_dts].on_duplicate_replace.unique_rows ON;
INSERT INTO [ape_dts].on_duplicate_replace.unique_rows (id, code, value)
VALUES (99, N'code-1', N'target-unique-conflict');
SET IDENTITY_INSERT [ape_dts].on_duplicate_replace.unique_rows OFF;
INSERT INTO [ape_dts].on_duplicate_replace.nullable_unique_rows VALUES
    (99, NULL, N'target-null-conflict');
INSERT INTO [ape_dts].on_duplicate_replace.key_only_rows VALUES (1), (4);
INSERT INTO [ape_dts].on_duplicate_replace.primary_and_unique_rows VALUES
    (1, N'target-code-1', N'target-primary-conflict'),
    (4, N'target-code-4', N'target-primary-conflict');
GO
