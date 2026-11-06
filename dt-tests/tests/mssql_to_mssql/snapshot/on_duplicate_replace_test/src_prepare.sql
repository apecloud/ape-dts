USE [ape_dts];
GO
DROP TABLE IF EXISTS [ape_dts].on_duplicate_replace.conflict_rows;
DROP TABLE IF EXISTS [ape_dts].on_duplicate_replace.unique_rows;
DROP TABLE IF EXISTS [ape_dts].on_duplicate_replace.nullable_unique_rows;
DROP TABLE IF EXISTS [ape_dts].on_duplicate_replace.key_only_rows;
DROP TABLE IF EXISTS [ape_dts].on_duplicate_replace.primary_and_unique_rows;
DROP TABLE IF EXISTS [ape_dts].on_duplicate_replace.udt_rows;
IF SCHEMA_ID(N'on_duplicate_replace') IS NULL EXEC(N'CREATE SCHEMA on_duplicate_replace');
CREATE TABLE [ape_dts].on_duplicate_replace.conflict_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE [ape_dts].on_duplicate_replace.unique_rows (
    -- The identity variant is not runnable with replace: MERGE cannot update an
    -- identity column when another unique key matches.
    -- id int IDENTITY(1, 1) NOT NULL,
    id int NOT NULL,
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
IF TYPE_ID(N'dbo.BigVariant') IS NULL
    THROW 50000, 'dbo.BigVariant is not installed', 1;
CREATE TABLE [ape_dts].on_duplicate_replace.udt_rows (
    id int NOT NULL PRIMARY KEY,
    geometry_value geometry NULL,
    geography_value geography NULL,
    hierarchyid_value hierarchyid NULL,
    variant_value [dbo].[BigVariant] NULL
);
GO
