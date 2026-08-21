USE [ape_dts];
GO
DROP TABLE IF EXISTS [ape_dts].parallel_test.nullable_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_test.where_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_test.integer_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_test.integer_more_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_test.string_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_test.no_key_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_test.nullable_partition_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_test.unique_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_test.all_null_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_test.where_condition_1;
DROP TABLE IF EXISTS [ape_dts].parallel_test.where_condition_2;
DROP TABLE IF EXISTS [ape_dts].parallel_test.fallback_no_key_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_test.fallback_primary_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_test.bigint_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_test.decimal_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_test.date_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_test.binary_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_test.guid_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_test.datetimeoffset_rows;
DROP TABLE IF EXISTS [ape_dts].parallel_test.composite_rows;
IF SCHEMA_ID(N'parallel_test') IS NULL EXEC(N'CREATE SCHEMA parallel_test');
CREATE TABLE [ape_dts].parallel_test.integer_rows (id int NOT NULL PRIMARY KEY, value int NULL);
CREATE TABLE [ape_dts].parallel_test.integer_more_rows (id int NOT NULL PRIMARY KEY, value int NULL);
CREATE TABLE [ape_dts].parallel_test.string_rows (id nvarchar(255) NOT NULL PRIMARY KEY, value int NULL);
CREATE TABLE [ape_dts].parallel_test.no_key_rows (id int NOT NULL, value int NULL);
CREATE TABLE [ape_dts].parallel_test.nullable_partition_rows (
    row_id int NOT NULL PRIMARY KEY, id int NULL, value int NULL
);
CREATE TABLE [ape_dts].parallel_test.unique_rows (row_id int NOT NULL, id int NULL UNIQUE, value int NULL);
CREATE TABLE [ape_dts].parallel_test.all_null_rows (id int NULL, value int NULL);
CREATE TABLE [ape_dts].parallel_test.where_condition_1 (id int NOT NULL PRIMARY KEY, value int NOT NULL);
CREATE TABLE [ape_dts].parallel_test.where_condition_2 (id int NOT NULL PRIMARY KEY, value int NOT NULL);
CREATE TABLE [ape_dts].parallel_test.fallback_no_key_rows (id int NOT NULL, value int NULL);
CREATE TABLE [ape_dts].parallel_test.fallback_primary_rows (id int NOT NULL PRIMARY KEY, value int NULL);
CREATE TABLE [ape_dts].parallel_test.bigint_rows (id bigint NOT NULL PRIMARY KEY, value nvarchar(30) NULL);
CREATE TABLE [ape_dts].parallel_test.decimal_rows (
    id decimal(20, 4) NOT NULL PRIMARY KEY, value nvarchar(30) NULL
);
CREATE TABLE [ape_dts].parallel_test.date_rows (id date NOT NULL PRIMARY KEY, value nvarchar(30) NULL);
CREATE TABLE [ape_dts].parallel_test.binary_rows (
    id varbinary(32) NOT NULL PRIMARY KEY, value nvarchar(30) NULL
);
CREATE TABLE [ape_dts].parallel_test.guid_rows (
    id uniqueidentifier NOT NULL PRIMARY KEY, value nvarchar(30) NULL
);
CREATE TABLE [ape_dts].parallel_test.datetimeoffset_rows (
    id datetimeoffset(7) NOT NULL PRIMARY KEY, value nvarchar(30) NULL
);
CREATE TABLE [ape_dts].parallel_test.composite_rows (
    tenant_id int NOT NULL, row_id int NOT NULL, value nvarchar(30) NULL,
    PRIMARY KEY (tenant_id, row_id)
);
GO
