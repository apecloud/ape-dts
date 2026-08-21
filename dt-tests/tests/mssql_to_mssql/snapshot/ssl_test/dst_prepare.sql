USE [ape_dts];
GO
DROP TABLE IF EXISTS [ape_dts].ssl_test.encrypted_rows;
DROP TABLE IF EXISTS [ape_dts].ssl_test.ssl_test_1;
DROP TABLE IF EXISTS [ape_dts].ssl_test.ssl_test_2;
DROP TABLE IF EXISTS [ape_dts].ssl_test.ssl_test_3;
DROP TABLE IF EXISTS [ape_dts].ssl_test.ssl_test_4;
DROP TABLE IF EXISTS [ape_dts].ssl_test.ssl_test_5;
DROP TABLE IF EXISTS [ape_dts].ssl_test.ssl_test_6;
DROP TABLE IF EXISTS [ape_dts].ssl_test.ssl_test_7;
DROP TABLE IF EXISTS [ape_dts].ssl_test.ssl_test_8;
IF SCHEMA_ID(N'ssl_test') IS NULL EXEC(N'CREATE SCHEMA ssl_test');
CREATE TABLE [ape_dts].ssl_test.ssl_test_1 (id int NOT NULL PRIMARY KEY, value smallint NULL);
CREATE TABLE [ape_dts].ssl_test.ssl_test_2 (id int NOT NULL PRIMARY KEY, value int NULL);
CREATE TABLE [ape_dts].ssl_test.ssl_test_3 (id int NOT NULL PRIMARY KEY, value bigint NULL);
CREATE TABLE [ape_dts].ssl_test.ssl_test_4 (id int NOT NULL PRIMARY KEY, value decimal(20, 4) NULL);
CREATE TABLE [ape_dts].ssl_test.ssl_test_5 (id int NOT NULL PRIMARY KEY, value real NULL);
CREATE TABLE [ape_dts].ssl_test.ssl_test_6 (id int NOT NULL PRIMARY KEY, value float NULL);
CREATE TABLE [ape_dts].ssl_test.ssl_test_7 (id int NOT NULL PRIMARY KEY, value bit NULL);
CREATE TABLE [ape_dts].ssl_test.ssl_test_8 (
    id int NOT NULL PRIMARY KEY, value nvarchar(100) NULL, payload varbinary(100) NULL
);
GO
