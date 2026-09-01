USE [master];

IF DB_ID(N'ape_dts_col_value_conversion_destination') IS NOT NULL
BEGIN
    ALTER DATABASE [ape_dts_col_value_conversion_destination]
        SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [ape_dts_col_value_conversion_destination];
END;

CREATE DATABASE [ape_dts_col_value_conversion_destination];
GO

CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[bit_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] bit NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[tinyint_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] tinyint NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[smallint_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] smallint NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[int_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] int NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[bigint_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] bigint NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[real_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] real NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[float_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] float NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[smallmoney_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] smallmoney NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[money_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] money NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[decimal_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] decimal(38, 0) NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[numeric_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] numeric(38, 37) NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[char_latin1_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] char(16) COLLATE Latin1_General_100_CI_AS NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[char_chinese_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] char(16) COLLATE Chinese_PRC_CI_AS NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[varchar_latin1_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] varchar(32) COLLATE Latin1_General_100_CI_AS NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[varchar_chinese_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] varchar(32) COLLATE Chinese_PRC_CI_AS NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[nchar_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] nchar(16) COLLATE Latin1_General_100_CI_AS_SC NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[nvarchar_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] nvarchar(32) COLLATE Latin1_General_100_CI_AS_SC NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[text_latin1_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] text COLLATE Latin1_General_100_CI_AS NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[text_chinese_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] text COLLATE Chinese_PRC_CI_AS NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[ntext_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] ntext COLLATE Latin1_General_100_CI_AS NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[binary_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] binary(8) NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[varbinary_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] varbinary(32) NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[image_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] image NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[uuid_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] uniqueidentifier NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[xml_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] xml NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[date_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] date NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[time_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] time(7) NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[smalldatetime_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] smalldatetime NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[datetime_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] datetime NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[datetime2_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] datetime2(7) NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[offset_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] datetimeoffset(7) NULL);
CREATE TABLE [ape_dts_col_value_conversion_destination].[dbo].[rowversion_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] binary(8) NOT NULL);
GO
