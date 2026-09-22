USE [master];

IF DB_ID(N'ape_dts_col_value_conversion_source') IS NOT NULL
BEGIN
    ALTER DATABASE [ape_dts_col_value_conversion_source]
        SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [ape_dts_col_value_conversion_source];
END;

CREATE DATABASE [ape_dts_col_value_conversion_source];
GO

CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[bit_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] bit NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[tinyint_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] tinyint NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[smallint_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] smallint NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[int_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] int NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[bigint_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] bigint NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[real_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] real NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[float_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] float NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[smallmoney_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] smallmoney NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[money_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] money NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[decimal_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] decimal(38, 0) NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[numeric_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] numeric(38, 37) NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[char_latin1_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] char(16) COLLATE Latin1_General_100_CI_AS NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[char_chinese_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] char(16) COLLATE Chinese_PRC_CI_AS NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[varchar_latin1_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] varchar(32) COLLATE Latin1_General_100_CI_AS NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[varchar_chinese_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] varchar(32) COLLATE Chinese_PRC_CI_AS NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[nchar_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] nchar(16) COLLATE Latin1_General_100_CI_AS_SC NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[nvarchar_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] nvarchar(32) COLLATE Latin1_General_100_CI_AS_SC NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[text_latin1_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] text COLLATE Latin1_General_100_CI_AS NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[text_chinese_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] text COLLATE Chinese_PRC_CI_AS NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[ntext_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] ntext COLLATE Latin1_General_100_CI_AS NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[binary_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] binary(8) NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[varbinary_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] varbinary(32) NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[image_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] image NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[uuid_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] uniqueidentifier NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[xml_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] xml NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[date_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] date NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[time_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] time(7) NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[smalldatetime_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] smalldatetime NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[datetime_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] datetime NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[datetime2_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] datetime2(7) NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[offset_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] datetimeoffset(7) NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[rowversion_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] rowversion NOT NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[geometry_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] geometry NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[geography_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] geography NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[hierarchyid_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] hierarchyid NULL);
CREATE TABLE [ape_dts_col_value_conversion_source].[dbo].[bigvariant_value]
    ([case_id] tinyint NOT NULL PRIMARY KEY, [value] [dbo].[BigVariant] NULL);
GO
