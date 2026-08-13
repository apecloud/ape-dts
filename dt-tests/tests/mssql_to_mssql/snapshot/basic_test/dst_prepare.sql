DROP TABLE IF EXISTS dbo.basic_test;
DROP TABLE IF EXISTS basic_test.fk_child;
DROP TABLE IF EXISTS basic_test.fk_parent;
DROP TABLE IF EXISTS basic_test.basic_types;
DROP TABLE IF EXISTS basic_test.no_pk_no_uk;
DROP TABLE IF EXISTS basic_test.one_pk_no_uk;
DROP TABLE IF EXISTS basic_test.no_pk_one_uk;
DROP TABLE IF EXISTS basic_test.no_pk_multi_uk;
DROP TABLE IF EXISTS basic_test.one_pk_multi_uk;
DROP TABLE IF EXISTS basic_test.numeric_table;
DROP TABLE IF EXISTS basic_test.date_time_table;
DROP TABLE IF EXISTS basic_test.string_binary_table;
DROP TABLE IF EXISTS basic_test.[col has special character];
DROP TABLE IF EXISTS basic_test.ignore_cols_1;
DROP TABLE IF EXISTS basic_test.ignore_cols_2;
DROP TABLE IF EXISTS [Upper_Case_DB].[Upper_Case_TB];
DROP TABLE IF EXISTS basic_test.where_condition_1;
DROP TABLE IF EXISTS basic_test.where_condition_2;
DROP TABLE IF EXISTS basic_test.where_condition_3;
DROP TABLE IF EXISTS basic_test.composite_pk_table;
DROP TABLE IF EXISTS basic_test.composite_unique_key_table;
DROP TABLE IF EXISTS basic_test.nullable_composite_unique_key_table;
DROP TABLE IF EXISTS basic_test.multi_primary_and_single_unique_table;
DROP TABLE IF EXISTS basic_test.all_pks;
DROP TABLE IF EXISTS basic_test.tbl_1;
DROP TABLE IF EXISTS basic_test.tbl_2;
DROP TABLE IF EXISTS basic_test.tbl_3;
DROP TABLE IF EXISTS basic_test.tbl_4;
DROP TABLE IF EXISTS basic_test.tbl_5;
IF SCHEMA_ID(N'basic_test') IS NULL EXEC(N'CREATE SCHEMA basic_test');
IF SCHEMA_ID(N'Upper_Case_DB') IS NULL EXEC(N'CREATE SCHEMA [Upper_Case_DB]');

CREATE TABLE basic_test.basic_types (
    id int IDENTITY(1, 1) NOT NULL PRIMARY KEY,
    enabled bit NOT NULL,
    amount decimal(18, 4) NOT NULL,
    name nvarchar(100) NOT NULL,
    note nvarchar(100) NULL,
    payload varbinary(32) NULL,
    event_date date NOT NULL,
    event_time datetime2(7) NOT NULL,
    external_id uniqueidentifier NOT NULL
);
CREATE TABLE basic_test.no_pk_no_uk (
    tiny_value tinyint NULL, small_value smallint NULL, int_value int NULL,
    big_value bigint NULL, decimal_value decimal(20, 4) NULL, real_value real NULL,
    float_value float NULL, bit_value bit NULL, date_value date NULL,
    time_value time(7) NULL, datetime_value datetime2(7) NULL,
    text_value nvarchar(100) NULL, binary_value varbinary(32) NULL,
    uuid_value uniqueidentifier NULL
);
CREATE TABLE basic_test.one_pk_no_uk (
    id int NOT NULL PRIMARY KEY, nullable_int int NULL, amount decimal(20, 4) NULL,
    text_value nvarchar(100) NULL, binary_value varbinary(32) NULL,
    datetime_value datetime2(7) NULL
);
CREATE TABLE basic_test.no_pk_one_uk (
    row_value int NULL, code nvarchar(30) NOT NULL UNIQUE,
    amount decimal(20, 4) NULL, text_value nvarchar(100) NULL
);
CREATE TABLE basic_test.no_pk_multi_uk (
    row_value int NULL, code nvarchar(30) NOT NULL,
    external_id uniqueidentifier NOT NULL, group_id int NOT NULL,
    sequence_id int NOT NULL, payload varbinary(32) NULL,
    UNIQUE (code), UNIQUE (external_id), UNIQUE (group_id, sequence_id)
);
CREATE TABLE basic_test.one_pk_multi_uk (
    id int NOT NULL PRIMARY KEY, code nvarchar(30) NOT NULL UNIQUE,
    external_id uniqueidentifier NOT NULL UNIQUE, group_id int NOT NULL,
    sequence_id int NOT NULL, value nvarchar(100) NULL,
    UNIQUE (group_id, sequence_id)
);
CREATE TABLE basic_test.numeric_table (
    id int NOT NULL PRIMARY KEY, bit_value bit NULL, tiny_value tinyint NULL,
    small_value smallint NULL, int_value int NULL, big_value bigint NULL,
    real_value real NULL, float_value float NULL, smallmoney_value smallmoney NULL,
    money_value money NULL, decimal_value decimal(38, 4) NULL,
    numeric_value numeric(20, 6) NULL
);
CREATE TABLE basic_test.date_time_table (
    id int NOT NULL PRIMARY KEY, date_value date NULL, time_value time(7) NULL,
    smalldatetime_value smalldatetime NULL, datetime_value datetime NULL,
    datetime2_value datetime2(7) NULL, datetimeoffset_value datetimeoffset(7) NULL
);
CREATE TABLE basic_test.string_binary_table (
    id int NOT NULL PRIMARY KEY, char_value char(8) NULL,
    varchar_value varchar(100) NULL, nchar_value nchar(8) NULL,
    nvarchar_value nvarchar(100) NULL, max_text_value nvarchar(max) NULL,
    binary_value binary(8) NULL, varbinary_value varbinary(max) NULL,
    xml_value xml NULL,
    json_value nvarchar(max) NULL CHECK (json_value IS NULL OR ISJSON(json_value) = 1)
);
CREATE TABLE basic_test.[col has special character] (
    [p:k] int NOT NULL PRIMARY KEY, [select] nvarchar(100) NULL,
    [col,2] nvarchar(100) NULL, [col.3] nvarchar(100) NULL,
    [col with space] nvarchar(100) NULL, [col]]5] nvarchar(100) NULL
);
CREATE TABLE basic_test.ignore_cols_1 (
    id int NOT NULL PRIMARY KEY, keep_value int NULL,
    ignored_value_1 int NULL, ignored_value_2 int NULL
);
CREATE TABLE basic_test.ignore_cols_2 (
    id int NOT NULL PRIMARY KEY, keep_value int NULL,
    also_keep_value int NULL, ignored_value int NULL
);
CREATE TABLE [Upper_Case_DB].[Upper_Case_TB] (
    [Id] int NOT NULL PRIMARY KEY, [FIELD_1] int NOT NULL,
    [field_2] int NOT NULL, [Field_3] int NOT NULL, [field_4] int NULL,
    UNIQUE ([FIELD_1], [field_2], [Field_3])
);
CREATE TABLE basic_test.where_condition_1 (id int NOT NULL PRIMARY KEY, value int NOT NULL);
CREATE TABLE basic_test.where_condition_2 (id int NOT NULL PRIMARY KEY, value int NOT NULL);
CREATE TABLE basic_test.where_condition_3 (id int NOT NULL PRIMARY KEY, value int NOT NULL);
CREATE TABLE basic_test.fk_parent (
    id int NOT NULL PRIMARY KEY, code int NOT NULL UNIQUE, value nvarchar(30) NULL
);
CREATE TABLE basic_test.fk_child (
    id int NOT NULL PRIMARY KEY, parent_code int NOT NULL, value nvarchar(30) NULL
);
CREATE TABLE basic_test.composite_pk_table (
    pk1 int NOT NULL, pk2 nvarchar(10) NOT NULL, value int NULL,
    PRIMARY KEY (pk1, pk2)
);
CREATE TABLE basic_test.composite_unique_key_table (
    uk1 int NOT NULL, uk2 nvarchar(10) NOT NULL, value int NULL,
    UNIQUE (uk1, uk2)
);
CREATE TABLE basic_test.nullable_composite_unique_key_table (
    value int NULL, uk2 nvarchar(10) NULL, uk1 int NULL, UNIQUE (uk1, uk2)
);
CREATE TABLE basic_test.multi_primary_and_single_unique_table (
    pk1 int NOT NULL, pk2 nvarchar(10) NOT NULL, uk1 int NOT NULL UNIQUE,
    uk2 nvarchar(10) NOT NULL UNIQUE, value int NULL, PRIMARY KEY (pk1, pk2)
);
CREATE TABLE basic_test.all_pks (
    pk1 int NOT NULL, pk2 int NOT NULL, pk3 int NOT NULL, PRIMARY KEY (pk1, pk2, pk3)
);
CREATE TABLE basic_test.tbl_1 (
    id bigint NOT NULL PRIMARY KEY, code varchar(50) NOT NULL, name nvarchar(100) NULL
);
CREATE UNIQUE INDEX tbl_1_code_uidx ON basic_test.tbl_1 (code);
CREATE TABLE basic_test.tbl_2 (code varchar(21) NULL, name nvarchar(30) NOT NULL);
CREATE UNIQUE INDEX tbl_2_name_uidx ON basic_test.tbl_2 (name);
CREATE TABLE basic_test.tbl_3 (
    id int NOT NULL PRIMARY KEY, code varchar(21) NOT NULL,
    name nvarchar(30) NULL, CONSTRAINT tbl_3_code_uk UNIQUE (code)
);
CREATE TABLE basic_test.tbl_4 (
    code varchar(21) NOT NULL, name nvarchar(30) NOT NULL,
    CONSTRAINT tbl_4_code_name_uk UNIQUE (code, name)
);
CREATE TABLE basic_test.tbl_5 (code varchar(21) NULL, name nvarchar(30) NULL);
GO
