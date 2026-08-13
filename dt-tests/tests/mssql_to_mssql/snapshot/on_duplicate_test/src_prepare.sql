DROP TABLE IF EXISTS on_duplicate_test.conflict_rows;
IF SCHEMA_ID(N'on_duplicate_test') IS NULL EXEC(N'CREATE SCHEMA on_duplicate_test');
CREATE TABLE on_duplicate_test.conflict_rows (
    id int NOT NULL PRIMARY KEY,
    small_value smallint NULL,
    big_value bigint NULL,
    decimal_value decimal(20, 4) NULL,
    float_value float NULL,
    bit_value bit NULL,
    date_value date NULL,
    time_value time(7) NULL,
    datetime_value datetime2(7) NULL,
    text_value nvarchar(100) NULL,
    binary_value varbinary(100) NULL,
    uuid_value uniqueidentifier NULL,
    xml_value xml NULL,
    json_value nvarchar(max) NULL CHECK (json_value IS NULL OR ISJSON(json_value) = 1),
    alternate_a int NULL,
    alternate_b nvarchar(20) NULL,
    UNIQUE (alternate_a, alternate_b)
);
GO
