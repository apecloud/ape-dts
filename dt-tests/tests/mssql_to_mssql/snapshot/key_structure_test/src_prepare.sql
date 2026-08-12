DROP TABLE IF EXISTS key_structure.no_key_rows;
DROP TABLE IF EXISTS key_structure.composite_primary_rows;
DROP TABLE IF EXISTS key_structure.unique_rows;
DROP TABLE IF EXISTS key_structure.default_rows;
DROP TABLE IF EXISTS key_structure.empty_rows;
IF SCHEMA_ID(N'key_structure') IS NULL EXEC(N'CREATE SCHEMA key_structure');
CREATE TABLE key_structure.no_key_rows (
    value_a int NULL,
    value_b nvarchar(30) NULL
);
CREATE TABLE key_structure.composite_primary_rows (
    tenant_id int NOT NULL,
    row_id int NOT NULL,
    value nvarchar(30) NULL,
    PRIMARY KEY (tenant_id, row_id)
);
CREATE TABLE key_structure.unique_rows (
    id int NULL,
    external_id uniqueidentifier NOT NULL,
    alternate_code nvarchar(30) NOT NULL,
    value int NULL,
    UNIQUE (external_id),
    UNIQUE (alternate_code)
);
CREATE TABLE key_structure.default_rows (
    id int NOT NULL PRIMARY KEY,
    default_text nvarchar(30) NOT NULL DEFAULT N'default text',
    default_number int NOT NULL DEFAULT 42,
    nullable_value nvarchar(30) NULL
);
CREATE TABLE key_structure.empty_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NULL
);
GO
