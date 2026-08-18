DROP TABLE IF EXISTS key_structure.no_key_rows;
DROP TABLE IF EXISTS key_structure.single_primary_rows;
DROP TABLE IF EXISTS key_structure.composite_primary_rows;
DROP TABLE IF EXISTS key_structure.unique_rows;
DROP TABLE IF EXISTS key_structure.primary_and_unique_rows;
DROP TABLE IF EXISTS key_structure.composite_unique_rows;
DROP TABLE IF EXISTS key_structure.nullable_composite_unique_rows;
DROP TABLE IF EXISTS key_structure.all_primary_rows;
DROP TABLE IF EXISTS key_structure.default_rows;
DROP TABLE IF EXISTS key_structure.empty_rows;
IF SCHEMA_ID(N'key_structure') IS NULL EXEC(N'CREATE SCHEMA key_structure');
CREATE TABLE key_structure.no_key_rows (
    value_a int NULL,
    value_b nvarchar(30) NULL
);
CREATE TABLE key_structure.single_primary_rows (
    id bigint NOT NULL PRIMARY KEY,
    code nvarchar(30) NULL,
    payload varbinary(30) NULL
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
CREATE TABLE key_structure.primary_and_unique_rows (
    tenant_id int NOT NULL,
    row_id int NOT NULL,
    external_id uniqueidentifier NOT NULL UNIQUE,
    alternate_code nvarchar(30) NULL,
    value nvarchar(30) NULL,
    PRIMARY KEY (tenant_id, row_id)
);
CREATE UNIQUE INDEX ux_primary_and_unique_alternate_code
    ON key_structure.primary_and_unique_rows (alternate_code)
    WHERE alternate_code IS NOT NULL;
CREATE TABLE key_structure.composite_unique_rows (
    row_id int NOT NULL,
    region nvarchar(20) NOT NULL,
    sequence_no int NOT NULL,
    value nvarchar(30) NULL,
    UNIQUE (region, sequence_no)
);
CREATE TABLE key_structure.nullable_composite_unique_rows (
    row_id int NOT NULL PRIMARY KEY,
    key_a int NULL,
    key_b nvarchar(20) NULL,
    value nvarchar(30) NULL
);
CREATE UNIQUE INDEX ux_nullable_composite_key
    ON key_structure.nullable_composite_unique_rows (key_a, key_b)
    WHERE key_a IS NOT NULL AND key_b IS NOT NULL;
CREATE TABLE key_structure.all_primary_rows (
    key_a int NOT NULL,
    key_b nvarchar(20) NOT NULL,
    key_c varbinary(8) NOT NULL,
    PRIMARY KEY (key_a, key_b, key_c)
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
