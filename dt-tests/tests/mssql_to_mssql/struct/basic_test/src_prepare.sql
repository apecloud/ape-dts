IF OBJECT_ID(N'struct_it_mssql2mssql_1.constraint_table', N'U') IS NOT NULL
    DROP TABLE struct_it_mssql2mssql_1.constraint_table;
IF OBJECT_ID(N'struct_it_mssql2mssql_1.full_index_type', N'U') IS NOT NULL
    DROP TABLE struct_it_mssql2mssql_1.full_index_type;
IF OBJECT_ID(N'struct_it_mssql2mssql_1.defaults_and_generated', N'U') IS NOT NULL
    DROP TABLE struct_it_mssql2mssql_1.defaults_and_generated;
IF OBJECT_ID(N'struct_it_mssql2mssql_1.full_column_type', N'U') IS NOT NULL
    DROP TABLE struct_it_mssql2mssql_1.full_column_type;
IF OBJECT_ID(N'struct_it_mssql2mssql_1.[match]', N'U') IS NOT NULL
    DROP TABLE struct_it_mssql2mssql_1.[match];
GO
IF SCHEMA_ID(N'struct_it_mssql2mssql_1') IS NOT NULL
    EXEC(N'DROP SCHEMA struct_it_mssql2mssql_1');
GO
EXEC(N'CREATE SCHEMA struct_it_mssql2mssql_1');
GO

CREATE TABLE struct_it_mssql2mssql_1.full_column_type (
    id INT IDENTITY(1, 1) NOT NULL,
    bit_col BIT NULL,
    tinyint_col TINYINT NULL,
    smallint_col SMALLINT NULL,
    int_col INT NULL,
    bigint_col BIGINT NULL,
    decimal_col DECIMAL(18, 4) NULL,
    numeric_col NUMERIC(20, 6) NULL,
    real_col REAL NULL,
    float_col FLOAT(53) NULL,
    money_col MONEY NULL,
    smallmoney_col SMALLMONEY NULL,
    date_col DATE NULL,
    time_col TIME(6) NULL,
    smalldatetime_col SMALLDATETIME NULL,
    datetime_col DATETIME NULL,
    datetime2_col DATETIME2(6) NULL,
    datetimeoffset_col DATETIMEOFFSET(6) NULL,
    char_col CHAR(10) NULL,
    varchar_col VARCHAR(100) NULL,
    varchar_max_col VARCHAR(MAX) NULL,
    nchar_col NCHAR(10) NULL,
    nvarchar_col NVARCHAR(100) NULL,
    nvarchar_max_col NVARCHAR(MAX) NULL,
    binary_col BINARY(16) NULL,
    varbinary_col VARBINARY(100) NULL,
    varbinary_max_col VARBINARY(MAX) NULL,
    uuid_col UNIQUEIDENTIFIER NULL,
    xml_col XML NULL,
    sql_variant_col SQL_VARIANT NULL,
    rowversion_col ROWVERSION NOT NULL,
    CONSTRAINT pk_full_column_type PRIMARY KEY CLUSTERED (id)
);
GO

CREATE TABLE struct_it_mssql2mssql_1.defaults_and_generated (
    id BIGINT IDENTITY(100, 5) NOT NULL,
    code NVARCHAR(40) NOT NULL
        CONSTRAINT df_defaults_and_generated_code DEFAULT (N'ape-dts'),
    created_at DATETIME2(6) NOT NULL
        CONSTRAINT df_defaults_and_generated_created_at DEFAULT (SYSUTCDATETIME()),
    quantity INT NOT NULL CONSTRAINT df_defaults_and_generated_quantity DEFAULT ((1)),
    unit_price DECIMAL(12, 2) NOT NULL,
    total AS (CONVERT(DECIMAL(18, 2), quantity * unit_price)) PERSISTED,
    CONSTRAINT pk_defaults_and_generated PRIMARY KEY NONCLUSTERED (id)
);
GO

CREATE TABLE struct_it_mssql2mssql_1.constraint_table (
    id INT IDENTITY(1, 1) NOT NULL,
    code NVARCHAR(32) NOT NULL,
    quantity INT NOT NULL,
    CONSTRAINT pk_constraint_table PRIMARY KEY CLUSTERED (id),
    CONSTRAINT uq_constraint_table_code UNIQUE NONCLUSTERED (code),
    CONSTRAINT ck_constraint_table_quantity CHECK (quantity > 0)
);
GO

CREATE TABLE struct_it_mssql2mssql_1.full_index_type (
    id INT NOT NULL,
    tenant_id INT NOT NULL,
    email NVARCHAR(255) NULL,
    created_at DATETIME2(6) NOT NULL,
    status TINYINT NOT NULL,
    payload NVARCHAR(MAX) NULL,
    CONSTRAINT pk_full_index_type PRIMARY KEY CLUSTERED (id)
);
GO
CREATE UNIQUE NONCLUSTERED INDEX uq_full_index_type_tenant_email
    ON struct_it_mssql2mssql_1.full_index_type (tenant_id ASC, email ASC)
    WHERE email IS NOT NULL;
CREATE NONCLUSTERED INDEX idx_full_index_type_created_at
    ON struct_it_mssql2mssql_1.full_index_type (created_at DESC)
    INCLUDE (tenant_id, status);
GO

CREATE TABLE struct_it_mssql2mssql_1.[match] (
    [select] INT NOT NULL,
    [table] NVARCHAR(50) NOT NULL,
    [column] NVARCHAR(50) NULL,
    [special_$#@] NVARCHAR(50) NULL,
    CONSTRAINT [pk match] PRIMARY KEY ([select])
);
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'table comment: basic struct coverage',
    @level0type = N'SCHEMA', @level0name = N'struct_it_mssql2mssql_1',
    @level1type = N'TABLE', @level1name = N'full_column_type';
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'column comment with ''quotes'' and special characters',
    @level0type = N'SCHEMA', @level0name = N'struct_it_mssql2mssql_1',
    @level1type = N'TABLE', @level1name = N'full_column_type',
    @level2type = N'COLUMN', @level2name = N'nvarchar_col';
GO
