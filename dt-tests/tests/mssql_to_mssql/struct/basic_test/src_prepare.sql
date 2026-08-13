IF OBJECT_ID(N'struct_it_mssql2mssql_1.full_column_type_view', N'V') IS NOT NULL
    DROP VIEW struct_it_mssql2mssql_1.full_column_type_view;
IF OBJECT_ID(N'struct_it_mssql2mssql_1.[special_character_$1#@*_table]', N'U') IS NOT NULL
    DROP TABLE struct_it_mssql2mssql_1.[special_character_$1#@*_table];
IF OBJECT_ID(N'struct_it_mssql2mssql_1.case_sensitive_column_name', N'U') IS NOT NULL
    DROP TABLE struct_it_mssql2mssql_1.case_sensitive_column_name;
IF OBJECT_ID(N'struct_it_mssql2mssql_1.special_default_and_comment', N'U') IS NOT NULL
    DROP TABLE struct_it_mssql2mssql_1.special_default_and_comment;
IF OBJECT_ID(N'struct_it_mssql2mssql_1.spatial_column_type', N'U') IS NOT NULL
    DROP TABLE struct_it_mssql2mssql_1.spatial_column_type;
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

-- Align with the MySQL/PG full_column_type tables using SQL Server equivalents.
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
    varchar_col VARCHAR(255) NOT NULL,
    varchar_max_col VARCHAR(MAX) NULL,
    text_col TEXT NULL,
    nchar_col NCHAR(10) NULL,
    nvarchar_col NVARCHAR(255) NULL,
    nvarchar_max_col NVARCHAR(MAX) NULL,
    ntext_col NTEXT NULL,
    binary_col BINARY(16) NULL,
    varbinary_col VARBINARY(255) NULL,
    varbinary_max_col VARBINARY(MAX) NULL,
    image_col IMAGE NULL,
    uuid_col UNIQUEIDENTIFIER NULL,
    xml_col XML NULL,
    sql_variant_col SQL_VARIANT NULL,
    rowversion_col ROWVERSION NOT NULL,
    CONSTRAINT pk_full_column_type PRIMARY KEY CLUSTERED (id)
);
GO

-- SQL Server spatial types corresponding to the spatial coverage in MySQL/PG.
CREATE TABLE struct_it_mssql2mssql_1.spatial_column_type (
    id INT IDENTITY(1, 1) NOT NULL,
    geometry_col GEOMETRY NULL,
    geography_col GEOGRAPHY NULL,
    CONSTRAINT pk_spatial_column_type PRIMARY KEY CLUSTERED (id)
);
GO

-- Literal/expression defaults, identity, and persisted/non-persisted computed columns.
CREATE TABLE struct_it_mssql2mssql_1.defaults_and_generated (
    id BIGINT IDENTITY(100, 5) NOT NULL,
    code NVARCHAR(40) NOT NULL
        CONSTRAINT df_defaults_and_generated_code DEFAULT (N'ape-dts'),
    unicode_text NVARCHAR(100) NOT NULL
        CONSTRAINT df_defaults_and_generated_unicode DEFAULT (N'abc中文''value'),
    enabled BIT NOT NULL
        CONSTRAINT df_defaults_and_generated_enabled DEFAULT ((1)),
    created_at DATETIME2(6) NOT NULL
        CONSTRAINT df_defaults_and_generated_created_at DEFAULT (SYSUTCDATETIME()),
    business_date DATE NOT NULL
        CONSTRAINT df_defaults_and_generated_date DEFAULT (CONVERT(DATE, '19700101')),
    request_id UNIQUEIDENTIFIER NOT NULL
        CONSTRAINT df_defaults_and_generated_request_id DEFAULT (NEWID()),
    quantity INT NOT NULL CONSTRAINT df_defaults_and_generated_quantity DEFAULT ((1)),
    unit_price DECIMAL(12, 2) NOT NULL CONSTRAINT df_defaults_and_generated_price DEFAULT ((1.25)),
    total AS (CONVERT(DECIMAL(18, 2), quantity * unit_price)) PERSISTED,
    quantity_label AS (CONVERT(VARCHAR(20), quantity)),
    CONSTRAINT pk_defaults_and_generated PRIMARY KEY NONCLUSTERED (id)
);
GO

-- Primary/unique/check/not-null coverage, excluding foreign keys by design.
CREATE TABLE struct_it_mssql2mssql_1.constraint_table (
    id INT IDENTITY(1, 1) NOT NULL,
    username NVARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    tenant_id INT NOT NULL,
    age INT NULL,
    status VARCHAR(16) NOT NULL,
    CONSTRAINT pk_constraint_table PRIMARY KEY CLUSTERED (id),
    CONSTRAINT uq_constraint_table_username UNIQUE NONCLUSTERED (username),
    CONSTRAINT uq_constraint_table_tenant_email UNIQUE NONCLUSTERED (tenant_id, email),
    CONSTRAINT ck_constraint_table_age CHECK (age >= 18),
    CONSTRAINT ck_constraint_table_email CHECK (email LIKE '%@%.%'),
    CONSTRAINT ck_constraint_table_status CHECK (status IN ('active', 'disabled'))
);
GO

-- Single, composite, unique, descending, included-column, and filtered indexes.
CREATE TABLE struct_it_mssql2mssql_1.full_index_type (
    id INT NOT NULL,
    unique_col VARCHAR(255) NOT NULL,
    index_col VARCHAR(255) NULL,
    simple_index_col VARCHAR(255) NULL,
    composite_index_col1 VARCHAR(255) NULL,
    composite_index_col2 VARCHAR(255) NULL,
    composite_index_col3 VARCHAR(255) NULL,
    created_at DATETIME2(6) NOT NULL,
    status TINYINT NOT NULL,
    payload NVARCHAR(MAX) NULL,
    CONSTRAINT pk_full_index_type PRIMARY KEY CLUSTERED (id)
);
GO
CREATE UNIQUE NONCLUSTERED INDEX unique_index
    ON struct_it_mssql2mssql_1.full_index_type (unique_col ASC);
CREATE NONCLUSTERED INDEX index_index
    ON struct_it_mssql2mssql_1.full_index_type (index_col ASC);
CREATE NONCLUSTERED INDEX simple_index
    ON struct_it_mssql2mssql_1.full_index_type (simple_index_col ASC);
CREATE NONCLUSTERED INDEX composite_index
    ON struct_it_mssql2mssql_1.full_index_type
       (composite_index_col1 ASC, composite_index_col2 DESC, composite_index_col3 ASC);
CREATE NONCLUSTERED INDEX included_index
    ON struct_it_mssql2mssql_1.full_index_type (created_at DESC)
    INCLUDE (status, unique_col);
CREATE NONCLUSTERED INDEX filtered_index
    ON struct_it_mssql2mssql_1.full_index_type (status ASC)
    WHERE status > 0;
GO

-- Quoted Unicode defaults and table/column comments.
CREATE TABLE struct_it_mssql2mssql_1.special_default_and_comment (
    id INT IDENTITY(1, 1) NOT NULL,
    f_1 NVARCHAR(255) NOT NULL
        CONSTRAINT df_special_default_f1 DEFAULT (N'abc''中文'''),
    CONSTRAINT pk_special_default_and_comment PRIMARY KEY CLUSTERED (id)
);
GO

-- Preserve case-sensitive column spelling even under a case-insensitive database collation.
CREATE TABLE struct_it_mssql2mssql_1.case_sensitive_column_name (
    id INT IDENTITY(1, 1) NOT NULL,
    name VARCHAR(255) NOT NULL CONSTRAINT df_case_name DEFAULT ('jack'),
    Age INT NOT NULL CONSTRAINT df_case_age DEFAULT ((100)),
    GRADE INT NOT NULL CONSTRAINT df_case_grade DEFAULT ((100)),
    CONSTRAINT pk_case_sensitive_column_name PRIMARY KEY CLUSTERED (id)
);
GO

-- Special table/column identifiers matching the PG special-character case.
CREATE TABLE struct_it_mssql2mssql_1.[special_character_$1#@*_table] (
    id INT IDENTITY(1, 1) NOT NULL,
    [column with space] VARCHAR(255) NOT NULL,
    [unique_$#@] VARCHAR(255) NULL,
    check_col VARCHAR(255) NULL,
    CONSTRAINT [pk special character] PRIMARY KEY CLUSTERED (id),
    CONSTRAINT [uq special character] UNIQUE NONCLUSTERED ([unique_$#@]),
    CONSTRAINT [ck special character] CHECK (LEN(check_col) > 3)
);
GO

-- SQL Server keywords and indexes on quoted identifiers.
CREATE TABLE struct_it_mssql2mssql_1.[match] (
    select_id INT IDENTITY(1, 1) NOT NULL,
    [table] NVARCHAR(255) NOT NULL,
    [column] NVARCHAR(255) NOT NULL,
    [offset] INT NOT NULL,
    unique_col VARCHAR(255) NULL,
    [match] INT NULL,
    check_col INT NULL,
    constraint_col INT NULL,
    [special_$#@] NVARCHAR(50) NULL,
    CONSTRAINT [pk match] PRIMARY KEY CLUSTERED (select_id)
);
GO
CREATE NONCLUSTERED INDEX idx_index_on_index
    ON struct_it_mssql2mssql_1.[match] ([offset] ASC);
CREATE NONCLUSTERED INDEX idx_key_col
    ON struct_it_mssql2mssql_1.[match] ([match] ASC);
CREATE UNIQUE NONCLUSTERED INDEX uniq_unique_col
    ON struct_it_mssql2mssql_1.[match] (unique_col ASC);
GO

-- Views are intentionally outside the MSSQL struct task's table scope.
CREATE VIEW struct_it_mssql2mssql_1.full_column_type_view
AS SELECT * FROM struct_it_mssql2mssql_1.full_column_type;
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Comment on full_column_type.',
    @level0type = N'SCHEMA', @level0name = N'struct_it_mssql2mssql_1',
    @level1type = N'TABLE', @level1name = N'full_column_type';
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Comment on full_column_type.id.',
    @level0type = N'SCHEMA', @level0name = N'struct_it_mssql2mssql_1',
    @level1type = N'TABLE', @level1name = N'full_column_type',
    @level2type = N'COLUMN', @level2name = N'id';
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Comment on full_index_type.',
    @level0type = N'SCHEMA', @level0name = N'struct_it_mssql2mssql_1',
    @level1type = N'TABLE', @level1name = N'full_index_type';
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Comment on full_index_type.id.',
    @level0type = N'SCHEMA', @level0name = N'struct_it_mssql2mssql_1',
    @level1type = N'TABLE', @level1name = N'full_index_type',
    @level2type = N'COLUMN', @level2name = N'id';
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'中文注释''special_default_and_comment''',
    @level0type = N'SCHEMA', @level0name = N'struct_it_mssql2mssql_1',
    @level1type = N'TABLE', @level1name = N'special_default_and_comment';
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'中文注释''f_1'' #?&^%$#@<>!',
    @level0type = N'SCHEMA', @level0name = N'struct_it_mssql2mssql_1',
    @level1type = N'TABLE', @level1name = N'special_default_and_comment',
    @level2type = N'COLUMN', @level2name = N'f_1';
GO
