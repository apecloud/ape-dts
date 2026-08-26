IF DB_ID(N'struct_it_mssql2mssql_1') IS NOT NULL
BEGIN
    ALTER DATABASE [struct_it_mssql2mssql_1] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [struct_it_mssql2mssql_1];
END;
CREATE DATABASE [struct_it_mssql2mssql_1];
GO

-- Align with the MySQL/PG full_column_type tables using SQL Server equivalents.
CREATE TABLE [struct_it_mssql2mssql_1].dbo.full_column_type (
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
    hierarchyid_col HIERARCHYID NULL,
    rowversion_col ROWVERSION NOT NULL,
    CONSTRAINT pk_full_column_type PRIMARY KEY CLUSTERED (id)
);
CREATE PRIMARY XML INDEX pxml_full_column_type
    ON [struct_it_mssql2mssql_1].dbo.full_column_type (xml_col);
CREATE XML INDEX pxml_full_column_type_path
    ON [struct_it_mssql2mssql_1].dbo.full_column_type (xml_col)
    USING XML INDEX pxml_full_column_type FOR PATH;
CREATE XML INDEX pxml_full_column_type_value
    ON [struct_it_mssql2mssql_1].dbo.full_column_type (xml_col)
    USING XML INDEX pxml_full_column_type FOR VALUE;
CREATE XML INDEX pxml_full_column_type_property
    ON [struct_it_mssql2mssql_1].dbo.full_column_type (xml_col)
    USING XML INDEX pxml_full_column_type FOR PROPERTY;
GO

-- SQL Server 2025 types are kept visible but disabled for the SQL Server 2022 CI image.
-- ALTER TABLE [struct_it_mssql2mssql_1].dbo.full_column_type ADD json_col JSON NULL;
-- ALTER TABLE [struct_it_mssql2mssql_1].dbo.full_column_type ADD vector_col VECTOR(3) NULL;
-- CURSOR and TABLE are transient variable/return types and cannot be table columns.
-- TIMESTAMP is the deprecated synonym of ROWVERSION and cannot coexist with another
-- ROWVERSION column in the same table. SYSNAME is an alias type over NVARCHAR(128).

-- SQL Server spatial types corresponding to the spatial coverage in MySQL/PG.
CREATE TABLE [struct_it_mssql2mssql_1].dbo.spatial_column_type (
    id INT IDENTITY(1, 1) NOT NULL,
    geometry_col GEOMETRY NULL,
    geography_col GEOGRAPHY NULL,
    CONSTRAINT pk_spatial_column_type PRIMARY KEY CLUSTERED (id)
);
CREATE SPATIAL INDEX spatial_geography_col
    ON [struct_it_mssql2mssql_1].dbo.spatial_column_type (geography_col);
GO

-- Literal/expression defaults, identity, and persisted/non-persisted computed columns.
CREATE TABLE [struct_it_mssql2mssql_1].dbo.defaults_and_generated (
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

-- DEFAULT constraint syntax: minimal, named, expression, ALTER, and WITH VALUES.
CREATE TABLE [struct_it_mssql2mssql_1].dbo.default_constraint_syntax (
    id INT NOT NULL,
    inline_unnamed INT DEFAULT 0 NULL,
    inline_named NVARCHAR(40)
        CONSTRAINT df_default_syntax_inline_named DEFAULT (N'named') NOT NULL,
    inline_expression DATE
        CONSTRAINT df_default_syntax_inline_expression
        DEFAULT (CONVERT(DATE, '20000101')) NOT NULL,
    inline_niladic NVARCHAR(128) DEFAULT USER NULL,
    alter_unnamed INT NULL,
    alter_named DATETIME2(6) NULL,
    alter_named_with_values INT NULL
);
ALTER TABLE [struct_it_mssql2mssql_1].dbo.default_constraint_syntax
    ADD DEFAULT ((2)) FOR alter_unnamed;
ALTER TABLE [struct_it_mssql2mssql_1].dbo.default_constraint_syntax
    ADD CONSTRAINT df_default_syntax_alter_named
        DEFAULT (SYSUTCDATETIME()) FOR alter_named;
ALTER TABLE [struct_it_mssql2mssql_1].dbo.default_constraint_syntax
    ADD CONSTRAINT df_default_syntax_alter_with_values
        DEFAULT ((4)) FOR alter_named_with_values WITH VALUES;
ALTER TABLE [struct_it_mssql2mssql_1].dbo.default_constraint_syntax
    ADD added_with_values INT
        CONSTRAINT df_default_syntax_with_values DEFAULT ((3)) WITH VALUES NULL;
GO

-- Dedicated coverage for SQL Server's non-writable rowversion type.
CREATE TABLE [struct_it_mssql2mssql_1].dbo.rowversion_type (
    id BIGINT NOT NULL,
    row_version ROWVERSION NOT NULL,
    CONSTRAINT pk_rowversion_type PRIMARY KEY CLUSTERED (id)
);
GO

-- Primary/unique/check/not-null coverage, excluding foreign keys by design.
CREATE TABLE [struct_it_mssql2mssql_1].dbo.constraint_table (
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

-- Constraint syntax: named/unnamed column and table constraints, plus ALTER TABLE.
CREATE TABLE [struct_it_mssql2mssql_1].dbo.constraint_syntax_variants (
    id INT NOT NULL PRIMARY KEY,
    inline_unique INT NULL UNIQUE,
    inline_check INT NULL CHECK (inline_check IS NULL OR inline_check >= 0),
    inline_named_unique INT NULL
        CONSTRAINT uq_constraint_syntax_inline UNIQUE,
    inline_named_check INT NULL
        CONSTRAINT ck_constraint_syntax_inline
        CHECK (inline_named_check IS NULL OR inline_named_check <> 0),
    table_unnamed_unique INT NOT NULL,
    table_unnamed_check INT NULL,
    named_unique_a INT NOT NULL,
    named_unique_b INT NOT NULL,
    named_check INT NULL,
    alter_unique_a INT NOT NULL,
    alter_unique_b INT NOT NULL,
    alter_check INT NULL,
    UNIQUE (table_unnamed_unique),
    CHECK (table_unnamed_check IS NULL OR table_unnamed_check <= 1000),
    CONSTRAINT uq_constraint_syntax_named
        UNIQUE NONCLUSTERED (named_unique_a ASC, named_unique_b DESC),
    CONSTRAINT ck_constraint_syntax_named
        CHECK NOT FOR REPLICATION (named_check IS NULL OR named_check > 0)
);
ALTER TABLE [struct_it_mssql2mssql_1].dbo.constraint_syntax_variants
    ADD CONSTRAINT uq_constraint_syntax_alter
        UNIQUE NONCLUSTERED (alter_unique_a ASC, alter_unique_b DESC);
ALTER TABLE [struct_it_mssql2mssql_1].dbo.constraint_syntax_variants WITH CHECK
    ADD CONSTRAINT ck_constraint_syntax_alter
        CHECK (alter_check IS NULL OR alter_check BETWEEN 0 AND 100);

CREATE TABLE [struct_it_mssql2mssql_1].dbo.constraint_alter_primary (
    id BIGINT NOT NULL,
    version_no INT NOT NULL,
    payload NVARCHAR(100) NULL
);
ALTER TABLE [struct_it_mssql2mssql_1].dbo.constraint_alter_primary
    ADD CONSTRAINT pk_constraint_syntax_alter
        PRIMARY KEY NONCLUSTERED (id ASC, version_no DESC);

CREATE TABLE [struct_it_mssql2mssql_1].dbo.constraint_inline_primary (
    id BIGINT NOT NULL
        CONSTRAINT pk_constraint_syntax_inline PRIMARY KEY NONCLUSTERED,
    payload NVARCHAR(100) NULL
);
GO

-- Single, composite, unique, descending, included-column, and filtered indexes.
CREATE TABLE [struct_it_mssql2mssql_1].dbo.full_index_type (
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
    ON [struct_it_mssql2mssql_1].dbo.full_index_type (unique_col ASC);
CREATE NONCLUSTERED INDEX index_index
    ON [struct_it_mssql2mssql_1].dbo.full_index_type (index_col ASC);
CREATE NONCLUSTERED INDEX simple_index
    ON [struct_it_mssql2mssql_1].dbo.full_index_type (simple_index_col ASC);
CREATE NONCLUSTERED INDEX composite_index
    ON [struct_it_mssql2mssql_1].dbo.full_index_type
       (composite_index_col1 ASC, composite_index_col2 DESC, composite_index_col3 ASC);
CREATE NONCLUSTERED INDEX included_index
    ON [struct_it_mssql2mssql_1].dbo.full_index_type (created_at DESC)
    INCLUDE (status, unique_col);
CREATE NONCLUSTERED INDEX filtered_index
    ON [struct_it_mssql2mssql_1].dbo.full_index_type (status ASC)
    WHERE status > 0;
CREATE NONCLUSTERED INDEX disabled_index
    ON [struct_it_mssql2mssql_1].dbo.full_index_type (index_col ASC);
ALTER INDEX disabled_index
    ON [struct_it_mssql2mssql_1].dbo.full_index_type DISABLE;
GO

-- Index syntax: column-level, table-level, minimal CREATE INDEX, and richer forms.
CREATE TABLE [struct_it_mssql2mssql_1].dbo.index_syntax_variants (
    id INT NOT NULL,
    column_inline INT INDEX idx_index_syntax_column,
    table_inline INT NULL,
    composite_a INT NOT NULL,
    composite_b DATETIME2(6) NOT NULL,
    status TINYINT NOT NULL,
    payload NVARCHAR(100) NULL,
    external_unique INT NOT NULL,
    external_complex INT NOT NULL,
    INDEX idx_index_syntax_table (table_inline),
    INDEX idx_index_syntax_table_complex UNIQUE NONCLUSTERED
        (composite_a ASC, composite_b DESC)
        INCLUDE (payload)
        WHERE status > 0
);
CREATE INDEX idx_index_syntax_create_minimal
    ON [struct_it_mssql2mssql_1].dbo.index_syntax_variants (status);
CREATE UNIQUE INDEX idx_index_syntax_create_unique
    ON [struct_it_mssql2mssql_1].dbo.index_syntax_variants (external_unique);
CREATE UNIQUE NONCLUSTERED INDEX idx_index_syntax_create_complex
    ON [struct_it_mssql2mssql_1].dbo.index_syntax_variants
       (external_complex ASC, composite_b DESC)
    INCLUDE (payload)
    WHERE external_complex > 0;
CREATE CLUSTERED INDEX idx_index_syntax_create_clustered
    ON [struct_it_mssql2mssql_1].dbo.index_syntax_variants (id);
GO

-- Quoted Unicode defaults and table/column comments.
CREATE TABLE [struct_it_mssql2mssql_1].dbo.special_default_and_comment (
    id INT IDENTITY(1, 1) NOT NULL,
    f_1 NVARCHAR(255) NOT NULL
        CONSTRAINT df_special_default_f1 DEFAULT (N'abc''中文'''),
    CONSTRAINT pk_special_default_and_comment PRIMARY KEY CLUSTERED (id)
);
GO

-- Preserve case-sensitive column spelling even under a case-insensitive database collation.
CREATE TABLE [struct_it_mssql2mssql_1].dbo.case_sensitive_column_name (
    id INT IDENTITY(1, 1) NOT NULL,
    name VARCHAR(255) NOT NULL CONSTRAINT df_case_name DEFAULT ('jack'),
    Age INT NOT NULL CONSTRAINT df_case_age DEFAULT ((100)),
    GRADE INT NOT NULL CONSTRAINT df_case_grade DEFAULT ((100)),
    CONSTRAINT pk_case_sensitive_column_name PRIMARY KEY CLUSTERED (id)
);
GO

-- Bracket escaping and dots in table, column, and constraint identifiers.
CREATE TABLE [struct_it_mssql2mssql_1].dbo.[special_character_$1#@*]].table] (
    id INT IDENTITY(1, 1) NOT NULL,
    [column ]] with.dot] VARCHAR(255) NOT NULL,
    [unique_$#@]]] VARCHAR(255) NULL,
    [check ]] column] VARCHAR(255) NULL,
    CONSTRAINT [pk ]] special.constraint] PRIMARY KEY CLUSTERED (id),
    CONSTRAINT [uq_$#@ ]] special.constraint] UNIQUE NONCLUSTERED ([unique_$#@]]]),
    CONSTRAINT [ck ]] special.constraint] CHECK (LEN([check ]] column]) > 3)
);
GO

-- SQL Server keywords and indexes on quoted identifiers.
CREATE TABLE [struct_it_mssql2mssql_1].dbo.[match] (
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
    ON [struct_it_mssql2mssql_1].dbo.[match] ([offset] ASC);
CREATE NONCLUSTERED INDEX idx_key_col
    ON [struct_it_mssql2mssql_1].dbo.[match] ([match] ASC);
CREATE UNIQUE NONCLUSTERED INDEX uniq_unique_col
    ON [struct_it_mssql2mssql_1].dbo.[match] (unique_col ASC);
GO

-- Views are intentionally outside the MSSQL struct task's table scope.
-- CREATE VIEW [struct_it_mssql2mssql_1].dbo.full_column_type_view
-- AS SELECT * FROM [struct_it_mssql2mssql_1].dbo.full_column_type;
-- GO

EXEC [struct_it_mssql2mssql_1].sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Comment on full_column_type.',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE', @level1name = N'full_column_type';
EXEC [struct_it_mssql2mssql_1].sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Comment on full_column_type.id.',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE', @level1name = N'full_column_type',
    @level2type = N'COLUMN', @level2name = N'id';
EXEC [struct_it_mssql2mssql_1].sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Comment on full_index_type.',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE', @level1name = N'full_index_type';
EXEC [struct_it_mssql2mssql_1].sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Comment on full_index_type.id.',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE', @level1name = N'full_index_type',
    @level2type = N'COLUMN', @level2name = N'id';
EXEC [struct_it_mssql2mssql_1].sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'中文注释''special_default_and_comment''',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE', @level1name = N'special_default_and_comment';
EXEC [struct_it_mssql2mssql_1].sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'中文注释''f_1'' #?&^%$#@<>!',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE', @level1name = N'special_default_and_comment',
    @level2type = N'COLUMN', @level2name = N'f_1';
GO

-- Columnstore indexes must not disappear from a successful struct migration.
CREATE TABLE [struct_it_mssql2mssql_1].dbo.columnstore_index (
    id BIGINT NOT NULL,
    category INT NULL,
    amount DECIMAL(18, 2) NULL
);
CREATE CLUSTERED COLUMNSTORE INDEX cci_columnstore_index
    ON [struct_it_mssql2mssql_1].dbo.columnstore_index;
GO

CREATE TABLE [struct_it_mssql2mssql_1].dbo.nonclustered_columnstore_index (
    id BIGINT NOT NULL,
    category INT NULL,
    amount DECIMAL(18, 2) NULL,
    CONSTRAINT pk_nonclustered_columnstore_index PRIMARY KEY CLUSTERED (id)
);
CREATE NONCLUSTERED COLUMNSTORE INDEX ncci_columnstore_index
    ON [struct_it_mssql2mssql_1].dbo.nonclustered_columnstore_index (category, amount);
GO

-- A heap is still covered as an ordinary table. Its sys.indexes.type = 0 row is
-- intentionally not treated as an index to migrate.
CREATE TABLE [struct_it_mssql2mssql_1].dbo.heap_table (
    id BIGINT NOT NULL,
    payload NVARCHAR(100) NULL
);
GO

-- The remaining index/table categories are documented as executable examples but are
-- disabled until the struct model can recreate their prerequisites and type-specific options.

-- Type 7, NONCLUSTERED HASH. It requires a MEMORY_OPTIMIZED_DATA filegroup whose
-- physical file path cannot be inferred safely by a generic CREATE DATABASE migration.
-- CREATE TABLE [struct_it_mssql2mssql_1].dbo.memory_optimized_table (
--     id BIGINT NOT NULL,
--     payload NVARCHAR(100) NULL,
--     INDEX ix_memory_optimized_hash HASH (id) WITH (BUCKET_COUNT = 1024)
-- ) WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA);
-- GO

-- Type 9, JSON, is available in SQL Server 2025 rather than the SQL Server 2022 CI image.
-- CREATE TABLE [struct_it_mssql2mssql_1].dbo.json_index_table (
--     id BIGINT NOT NULL PRIMARY KEY,
--     document JSON NULL
-- );
-- CREATE JSON INDEX ix_json_document
--     ON [struct_it_mssql2mssql_1].dbo.json_index_table (document);
-- GO

-- Selective XML indexes and explicit spatial tessellation carry options not yet modeled.
-- CREATE SELECTIVE XML INDEX sxml_full_column_type
--     ON [struct_it_mssql2mssql_1].dbo.full_column_type (xml_col)
--     FOR (path_id = '/root/item' AS XQUERY 'xs:int' SINGLETON);
-- CREATE SPATIAL INDEX spatial_geometry_col
--     ON [struct_it_mssql2mssql_1].dbo.spatial_column_type (geometry_col)
--     USING GEOMETRY_AUTO_GRID
--     WITH (BOUNDING_BOX = (-180, -90, 180, 90));
-- GO

-- Specialized table categories are intentionally disabled until their system-catalog
-- metadata and complete CREATE TABLE options are represented by the struct model.
-- CREATE TABLE [struct_it_mssql2mssql_1].dbo.temporal_table (
--     id BIGINT NOT NULL PRIMARY KEY,
--     valid_from DATETIME2 GENERATED ALWAYS AS ROW START NOT NULL,
--     valid_to DATETIME2 GENERATED ALWAYS AS ROW END NOT NULL,
--     PERIOD FOR SYSTEM_TIME (valid_from, valid_to)
-- ) WITH (SYSTEM_VERSIONING = ON);
-- CREATE TABLE [struct_it_mssql2mssql_1].dbo.graph_node (id BIGINT) AS NODE;
-- CREATE TABLE [struct_it_mssql2mssql_1].dbo.graph_edge AS EDGE;
-- CREATE TABLE [struct_it_mssql2mssql_1].dbo.ledger_table (
--     id BIGINT NOT NULL PRIMARY KEY
-- ) WITH (LEDGER = ON);
-- CREATE TABLE [struct_it_mssql2mssql_1].dbo.file_table AS FILETABLE;
-- CREATE EXTERNAL TABLE [struct_it_mssql2mssql_1].dbo.external_table (
--     id BIGINT NOT NULL
-- ) WITH (LOCATION = '/external_table', DATA_SOURCE = external_data_source);
-- GO

-- CHECK constraints can be disabled or enabled without being trusted. This known
-- unsupported state case remains executable, but follows all index coverage.
CREATE TABLE [struct_it_mssql2mssql_1].dbo.check_constraint_state (
    id INT NOT NULL,
    disabled_value INT NULL,
    untrusted_value INT NULL,
    trusted_value INT NULL,
    CONSTRAINT pk_check_constraint_state PRIMARY KEY CLUSTERED (id),
    CONSTRAINT ck_check_constraint_disabled CHECK (disabled_value >= 0),
    CONSTRAINT ck_check_constraint_trusted CHECK (trusted_value >= 0)
);
ALTER TABLE [struct_it_mssql2mssql_1].dbo.check_constraint_state WITH NOCHECK
    ADD CONSTRAINT ck_check_constraint_untrusted CHECK (untrusted_value >= 0);
ALTER TABLE [struct_it_mssql2mssql_1].dbo.check_constraint_state
    NOCHECK CONSTRAINT ck_check_constraint_disabled;
ALTER TABLE [struct_it_mssql2mssql_1].dbo.check_constraint_state
    WITH NOCHECK CHECK CONSTRAINT ck_check_constraint_untrusted;
GO

-- These table paths flatten to the same dot-delimited checker key.
EXEC [struct_it_mssql2mssql_1].sys.sp_executesql N'CREATE SCHEMA [schema.with]';
EXEC [struct_it_mssql2mssql_1].sys.sp_executesql N'CREATE SCHEMA [schema]';
GO
CREATE TABLE [struct_it_mssql2mssql_1].[schema.with].[dot] (
    id INT NOT NULL
);
CREATE TABLE [struct_it_mssql2mssql_1].[schema].[with.dot] (
    id INT NOT NULL
);
GO
