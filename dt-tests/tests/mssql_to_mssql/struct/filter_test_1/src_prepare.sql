IF OBJECT_ID(N'struct_filter_mssql2mssql_1.full_index_type', N'U') IS NOT NULL
    DROP TABLE struct_filter_mssql2mssql_1.full_index_type;
IF OBJECT_ID(N'struct_filter_mssql2mssql_1.constraint_table', N'U') IS NOT NULL
    DROP TABLE struct_filter_mssql2mssql_1.constraint_table;
GO
IF SCHEMA_ID(N'struct_filter_mssql2mssql_1') IS NOT NULL
    EXEC(N'DROP SCHEMA struct_filter_mssql2mssql_1');
GO
EXEC(N'CREATE SCHEMA struct_filter_mssql2mssql_1');
GO
CREATE TABLE struct_filter_mssql2mssql_1.full_index_type (
    id INT NOT NULL,
    unique_col VARCHAR(64) NOT NULL,
    index_col INT NULL,
    check_col INT NULL,
    CONSTRAINT pk_filter_1 PRIMARY KEY CLUSTERED (id),
    CONSTRAINT uq_filter_1 UNIQUE NONCLUSTERED (unique_col),
    CONSTRAINT ck_filter_1 CHECK (check_col >= 0)
);
CREATE NONCLUSTERED INDEX idx_filter_1
    ON struct_filter_mssql2mssql_1.full_index_type (index_col DESC);
GO
CREATE TABLE struct_filter_mssql2mssql_1.constraint_table (
    id INT NOT NULL,
    code NVARCHAR(40) NOT NULL,
    amount DECIMAL(12, 2) NULL,
    status VARCHAR(16) NOT NULL,
    CONSTRAINT pk_filter_1_constraint PRIMARY KEY CLUSTERED (id),
    CONSTRAINT uq_filter_1_constraint UNIQUE NONCLUSTERED (code),
    CONSTRAINT ck_filter_1_amount CHECK (amount >= 0),
    CONSTRAINT ck_filter_1_status CHECK (status IN ('active', 'disabled'))
);
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'not migrated by table-only filter',
    @level0type = N'SCHEMA', @level0name = N'struct_filter_mssql2mssql_1',
    @level1type = N'TABLE', @level1name = N'full_index_type';
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'second filtered table',
    @level0type = N'SCHEMA', @level0name = N'struct_filter_mssql2mssql_1',
    @level1type = N'TABLE', @level1name = N'constraint_table';
GO
