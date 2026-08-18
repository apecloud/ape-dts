IF SCHEMA_ID(N'struct_filter_mssql2mssql_1') IS NULL
    THROW 51000, 'filtered schema was not created', 1;

IF OBJECT_ID(N'struct_filter_mssql2mssql_1.full_index_type', N'U') IS NULL
    THROW 51000, 'full_index_type was not created', 1;

IF OBJECT_ID(N'struct_filter_mssql2mssql_1.constraint_table', N'U') IS NULL
    THROW 51000, 'constraint_table was not created', 1;

GO

IF (
    SELECT COUNT_BIG(*)
    FROM sys.key_constraints
    WHERE parent_object_id IN (
        OBJECT_ID(N'struct_filter_mssql2mssql_1.full_index_type'),
        OBJECT_ID(N'struct_filter_mssql2mssql_1.constraint_table')
    )
) <> 4
    THROW 51000, 'primary or unique constraint was not created with its table', 1;
GO

IF EXISTS (
    SELECT 1
    FROM sys.check_constraints
    WHERE parent_object_id IN (
        OBJECT_ID(N'struct_filter_mssql2mssql_1.full_index_type'),
        OBJECT_ID(N'struct_filter_mssql2mssql_1.constraint_table')
    )
)
    THROW 51000, 'check constraints were not filtered', 1;

IF EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'struct_filter_mssql2mssql_1.full_index_type')
      AND is_primary_key = 0
      AND is_unique_constraint = 0
)
    THROW 51000, 'ordinary index was not filtered', 1;
GO
