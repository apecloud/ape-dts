IF OBJECT_ID(N'struct_filter_mssql2mssql_1.full_index_type', N'U') IS NOT NULL
    DROP TABLE struct_filter_mssql2mssql_1.full_index_type;
IF OBJECT_ID(N'struct_filter_mssql2mssql_1.constraint_table', N'U') IS NOT NULL
    DROP TABLE struct_filter_mssql2mssql_1.constraint_table;
GO
IF SCHEMA_ID(N'struct_filter_mssql2mssql_1') IS NOT NULL
    EXEC(N'DROP SCHEMA struct_filter_mssql2mssql_1');
GO
