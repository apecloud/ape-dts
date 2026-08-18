IF OBJECT_ID(N'struct_collation_mssql2mssql_1.collation_table', N'U') IS NOT NULL
    DROP TABLE struct_collation_mssql2mssql_1.collation_table;
GO
IF SCHEMA_ID(N'struct_collation_mssql2mssql_1') IS NOT NULL
    EXEC(N'DROP SCHEMA struct_collation_mssql2mssql_1');
GO
