IF OBJECT_ID(N'dst_struct_route_mssql2mssql_1.dst_constraint_table', N'U') IS NOT NULL
    DROP TABLE dst_struct_route_mssql2mssql_1.dst_constraint_table;
IF OBJECT_ID(N'dst_struct_route_mssql2mssql_1.dst_full_index_type', N'U') IS NOT NULL
    DROP TABLE dst_struct_route_mssql2mssql_1.dst_full_index_type;
IF OBJECT_ID(N'dst_struct_route_mssql2mssql_1.full_column_type', N'U') IS NOT NULL
    DROP TABLE dst_struct_route_mssql2mssql_1.full_column_type;
GO
IF SCHEMA_ID(N'dst_struct_route_mssql2mssql_1') IS NOT NULL
    EXEC(N'DROP SCHEMA dst_struct_route_mssql2mssql_1');
GO
