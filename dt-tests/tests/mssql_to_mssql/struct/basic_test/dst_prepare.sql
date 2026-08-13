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
