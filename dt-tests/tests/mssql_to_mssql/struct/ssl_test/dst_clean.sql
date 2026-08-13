IF OBJECT_ID(N'struct_ssl_mssql2mssql.encrypted_table', N'U') IS NOT NULL
    DROP TABLE struct_ssl_mssql2mssql.encrypted_table;
GO
IF SCHEMA_ID(N'struct_ssl_mssql2mssql') IS NOT NULL
    EXEC(N'DROP SCHEMA struct_ssl_mssql2mssql');
GO
