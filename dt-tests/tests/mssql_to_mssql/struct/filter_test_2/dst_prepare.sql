IF OBJECT_ID(N'struct_filter_mssql2mssql_2.full_index_type', N'U') IS NOT NULL
    DROP TABLE struct_filter_mssql2mssql_2.full_index_type;
IF OBJECT_ID(N'struct_filter_mssql2mssql_2.constraint_table', N'U') IS NOT NULL
    DROP TABLE struct_filter_mssql2mssql_2.constraint_table;
GO
IF SCHEMA_ID(N'struct_filter_mssql2mssql_2') IS NOT NULL
    EXEC(N'DROP SCHEMA struct_filter_mssql2mssql_2');
GO
EXEC(N'CREATE SCHEMA struct_filter_mssql2mssql_2');
GO
CREATE TABLE struct_filter_mssql2mssql_2.full_index_type (
    id INT NOT NULL,
    unique_col VARCHAR(64) NOT NULL,
    index_col INT NULL,
    check_col INT NULL
);
CREATE TABLE struct_filter_mssql2mssql_2.constraint_table (
    id INT NOT NULL,
    code NVARCHAR(40) NOT NULL,
    amount DECIMAL(12, 2) NULL,
    status VARCHAR(16) NOT NULL
);
GO
