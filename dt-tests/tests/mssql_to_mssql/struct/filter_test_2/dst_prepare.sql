IF DB_ID(N'struct_filter_mssql2mssql_2') IS NOT NULL
BEGIN
    ALTER DATABASE [struct_filter_mssql2mssql_2] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [struct_filter_mssql2mssql_2];
END;
CREATE DATABASE [struct_filter_mssql2mssql_2];
GO
CREATE TABLE [struct_filter_mssql2mssql_2].dbo.full_index_type (
    id INT NOT NULL,
    unique_col VARCHAR(64) NOT NULL,
    index_col VARCHAR(255) NULL,
    fulltext_col NVARCHAR(MAX) NULL,
    spatial_col GEOMETRY NULL,
    simple_index_col VARCHAR(255) NULL,
    composite_index_col1 VARCHAR(255) NULL,
    composite_index_col2 VARCHAR(255) NULL,
    composite_index_col3 VARCHAR(255) NULL,
    check_col INT NULL,
    CONSTRAINT pk_filter_2 PRIMARY KEY CLUSTERED (id),
    CONSTRAINT uq_filter_2 UNIQUE NONCLUSTERED (unique_col)
);
CREATE TABLE [struct_filter_mssql2mssql_2].dbo.constraint_table (
    id INT NOT NULL,
    code NVARCHAR(40) NOT NULL,
    amount DECIMAL(12, 2) NULL,
    status VARCHAR(16) NOT NULL,
    CONSTRAINT pk_filter_2_constraint PRIMARY KEY CLUSTERED (id),
    CONSTRAINT uq_filter_2_constraint UNIQUE NONCLUSTERED (code)
);
GO
