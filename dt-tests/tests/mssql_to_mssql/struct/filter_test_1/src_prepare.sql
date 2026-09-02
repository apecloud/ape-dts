IF DB_ID(N'struct_filter_mssql2mssql_1') IS NOT NULL
BEGIN
    ALTER DATABASE [struct_filter_mssql2mssql_1] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [struct_filter_mssql2mssql_1];
END;
CREATE DATABASE [struct_filter_mssql2mssql_1];
GO
EXEC [struct_filter_mssql2mssql_1].sys.sp_executesql N'CREATE SCHEMA filtered_schema';
CREATE TABLE [struct_filter_mssql2mssql_1].filtered_schema.filtered_table (
    id INT NOT NULL
);
GO
CREATE TABLE [struct_filter_mssql2mssql_1].dbo.full_index_type (
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
    CONSTRAINT pk_filter_1 PRIMARY KEY CLUSTERED (id),
    CONSTRAINT uq_filter_1 UNIQUE NONCLUSTERED (unique_col),
    CONSTRAINT ck_filter_1 CHECK (check_col >= 0)
);
CREATE NONCLUSTERED INDEX index_index
    ON [struct_filter_mssql2mssql_1].dbo.full_index_type (index_col ASC);
CREATE NONCLUSTERED INDEX simple_index
    ON [struct_filter_mssql2mssql_1].dbo.full_index_type (simple_index_col ASC);
CREATE NONCLUSTERED INDEX composite_index
    ON [struct_filter_mssql2mssql_1].dbo.full_index_type
       (composite_index_col1 ASC, composite_index_col2 DESC, composite_index_col3 ASC);
GO
CREATE TABLE [struct_filter_mssql2mssql_1].dbo.constraint_table (
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
