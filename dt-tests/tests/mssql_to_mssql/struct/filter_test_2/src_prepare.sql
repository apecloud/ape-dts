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
    index_col VARCHAR(255) NULL,
    fulltext_col NVARCHAR(MAX) NULL,
    spatial_col GEOMETRY NULL,
    simple_index_col VARCHAR(255) NULL,
    composite_index_col1 VARCHAR(255) NULL,
    composite_index_col2 VARCHAR(255) NULL,
    composite_index_col3 VARCHAR(255) NULL,
    check_col INT NULL,
    CONSTRAINT pk_filter_2 PRIMARY KEY CLUSTERED (id),
    CONSTRAINT uq_filter_2 UNIQUE NONCLUSTERED (unique_col),
    CONSTRAINT ck_filter_2 CHECK (check_col >= 0)
);
CREATE NONCLUSTERED INDEX index_index
    ON struct_filter_mssql2mssql_2.full_index_type (index_col ASC);
CREATE NONCLUSTERED INDEX simple_index
    ON struct_filter_mssql2mssql_2.full_index_type (simple_index_col ASC);
CREATE NONCLUSTERED INDEX composite_index
    ON struct_filter_mssql2mssql_2.full_index_type
       (composite_index_col1 ASC, composite_index_col2 DESC, composite_index_col3 ASC);
GO
CREATE TABLE struct_filter_mssql2mssql_2.constraint_table (
    id INT NOT NULL,
    code NVARCHAR(40) NOT NULL,
    amount DECIMAL(12, 2) NULL,
    status VARCHAR(16) NOT NULL,
    CONSTRAINT pk_filter_2_constraint PRIMARY KEY CLUSTERED (id),
    CONSTRAINT uq_filter_2_constraint UNIQUE NONCLUSTERED (code),
    CONSTRAINT ck_filter_2_amount CHECK (amount >= 0),
    CONSTRAINT ck_filter_2_status CHECK (status IN ('active', 'disabled'))
);
GO
