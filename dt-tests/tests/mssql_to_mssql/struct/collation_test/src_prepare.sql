IF OBJECT_ID(N'struct_collation_mssql2mssql_1.collation_table', N'U') IS NOT NULL
    DROP TABLE struct_collation_mssql2mssql_1.collation_table;
GO
IF SCHEMA_ID(N'struct_collation_mssql2mssql_1') IS NOT NULL
    EXEC(N'DROP SCHEMA struct_collation_mssql2mssql_1');
GO
EXEC(N'CREATE SCHEMA struct_collation_mssql2mssql_1');
GO
CREATE TABLE struct_collation_mssql2mssql_1.collation_table (
    id INT NOT NULL,
    case_insensitive VARCHAR(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    binary_text VARCHAR(100) COLLATE Latin1_General_100_BIN2 NULL,
    unicode_text NVARCHAR(100) COLLATE Latin1_General_100_CI_AS_SC NULL,
    CONSTRAINT pk_collation_table PRIMARY KEY CLUSTERED (id),
    CONSTRAINT uq_collation_table UNIQUE NONCLUSTERED (binary_text)
);
CREATE NONCLUSTERED INDEX idx_collation_unicode
    ON struct_collation_mssql2mssql_1.collation_table (unicode_text ASC);
GO
