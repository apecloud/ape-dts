IF DB_ID(N'struct_collation_mssql2mssql_1') IS NOT NULL
BEGIN
    ALTER DATABASE [struct_collation_mssql2mssql_1] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [struct_collation_mssql2mssql_1];
END;
CREATE DATABASE [struct_collation_mssql2mssql_1];
GO
CREATE TABLE [struct_collation_mssql2mssql_1].dbo.collation_table (
    id INT NOT NULL,
    case_insensitive VARCHAR(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    binary_text VARCHAR(100) COLLATE Latin1_General_100_BIN2 NULL,
    unicode_text NVARCHAR(100) COLLATE Latin1_General_100_CI_AS_SC NULL,
    CONSTRAINT pk_collation_table PRIMARY KEY CLUSTERED (id),
    CONSTRAINT uq_collation_table UNIQUE NONCLUSTERED (binary_text)
);
CREATE NONCLUSTERED INDEX idx_collation_unicode
    ON [struct_collation_mssql2mssql_1].dbo.collation_table (unicode_text ASC);
GO
