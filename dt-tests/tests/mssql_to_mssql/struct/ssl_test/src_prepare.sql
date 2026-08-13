IF OBJECT_ID(N'struct_ssl_mssql2mssql.encrypted_table', N'U') IS NOT NULL
    DROP TABLE struct_ssl_mssql2mssql.encrypted_table;
GO
IF SCHEMA_ID(N'struct_ssl_mssql2mssql') IS NOT NULL
    EXEC(N'DROP SCHEMA struct_ssl_mssql2mssql');
GO
EXEC(N'CREATE SCHEMA struct_ssl_mssql2mssql');
GO
CREATE TABLE struct_ssl_mssql2mssql.encrypted_table (
    id INT IDENTITY(1, 1) NOT NULL,
    secret NVARCHAR(100) NOT NULL CONSTRAINT df_encrypted_secret DEFAULT (N'secret'),
    CONSTRAINT pk_encrypted_table PRIMARY KEY CLUSTERED (id),
    CONSTRAINT ck_encrypted_secret CHECK (LEN(secret) > 0)
);
CREATE NONCLUSTERED INDEX idx_encrypted_secret
    ON struct_ssl_mssql2mssql.encrypted_table (secret ASC);
GO
