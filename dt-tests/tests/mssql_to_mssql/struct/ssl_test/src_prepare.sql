IF DB_ID(N'struct_ssl_mssql2mssql') IS NOT NULL
BEGIN
    ALTER DATABASE [struct_ssl_mssql2mssql] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [struct_ssl_mssql2mssql];
END;
CREATE DATABASE [struct_ssl_mssql2mssql];
GO
CREATE TABLE [struct_ssl_mssql2mssql].dbo.encrypted_table (
    id INT IDENTITY(1, 1) NOT NULL,
    secret NVARCHAR(100) NOT NULL CONSTRAINT df_encrypted_secret DEFAULT (N'secret'),
    CONSTRAINT pk_encrypted_table PRIMARY KEY CLUSTERED (id),
    CONSTRAINT ck_encrypted_secret CHECK (LEN(secret) > 0)
);
CREATE NONCLUSTERED INDEX idx_encrypted_secret
    ON [struct_ssl_mssql2mssql].dbo.encrypted_table (secret ASC);
GO
