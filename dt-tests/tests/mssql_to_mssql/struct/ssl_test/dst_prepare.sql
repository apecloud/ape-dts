IF DB_ID(N'struct_ssl_mssql2mssql') IS NOT NULL
BEGIN
    ALTER DATABASE [struct_ssl_mssql2mssql] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [struct_ssl_mssql2mssql];
END;
CREATE DATABASE [struct_ssl_mssql2mssql];
GO
