IF DB_ID(N'struct_collation_mssql2mssql_1') IS NOT NULL
BEGIN
    ALTER DATABASE [struct_collation_mssql2mssql_1] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [struct_collation_mssql2mssql_1];
END;
GO
