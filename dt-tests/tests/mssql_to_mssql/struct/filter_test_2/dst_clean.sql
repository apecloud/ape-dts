IF DB_ID(N'struct_filter_mssql2mssql_2') IS NOT NULL
BEGIN
    ALTER DATABASE [struct_filter_mssql2mssql_2] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [struct_filter_mssql2mssql_2];
END;
GO
