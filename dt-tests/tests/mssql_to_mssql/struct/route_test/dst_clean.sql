IF DB_ID(N'dst_struct_route_mssql2mssql_1') IS NOT NULL
BEGIN
    ALTER DATABASE [dst_struct_route_mssql2mssql_1] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [dst_struct_route_mssql2mssql_1];
END;
GO
