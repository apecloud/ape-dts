DECLARE @i INT = 1;
DECLARE @database SYSNAME;
DECLARE @sql NVARCHAR(MAX);
WHILE @i <= 5
BEGIN
    SET @database = N'struct_batch_mssql2mssql_' + CONVERT(NVARCHAR(10), @i);
    SET @sql = N'IF DB_ID(N''' + @database + N''') IS NOT NULL BEGIN '
        + N'ALTER DATABASE ' + QUOTENAME(@database) + N' SET SINGLE_USER WITH ROLLBACK IMMEDIATE; '
        + N'DROP DATABASE ' + QUOTENAME(@database) + N'; END; '
        + N'CREATE DATABASE ' + QUOTENAME(@database) + N';';
    EXEC sys.sp_executesql @sql;
    SET @i += 1;
END;
GO
