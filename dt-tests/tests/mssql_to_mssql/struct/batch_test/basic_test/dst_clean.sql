```
DECLARE @i INT = 1;
DECLARE @database SYSNAME;
DECLARE @sql NVARCHAR(MAX);
WHILE @i <= 5
BEGIN
    SET @database = N'struct_batch_mssql2mssql_' + CONVERT(NVARCHAR(10), @i);
    IF DB_ID(@database) IS NOT NULL
    BEGIN
        SET @sql = N'ALTER DATABASE ' + QUOTENAME(@database)
            + N' SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE '
            + QUOTENAME(@database) + N';';
        EXEC sys.sp_executesql @sql;
    END;
    SET @i += 1;
END;
```
GO
