DECLARE @i INT = 1;
DECLARE @schema SYSNAME;
DECLARE @qualified NVARCHAR(517);
DECLARE @sql NVARCHAR(MAX);
WHILE @i <= 100
BEGIN
    SET @schema = N'struct_bench_mssql2mssql_' + CONVERT(NVARCHAR(10), @i);
    SET @qualified = QUOTENAME(@schema) + N'.batch_table';
    IF OBJECT_ID(@qualified, N'U') IS NOT NULL
    BEGIN
        SET @sql = N'DROP TABLE ' + @qualified;
        EXEC sys.sp_executesql @sql;
    END;
    IF SCHEMA_ID(@schema) IS NOT NULL
    BEGIN
        SET @sql = N'DROP SCHEMA ' + QUOTENAME(@schema);
        EXEC sys.sp_executesql @sql;
    END;
    SET @i += 1;
END;
GO
