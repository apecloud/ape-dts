```
DECLARE @i INT = 1;
DECLARE @database SYSNAME;
DECLARE @qualified NVARCHAR(776);
DECLARE @sql NVARCHAR(MAX);
WHILE @i <= 100
BEGIN
    SET @database = N'struct_bench_mssql2mssql_' + CONVERT(NVARCHAR(10), @i);
    SET @sql = N'IF DB_ID(N''' + @database + N''') IS NOT NULL BEGIN '
        + N'ALTER DATABASE ' + QUOTENAME(@database) + N' SET SINGLE_USER WITH ROLLBACK IMMEDIATE; '
        + N'DROP DATABASE ' + QUOTENAME(@database) + N'; END; '
        + N'CREATE DATABASE ' + QUOTENAME(@database) + N';';
    EXEC sys.sp_executesql @sql;

    SET @qualified = QUOTENAME(@database) + N'.dbo.batch_table';
    SET @sql = N'CREATE TABLE ' + @qualified + N' (
        id BIGINT IDENTITY(100, 5) NOT NULL,
        code NVARCHAR(40) NOT NULL CONSTRAINT df_bench_batch_code DEFAULT (N''bench''),
        quantity INT NOT NULL CONSTRAINT df_bench_batch_quantity DEFAULT ((1)),
        total AS (quantity * 2) PERSISTED,
        CONSTRAINT pk_bench_batch PRIMARY KEY NONCLUSTERED (id),
        CONSTRAINT ck_bench_batch CHECK (quantity >= 0)
    );
    CREATE NONCLUSTERED INDEX idx_bench_batch_code ON '
        + @qualified + N' (code ASC);';
    EXEC sys.sp_executesql @sql;
    SET @i += 1;
END;
```
GO
