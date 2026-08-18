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
    SET @sql = N'CREATE SCHEMA ' + QUOTENAME(@schema);
    EXEC sys.sp_executesql @sql;
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
GO
