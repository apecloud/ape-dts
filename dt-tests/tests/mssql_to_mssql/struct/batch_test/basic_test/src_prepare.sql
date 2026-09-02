```
DECLARE @i INT = 1;
DECLARE @database SYSNAME;
DECLARE @qualified NVARCHAR(776);
DECLARE @sql NVARCHAR(MAX);
WHILE @i <= 5
BEGIN
    SET @database = N'struct_batch_mssql2mssql_' + CONVERT(NVARCHAR(10), @i);
    SET @sql = N'IF DB_ID(N''' + @database + N''') IS NOT NULL BEGIN '
        + N'ALTER DATABASE ' + QUOTENAME(@database) + N' SET SINGLE_USER WITH ROLLBACK IMMEDIATE; '
        + N'DROP DATABASE ' + QUOTENAME(@database) + N'; END; '
        + N'CREATE DATABASE ' + QUOTENAME(@database) + N';';
    EXEC sys.sp_executesql @sql;

    SET @qualified = QUOTENAME(@database) + N'.dbo.expression_defaults';
    SET @sql = N'CREATE TABLE ' + @qualified + N' (
        id INT IDENTITY(1, 1) NOT NULL,
        code NVARCHAR(40) NOT NULL CONSTRAINT df_expression_defaults_code DEFAULT (N''batch''),
        quantity INT NOT NULL CONSTRAINT df_expression_defaults_quantity DEFAULT ((1)),
        created_at DATETIME2(6) NOT NULL CONSTRAINT df_expression_defaults_created DEFAULT (SYSUTCDATETIME()),
        request_id UNIQUEIDENTIFIER NOT NULL CONSTRAINT df_expression_defaults_request DEFAULT (NEWID()),
        doubled AS (quantity * 2) PERSISTED,
        CONSTRAINT pk_expression_defaults PRIMARY KEY CLUSTERED (id),
        CONSTRAINT ck_expression_defaults CHECK (quantity >= 0)
    );
    CREATE NONCLUSTERED INDEX idx_expression_defaults_code ON '
        + @qualified + N' (code ASC);';
    EXEC sys.sp_executesql @sql;
    SET @i += 1;
END;
```
GO
