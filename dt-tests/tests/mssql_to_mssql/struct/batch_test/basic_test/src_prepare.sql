DECLARE @i INT = 1;
DECLARE @schema SYSNAME;
DECLARE @qualified NVARCHAR(517);
DECLARE @sql NVARCHAR(MAX);
WHILE @i <= 5
BEGIN
    SET @schema = N'struct_batch_mssql2mssql_' + CONVERT(NVARCHAR(10), @i);
    SET @qualified = QUOTENAME(@schema) + N'.expression_defaults';
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
GO
