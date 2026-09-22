IF DB_ID(N'struct_route_mssql2mssql_1') IS NOT NULL
BEGIN
    ALTER DATABASE [struct_route_mssql2mssql_1] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [struct_route_mssql2mssql_1];
END;
CREATE DATABASE [struct_route_mssql2mssql_1];
GO
CREATE TABLE [struct_route_mssql2mssql_1].dbo.full_column_type (
    id INT IDENTITY(1, 1) NOT NULL,
    bit_col BIT NULL,
    bigint_col BIGINT NULL,
    decimal_col DECIMAL(18, 4) NULL,
    date_col DATE NULL,
    datetime2_col DATETIME2(6) NULL,
    varchar_col VARCHAR(255) NOT NULL,
    nvarchar_col NVARCHAR(255) NULL,
    varbinary_col VARBINARY(255) NULL,
    uuid_col UNIQUEIDENTIFIER NULL,
    xml_col XML NULL,
    CONSTRAINT pk_route_full_column PRIMARY KEY CLUSTERED (id)
);
GO
CREATE TABLE [struct_route_mssql2mssql_1].dbo.constraint_table (
    id BIGINT IDENTITY(10, 2) NOT NULL,
    code NVARCHAR(40) NOT NULL CONSTRAINT df_route_code DEFAULT (N'route'),
    amount DECIMAL(12, 2) NOT NULL,
    total AS (amount * 2) PERSISTED,
    CONSTRAINT pk_route_constraint PRIMARY KEY NONCLUSTERED (id),
    CONSTRAINT uq_route_constraint UNIQUE CLUSTERED (code),
    CONSTRAINT ck_route_constraint CHECK (amount >= 0)
);
CREATE TABLE [struct_route_mssql2mssql_1].dbo.full_index_type (
    id INT NOT NULL,
    category VARCHAR(32) NOT NULL,
    created_at DATETIME2(6) NOT NULL,
    status TINYINT NOT NULL,
    payload NVARCHAR(100) NULL,
    CONSTRAINT pk_route_index PRIMARY KEY CLUSTERED (id)
);
CREATE NONCLUSTERED INDEX idx_route_composite
    ON [struct_route_mssql2mssql_1].dbo.full_index_type (category ASC, created_at DESC)
    INCLUDE (payload)
    WHERE status > 0;
GO
EXEC [struct_route_mssql2mssql_1].sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'routed table comment',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE', @level1name = N'constraint_table';
GO
