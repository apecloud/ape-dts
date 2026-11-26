DROP TABLE IF EXISTS [ape_dts].[dbo].[struct_check];
GO
CREATE TABLE [ape_dts].[dbo].[struct_check] (tenant INT NOT NULL, id INT NOT NULL, val INT NULL, CONSTRAINT [pk_basic_struct_test] PRIMARY KEY (tenant, id));
CREATE INDEX [ix_id] ON [ape_dts].[dbo].[struct_check] ([id] ASC);
CREATE INDEX [ix_val] ON [ape_dts].[dbo].[struct_check] ([val] DESC);

DROP TABLE IF EXISTS [ape_dts].[dbo].[sc_struct_typed_columns];
GO
CREATE TABLE [ape_dts].[dbo].[sc_struct_typed_columns] (
    id BIGINT IDENTITY(10, 5) NOT NULL,
    title NVARCHAR(64) NOT NULL,
    amount DECIMAL(16,4) NULL,
    observed_at DATETIME2(3) NULL,
    token UNIQUEIDENTIFIER NULL,
    payload VARBINARY(32) NULL,
    version ROWVERSION
);

DROP TABLE IF EXISTS [ape_dts].[dbo].[sc_struct_missing_table];

DROP TABLE IF EXISTS [ape_dts].[dbo].[sc_struct_column_difference];
GO
CREATE TABLE [ape_dts].[dbo].[sc_struct_column_difference] (
    id INT NOT NULL,
    qty INT NOT NULL
);

DROP TABLE IF EXISTS [ape_dts].[dbo].[sc_struct_constraints];
GO
CREATE TABLE [ape_dts].[dbo].[sc_struct_constraints] (
    id INT NOT NULL,
    code INT NOT NULL,
    qty INT NOT NULL,
    CONSTRAINT [pk_sc_constraints] PRIMARY KEY (id),
    CONSTRAINT [uq_sc_constraints] UNIQUE (code),
    CONSTRAINT [ck_sc_qty] CHECK (qty >= 0)
);

DROP TABLE IF EXISTS [ape_dts].[dbo].[sc_struct_missing_index];
GO
CREATE TABLE [ape_dts].[dbo].[sc_struct_missing_index] (
    id INT NOT NULL,
    qty INT NOT NULL
);

DROP TABLE IF EXISTS [ape_dts].[dbo].[sc_struct_computed];
GO
CREATE TABLE [ape_dts].[dbo].[sc_struct_computed] (
    units INT NOT NULL,
    price INT NOT NULL,
    total AS (units * price) PERSISTED
);
