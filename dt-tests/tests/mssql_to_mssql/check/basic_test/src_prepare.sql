DROP TABLE IF EXISTS [ape_dts].[dbo].[snapshot_check];
GO
CREATE TABLE [ape_dts].[dbo].[snapshot_check] (tenant INT NOT NULL, id INT NOT NULL, val INT NULL, PRIMARY KEY (tenant, id));

DROP TABLE IF EXISTS [ape_dts].[dbo].[sc_data_one_pk_multi_uk];
GO
CREATE TABLE [ape_dts].[dbo].[sc_data_one_pk_multi_uk] (
    id INT PRIMARY KEY,
    code INT NOT NULL UNIQUE,
    external_id BIGINT NOT NULL UNIQUE,
    payload NVARCHAR(40) NULL
);

DROP TABLE IF EXISTS [ape_dts].[dbo].[sc_data_no_pk_composite_uk];
GO
CREATE TABLE [ape_dts].[dbo].[sc_data_no_pk_composite_uk] (
    tenant INT NOT NULL,
    code INT NOT NULL,
    qty INT NOT NULL,
    UNIQUE (tenant, code)
);

DROP TABLE IF EXISTS [ape_dts].[dbo].[sc_data_no_pk_multi_uk];
GO
CREATE TABLE [ape_dts].[dbo].[sc_data_no_pk_multi_uk] (
    seq INT NOT NULL UNIQUE,
    external_code NVARCHAR(30) NOT NULL UNIQUE,
    amount INT NOT NULL
);

DROP TABLE IF EXISTS [ape_dts].[dbo].[sc_data_no_pk_no_uk];
GO
CREATE TABLE [ape_dts].[dbo].[sc_data_no_pk_no_uk] (
    category INT NOT NULL,
    label NVARCHAR(30) NOT NULL
);

DROP TABLE IF EXISTS [ape_dts].[dbo].[sc_data_quoted_keys];
GO
CREATE TABLE [ape_dts].[dbo].[sc_data_quoted_keys] (
    [order id] INT NOT NULL,
    [line:no] INT NOT NULL,
    [display name] NVARCHAR(30) NULL,
    PRIMARY KEY ([order id], [line:no])
);

DROP TABLE IF EXISTS [ape_dts].[dbo].[sc_data_typed_values];
GO
CREATE TABLE [ape_dts].[dbo].[sc_data_typed_values] (
    id INT PRIMARY KEY,
    amount DECIMAL(16,4),
    observed_at DATETIME2(3),
    event_date DATE,
    token UNIQUEIDENTIFIER,
    payload VARBINARY(16),
    active BIT
);

DROP TABLE IF EXISTS [ape_dts].[dbo].[sc_data_calculated];
GO
CREATE TABLE [ape_dts].[dbo].[sc_data_calculated] (
    id INT PRIMARY KEY,
    qty INT NULL,
    doubled AS (qty * 2) PERSISTED
);
