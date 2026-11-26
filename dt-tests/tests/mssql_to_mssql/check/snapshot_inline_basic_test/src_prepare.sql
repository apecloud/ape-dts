DROP TABLE IF EXISTS [ape_dts].[dbo].[snapshot_inline];
GO
CREATE TABLE [ape_dts].[dbo].[snapshot_inline] (tenant INT NOT NULL, id INT NOT NULL, val INT NULL, version ROWVERSION, PRIMARY KEY (tenant, id));
