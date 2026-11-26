DROP TABLE IF EXISTS [ape_dts].[dbo].[snapshot_inline];
GO
CREATE TABLE [ape_dts].[dbo].[snapshot_inline] (tenant INT NOT NULL, id INT NOT NULL, val INT NULL, version ROWVERSION, PRIMARY KEY (tenant, id));
INSERT INTO [ape_dts].[dbo].[snapshot_inline] (tenant, id, val) VALUES (0, 0, 0);
DELETE FROM [ape_dts].[dbo].[snapshot_inline] WHERE tenant = 0;
