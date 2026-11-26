DROP TABLE IF EXISTS [ape_dts].[dbo].[snapshot_check];
GO
CREATE TABLE [ape_dts].[dbo].[snapshot_check] (tenant INT NOT NULL, id INT NOT NULL, val INT NULL, PRIMARY KEY (tenant, id));
