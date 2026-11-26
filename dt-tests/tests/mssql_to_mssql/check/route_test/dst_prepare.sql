DROP TABLE IF EXISTS [ape_dts].[dbo].[check_route_dst];
GO
CREATE TABLE [ape_dts].[dbo].[check_route_dst] (target_id INT PRIMARY KEY, target_val INT);
INSERT INTO [ape_dts].[dbo].[check_route_dst] (target_id, target_val) VALUES (1, 10), (2, 99);
