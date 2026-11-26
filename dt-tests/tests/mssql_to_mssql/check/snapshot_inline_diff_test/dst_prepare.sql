DROP TABLE IF EXISTS [ape_dts].[dbo].[inline_computed];
GO
CREATE TABLE [ape_dts].[dbo].[inline_computed] (id INT PRIMARY KEY, val INT, calculated AS (val + 2));
