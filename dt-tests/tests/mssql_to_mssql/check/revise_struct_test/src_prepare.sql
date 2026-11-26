DROP TABLE IF EXISTS [ape_dts].[dbo].[check_struct_revise];
GO
CREATE TABLE [ape_dts].[dbo].[check_struct_revise] (id INT PRIMARY KEY, val INT NULL);
CREATE INDEX [ix_val] ON [ape_dts].[dbo].[check_struct_revise] (val);
