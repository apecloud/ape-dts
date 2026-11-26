DROP TABLE IF EXISTS [ape_dts].[dbo].[consistent_struct_check];
GO
CREATE TABLE [ape_dts].[dbo].[consistent_struct_check] (tenant INT NOT NULL, id INT NOT NULL, val INT NULL, CONSTRAINT [pk_consistent_struct_test] PRIMARY KEY (tenant, id));
CREATE INDEX [ix_id] ON [ape_dts].[dbo].[consistent_struct_check] ([id] ASC);
CREATE INDEX [ix_val] ON [ape_dts].[dbo].[consistent_struct_check] ([val] ASC);
