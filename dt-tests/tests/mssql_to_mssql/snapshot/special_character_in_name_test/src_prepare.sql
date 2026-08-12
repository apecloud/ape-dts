DROP TABLE IF EXISTS [special.* schema].[orders.* detail];
IF SCHEMA_ID(N'special.* schema') IS NULL EXEC(N'CREATE SCHEMA [special.* schema]');
CREATE TABLE [special.* schema].[orders.* detail] (
    [order.id] int NOT NULL PRIMARY KEY,
    [select] nvarchar(30) NOT NULL,
    [value with space] decimal(12, 3) NULL
);
GO
