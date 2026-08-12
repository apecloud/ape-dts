DROP TABLE IF EXISTS wild_a.keep_orders;
DROP TABLE IF EXISTS wild_a.keep_private;
DROP TABLE IF EXISTS wild_a.skip_orders;
DROP TABLE IF EXISTS wild_b.keep_orders;
DROP TABLE IF EXISTS wild_skip.keep_orders;
IF SCHEMA_ID(N'wild_a') IS NULL EXEC(N'CREATE SCHEMA wild_a');
IF SCHEMA_ID(N'wild_b') IS NULL EXEC(N'CREATE SCHEMA wild_b');
IF SCHEMA_ID(N'wild_skip') IS NULL EXEC(N'CREATE SCHEMA wild_skip');
CREATE TABLE wild_a.keep_orders (id int NOT NULL PRIMARY KEY, value nvarchar(30));
CREATE TABLE wild_a.keep_private (id int NOT NULL PRIMARY KEY, value nvarchar(30));
CREATE TABLE wild_a.skip_orders (id int NOT NULL PRIMARY KEY, value nvarchar(30));
CREATE TABLE wild_b.keep_orders (id int NOT NULL PRIMARY KEY, value nvarchar(30));
CREATE TABLE wild_skip.keep_orders (id int NOT NULL PRIMARY KEY, value nvarchar(30));
GO
