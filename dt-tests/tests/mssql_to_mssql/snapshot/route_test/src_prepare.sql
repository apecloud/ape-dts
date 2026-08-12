DROP TABLE IF EXISTS route_src_schema.schema_orders;
DROP TABLE IF EXISTS route_table_schema.source_orders;
DROP TABLE IF EXISTS route_col_schema.source_users;
IF SCHEMA_ID(N'route_src_schema') IS NULL EXEC(N'CREATE SCHEMA route_src_schema');
IF SCHEMA_ID(N'route_table_schema') IS NULL EXEC(N'CREATE SCHEMA route_table_schema');
IF SCHEMA_ID(N'route_col_schema') IS NULL EXEC(N'CREATE SCHEMA route_col_schema');
CREATE TABLE route_src_schema.schema_orders (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL,
    ignored_value nvarchar(30) NULL
);
CREATE TABLE route_table_schema.source_orders (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE route_col_schema.source_users (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
GO
