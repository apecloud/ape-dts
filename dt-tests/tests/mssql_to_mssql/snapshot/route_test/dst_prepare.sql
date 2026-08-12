DROP TABLE IF EXISTS route_dst_schema.schema_orders;
DROP TABLE IF EXISTS route_table_dst.target_orders;
DROP TABLE IF EXISTS route_col_dst.target_users;
IF SCHEMA_ID(N'route_dst_schema') IS NULL EXEC(N'CREATE SCHEMA route_dst_schema');
IF SCHEMA_ID(N'route_table_dst') IS NULL EXEC(N'CREATE SCHEMA route_table_dst');
IF SCHEMA_ID(N'route_col_dst') IS NULL EXEC(N'CREATE SCHEMA route_col_dst');
CREATE TABLE route_dst_schema.schema_orders (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL,
    ignored_value nvarchar(30) NULL
);
CREATE TABLE route_table_dst.target_orders (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(30) NOT NULL
);
CREATE TABLE route_col_dst.target_users (
    user_id int NOT NULL PRIMARY KEY,
    display_name nvarchar(30) NOT NULL
);
GO
