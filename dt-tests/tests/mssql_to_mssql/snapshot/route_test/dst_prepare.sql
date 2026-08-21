USE [ape_dts];
GO
DROP TABLE IF EXISTS [ape_dts].route_dst_schema.schema_orders;
DROP TABLE IF EXISTS [ape_dts].route_dst_schema.schema_payloads;
DROP TABLE IF EXISTS [ape_dts].route_table_dst.target_orders;
DROP TABLE IF EXISTS [ape_dts].route_table_dst.target_metrics;
DROP TABLE IF EXISTS [ape_dts].route_col_dst.target_users;
DROP TABLE IF EXISTS [ape_dts].route_col_dst.target_events;
DROP TABLE IF EXISTS [route_dst_db].dbo.database_orders;
DROP TABLE IF EXISTS [route.target.*].[schema.with.*].[database orders.*];
IF SCHEMA_ID(N'route_dst_schema') IS NULL EXEC(N'CREATE SCHEMA route_dst_schema');
IF SCHEMA_ID(N'route_table_dst') IS NULL EXEC(N'CREATE SCHEMA route_table_dst');
IF SCHEMA_ID(N'route_col_dst') IS NULL EXEC(N'CREATE SCHEMA route_col_dst');
IF NOT EXISTS (
    SELECT 1 FROM [route.target.*].sys.schemas WHERE name = N'schema.with.*'
)
    EXEC [route.target.*].sys.sp_executesql N'CREATE SCHEMA [schema.with.*]';
CREATE TABLE [ape_dts].route_dst_schema.schema_orders (
    id int NOT NULL PRIMARY KEY, value nvarchar(80) NULL,
    amount decimal(20, 4) NULL, ignored_value nvarchar(30) NULL
);
CREATE TABLE [ape_dts].route_dst_schema.schema_payloads (
    id int NOT NULL PRIMARY KEY, payload varbinary(100) NULL,
    created_at datetime2(7) NULL
);
CREATE TABLE [ape_dts].route_table_dst.target_orders (
    id bigint NOT NULL PRIMARY KEY, value nvarchar(80) NULL,
    payload varbinary(100) NULL
);
CREATE TABLE [ape_dts].route_table_dst.target_metrics (
    metric_id uniqueidentifier NOT NULL PRIMARY KEY,
    metric_value float NULL, observed_at datetimeoffset(7) NULL
);
CREATE TABLE [ape_dts].route_col_dst.target_users (
    user_id int NOT NULL PRIMARY KEY, display_name nvarchar(80) NULL,
    public_id uniqueidentifier NULL, raw_payload varbinary(100) NULL
);
CREATE TABLE [ape_dts].route_col_dst.target_events (
    target_event_id bigint NOT NULL PRIMARY KEY, target_event_name nvarchar(80) NULL,
    target_event_time datetime2(7) NULL, target_event_amount decimal(20, 4) NULL
);
CREATE TABLE [route_dst_db].dbo.database_orders (
    id int NOT NULL PRIMARY KEY, value nvarchar(80) NULL
);
CREATE TABLE [route.target.*].[schema.with.*].[database orders.*] (
    id int NOT NULL PRIMARY KEY, value nvarchar(80) NULL
);
GO
