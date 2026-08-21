USE [ape_dts];
GO
DROP TABLE IF EXISTS [ape_dts].route_src_schema.schema_orders;
DROP TABLE IF EXISTS [ape_dts].route_src_schema.schema_payloads;
DROP TABLE IF EXISTS [ape_dts].route_table_schema.source_orders;
DROP TABLE IF EXISTS [ape_dts].route_table_schema.source_metrics;
DROP TABLE IF EXISTS [ape_dts].route_col_schema.source_users;
DROP TABLE IF EXISTS [ape_dts].route_col_schema.source_events;
DROP TABLE IF EXISTS [route_src_db].dbo.database_orders;
DROP TABLE IF EXISTS [route.source.*].[schema.with.*].[database orders.*];
IF SCHEMA_ID(N'route_src_schema') IS NULL EXEC(N'CREATE SCHEMA route_src_schema');
IF SCHEMA_ID(N'route_table_schema') IS NULL EXEC(N'CREATE SCHEMA route_table_schema');
IF SCHEMA_ID(N'route_col_schema') IS NULL EXEC(N'CREATE SCHEMA route_col_schema');
IF NOT EXISTS (
    SELECT 1 FROM [route.source.*].sys.schemas WHERE name = N'schema.with.*'
)
    EXEC [route.source.*].sys.sp_executesql N'CREATE SCHEMA [schema.with.*]';
CREATE TABLE [ape_dts].route_src_schema.schema_orders (
    id int NOT NULL PRIMARY KEY, value nvarchar(80) NULL,
    amount decimal(20, 4) NULL, ignored_value nvarchar(30) NULL
);
CREATE TABLE [ape_dts].route_src_schema.schema_payloads (
    id int NOT NULL PRIMARY KEY, payload varbinary(100) NULL,
    created_at datetime2(7) NULL
);
CREATE TABLE [ape_dts].route_table_schema.source_orders (
    id bigint NOT NULL PRIMARY KEY, value nvarchar(80) NULL,
    payload varbinary(100) NULL
);
CREATE TABLE [ape_dts].route_table_schema.source_metrics (
    metric_id uniqueidentifier NOT NULL PRIMARY KEY,
    metric_value float NULL, observed_at datetimeoffset(7) NULL
);
CREATE TABLE [ape_dts].route_col_schema.source_users (
    id int NOT NULL PRIMARY KEY, value nvarchar(80) NULL,
    external_code uniqueidentifier NULL, payload varbinary(100) NULL
);
CREATE TABLE [ape_dts].route_col_schema.source_events (
    event_id bigint NOT NULL PRIMARY KEY, event_name nvarchar(80) NULL,
    event_time datetime2(7) NULL, event_amount decimal(20, 4) NULL
);
CREATE TABLE [route_src_db].dbo.database_orders (
    id int NOT NULL PRIMARY KEY, value nvarchar(80) NULL
);
CREATE TABLE [route.source.*].[schema.with.*].[database orders.*] (
    id int NOT NULL PRIMARY KEY, value nvarchar(80) NULL
);
GO
