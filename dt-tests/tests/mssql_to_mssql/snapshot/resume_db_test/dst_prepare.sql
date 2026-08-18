DROP TABLE IF EXISTS resume_test.resume_rows;
DROP TABLE IF EXISTS resume_test.composite_rows;
DROP TABLE IF EXISTS resume_test.binary_key_rows;
DROP TABLE IF EXISTS resume_test.[resume table.*];
DROP TABLE IF EXISTS resume_test.nullable_composite_unique_rows;
DROP TABLE IF EXISTS resume_test.string_key_rows;
DROP TABLE IF EXISTS resume_test.fresh_rows;
DROP TABLE IF EXISTS resume_test.finished_rows;
DROP TABLE IF EXISTS resume_test.finished_rows_2;
DROP TABLE IF EXISTS ape_dts_resume_test.positions;
IF SCHEMA_ID(N'resume_test') IS NULL EXEC(N'CREATE SCHEMA resume_test');
IF SCHEMA_ID(N'ape_dts_resume_test') IS NULL EXEC(N'CREATE SCHEMA ape_dts_resume_test');
CREATE TABLE resume_test.resume_rows (id int NOT NULL PRIMARY KEY, value nvarchar(50) NULL);
CREATE TABLE resume_test.composite_rows (
    tenant_id int NOT NULL, row_id int NOT NULL, value nvarchar(50) NULL,
    PRIMARY KEY (tenant_id, row_id)
);
CREATE TABLE resume_test.binary_key_rows (binary_id varbinary(32) NOT NULL PRIMARY KEY, value nvarchar(50) NULL);
CREATE TABLE resume_test.[resume table.*] ([p.k] int NOT NULL PRIMARY KEY, [value.*] nvarchar(50) NULL);
CREATE TABLE resume_test.nullable_composite_unique_rows (
    row_id int NOT NULL, uk1 int NULL, uk2 nvarchar(20) NULL, value nvarchar(50) NULL
);
CREATE UNIQUE INDEX uk_nullable_composite
    ON resume_test.nullable_composite_unique_rows (uk1, uk2)
    WHERE uk1 IS NOT NULL AND uk2 IS NOT NULL;
CREATE TABLE resume_test.string_key_rows (code nvarchar(30) NOT NULL PRIMARY KEY, value varbinary(30) NULL);
CREATE TABLE resume_test.fresh_rows (id int NOT NULL PRIMARY KEY, value nvarchar(50) NULL);
CREATE TABLE resume_test.finished_rows (id int NOT NULL PRIMARY KEY, value nvarchar(50) NULL);
CREATE TABLE resume_test.finished_rows_2 (id int NOT NULL PRIMARY KEY, value nvarchar(50) NULL);

INSERT INTO resume_test.resume_rows VALUES (1, N'one'), (2, N'two'), (3, N'three');
INSERT INTO resume_test.composite_rows VALUES (1, 1, N'one-one'), (1, 2, N'one-two');
INSERT INTO resume_test.binary_key_rows VALUES (0x00FF, N'binary-one'), (0x0102, N'binary-two');
INSERT INTO resume_test.[resume table.*] VALUES (1, N'special-one');
INSERT INTO resume_test.string_key_rows VALUES (N'a', 0x01), (N'b', 0x02);

CREATE TABLE ape_dts_resume_test.positions (
    id bigint IDENTITY(1, 1) PRIMARY KEY,
    task_id nvarchar(255) NOT NULL,
    resumer_type nvarchar(100) NOT NULL,
    position_key nvarchar(475) NOT NULL,
    position_data nvarchar(max) NULL,
    created_at datetime2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at datetime2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    UNIQUE (task_id, resumer_type, position_key)
);
INSERT INTO ape_dts_resume_test.positions
    (task_id, resumer_type, position_key, position_data) VALUES
    (N'mssql_resume_db_test', N'SnapshotDoing', N'resume_test-resume_rows',
     N'{"type":"RdbSnapshot","db_type":"mssql","schema":"resume_test","tb":"resume_rows","order_key":{"single":["id","3"]}}'),
    (N'mssql_resume_db_test', N'SnapshotDoing', N'resume_test-composite_rows',
     N'{"type":"RdbSnapshot","db_type":"mssql","schema":"resume_test","tb":"composite_rows","order_key":{"composite":[["tenant_id","1"],["row_id","2"]]}}'),
    (N'mssql_resume_db_test', N'SnapshotDoing', N'resume_test-binary_key_rows',
     N'{"type":"RdbSnapshot","db_type":"mssql","schema":"resume_test","tb":"binary_key_rows","order_key":{"single":["binary_id","0102"]}}'),
    (N'mssql_resume_db_test', N'SnapshotDoing', N'resume_test-resume table.*',
     N'{"type":"RdbSnapshot","db_type":"mssql","schema":"resume_test","tb":"resume table.*","order_key":{"single":["p.k","1"]}}'),
    (N'mssql_resume_db_test', N'SnapshotDoing', N'resume_test-string_key_rows',
     N'{"type":"RdbSnapshot","db_type":"mssql","schema":"resume_test","tb":"string_key_rows","order_key":{"single":["code","b"]}}'),
    (N'mssql_resume_db_test', N'SnapshotFinished', N'resume_test-finished_rows', NULL),
    (N'mssql_resume_db_test', N'SnapshotFinished', N'resume_test-finished_rows_2', NULL);
GO
