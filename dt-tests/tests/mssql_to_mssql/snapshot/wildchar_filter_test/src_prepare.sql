DROP TABLE IF EXISTS test_db_1.one_pk_no_uk_1;
DROP TABLE IF EXISTS test_db_1.one_pk_no_uk_2;
DROP TABLE IF EXISTS test_db_2.one_pk_no_uk_1;
DROP TABLE IF EXISTS test_db_2.one_pk_no_uk_2;
DROP TABLE IF EXISTS test_db_3.one_pk_no_uk_1;
DROP TABLE IF EXISTS test_db_3.one_pk_no_uk_2;
DROP TABLE IF EXISTS test_db_4.one_pk_no_uk_1;
DROP TABLE IF EXISTS test_db_4.one_pk_no_uk_2;
DROP TABLE IF EXISTS test_db_5.one_pk_no_uk_1;
DROP TABLE IF EXISTS test_db_5.one_pk_no_uk_2;
DROP TABLE IF EXISTS other_test_db_1.one_pk_no_uk_1;
DROP TABLE IF EXISTS other_test_db_1.one_pk_no_uk_2;
DROP TABLE IF EXISTS wild_a.keep_orders;
DROP TABLE IF EXISTS wild_a.keep_private;
DROP TABLE IF EXISTS wild_a.skip_orders;
DROP TABLE IF EXISTS wild_b.keep_orders;
DROP TABLE IF EXISTS wild_skip.keep_orders;

IF SCHEMA_ID(N'test_db_1') IS NULL EXEC(N'CREATE SCHEMA test_db_1');
IF SCHEMA_ID(N'test_db_2') IS NULL EXEC(N'CREATE SCHEMA test_db_2');
IF SCHEMA_ID(N'test_db_3') IS NULL EXEC(N'CREATE SCHEMA test_db_3');
IF SCHEMA_ID(N'test_db_4') IS NULL EXEC(N'CREATE SCHEMA test_db_4');
IF SCHEMA_ID(N'test_db_5') IS NULL EXEC(N'CREATE SCHEMA test_db_5');
IF SCHEMA_ID(N'other_test_db_1') IS NULL EXEC(N'CREATE SCHEMA other_test_db_1');

CREATE TABLE test_db_1.one_pk_no_uk_1 (id int NOT NULL PRIMARY KEY, value decimal(20, 8) NULL, text_value nvarchar(50) NULL, binary_value varbinary(16) NULL);
CREATE TABLE test_db_1.one_pk_no_uk_2 (id int NOT NULL PRIMARY KEY, value decimal(20, 8) NULL, text_value nvarchar(50) NULL, binary_value varbinary(16) NULL);
CREATE TABLE test_db_2.one_pk_no_uk_1 (id int NOT NULL PRIMARY KEY, value decimal(20, 8) NULL, text_value nvarchar(50) NULL, binary_value varbinary(16) NULL);
CREATE TABLE test_db_2.one_pk_no_uk_2 (id int NOT NULL PRIMARY KEY, value decimal(20, 8) NULL, text_value nvarchar(50) NULL, binary_value varbinary(16) NULL);
CREATE TABLE test_db_3.one_pk_no_uk_1 (id int NOT NULL PRIMARY KEY, value decimal(20, 8) NULL, text_value nvarchar(50) NULL, binary_value varbinary(16) NULL);
CREATE TABLE test_db_3.one_pk_no_uk_2 (id int NOT NULL PRIMARY KEY, value decimal(20, 8) NULL, text_value nvarchar(50) NULL, binary_value varbinary(16) NULL);
CREATE TABLE test_db_4.one_pk_no_uk_1 (id int NOT NULL PRIMARY KEY, value decimal(20, 8) NULL, text_value nvarchar(50) NULL, binary_value varbinary(16) NULL);
CREATE TABLE test_db_4.one_pk_no_uk_2 (id int NOT NULL PRIMARY KEY, value decimal(20, 8) NULL, text_value nvarchar(50) NULL, binary_value varbinary(16) NULL);
CREATE TABLE test_db_5.one_pk_no_uk_1 (id int NOT NULL PRIMARY KEY, value decimal(20, 8) NULL, text_value nvarchar(50) NULL, binary_value varbinary(16) NULL);
CREATE TABLE test_db_5.one_pk_no_uk_2 (id int NOT NULL PRIMARY KEY, value decimal(20, 8) NULL, text_value nvarchar(50) NULL, binary_value varbinary(16) NULL);
CREATE TABLE other_test_db_1.one_pk_no_uk_1 (id int NOT NULL PRIMARY KEY, value decimal(20, 8) NULL, text_value nvarchar(50) NULL, binary_value varbinary(16) NULL);
CREATE TABLE other_test_db_1.one_pk_no_uk_2 (id int NOT NULL PRIMARY KEY, value decimal(20, 8) NULL, text_value nvarchar(50) NULL, binary_value varbinary(16) NULL);
GO
