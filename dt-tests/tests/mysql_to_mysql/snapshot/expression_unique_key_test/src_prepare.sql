DROP DATABASE IF EXISTS snapshot_expression_uk;
CREATE DATABASE snapshot_expression_uk;
CREATE TABLE snapshot_expression_uk.mixed_rows (id INT NOT NULL, code VARCHAR(30), val VARCHAR(30));
/*!80013 CREATE UNIQUE INDEX mixed_uk ON snapshot_expression_uk.mixed_rows (id, (COALESCE(code, ''))) */;
CREATE TABLE snapshot_expression_uk.primary_rows (id INT PRIMARY KEY, code VARCHAR(30), val VARCHAR(30));
/*!80013 CREATE UNIQUE INDEX primary_expression_uk ON snapshot_expression_uk.primary_rows ((COALESCE(code, ''))) */;
CREATE TABLE snapshot_expression_uk.ordinary_rows (id INT NOT NULL UNIQUE, code VARCHAR(30), val VARCHAR(30));
/*!80013 CREATE UNIQUE INDEX ordinary_expression_uk ON snapshot_expression_uk.ordinary_rows ((COALESCE(code, ''))) */;
