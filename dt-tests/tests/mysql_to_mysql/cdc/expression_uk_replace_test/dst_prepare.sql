DROP DATABASE IF EXISTS expression_uk_replace;
CREATE DATABASE expression_uk_replace;

CREATE TABLE expression_uk_replace.mixed_rows (id INT NOT NULL, code VARCHAR(30), val VARCHAR(30));
-- Functional key parts require MySQL 8.0.13+; older versions retain an unkeyed table.
/*!80013 CREATE UNIQUE INDEX mixed_rows_uk ON expression_uk_replace.mixed_rows (id, (COALESCE(code, ''))) */;
INSERT INTO expression_uk_replace.mixed_rows VALUES (1, NULL, 'keep_null'), (1, 'code1', 'before_update');

CREATE TABLE expression_uk_replace.ordinary_rows (id INT NOT NULL UNIQUE, code VARCHAR(30), val VARCHAR(30));
/*!80013 CREATE UNIQUE INDEX ordinary_rows_uk ON expression_uk_replace.ordinary_rows ((LOWER(code))) */;
INSERT INTO expression_uk_replace.ordinary_rows VALUES (1, 'A', 'before_update'), (2, 'C', 'before_update');

CREATE TABLE expression_uk_replace.primary_rows (id INT PRIMARY KEY, code VARCHAR(30), val VARCHAR(30));
/*!80013 CREATE UNIQUE INDEX primary_rows_uk ON expression_uk_replace.primary_rows ((LOWER(code))) */;
INSERT INTO expression_uk_replace.primary_rows VALUES (1, 'A', 'before_update');

-- Without the expression UK, replay inserts would duplicate these unkeyed rows.
/*!80013 INSERT INTO expression_uk_replace.mixed_rows VALUES (2, 'replay', 'replayed'), (3, NULL, 'replayed_null'), (4, '', 'replayed_empty') */;
-- PK-based replacement is supported on all tested versions.
INSERT INTO expression_uk_replace.primary_rows VALUES (2, 'B', 'target_value');
