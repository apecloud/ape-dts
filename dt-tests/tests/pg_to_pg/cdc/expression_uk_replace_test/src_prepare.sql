DROP SCHEMA IF EXISTS expression_uk_replace CASCADE;
CREATE SCHEMA expression_uk_replace;

CREATE TABLE expression_uk_replace.mixed_rows (id INT NOT NULL, code VARCHAR(30), val TEXT);
CREATE UNIQUE INDEX mixed_rows_uk ON expression_uk_replace.mixed_rows (id, COALESCE(code, ''));
INSERT INTO expression_uk_replace.mixed_rows VALUES (1, NULL, 'keep_null'), (1, 'code1', 'before_update');

CREATE TABLE expression_uk_replace.ordinary_rows (id INT NOT NULL UNIQUE, code VARCHAR(30), val TEXT);
CREATE UNIQUE INDEX ordinary_rows_uk ON expression_uk_replace.ordinary_rows (lower(code));
INSERT INTO expression_uk_replace.ordinary_rows VALUES (1, 'A', 'before_update');

CREATE TABLE expression_uk_replace.primary_rows (id INT PRIMARY KEY, code VARCHAR(30), val TEXT);
CREATE UNIQUE INDEX primary_rows_uk ON expression_uk_replace.primary_rows (lower(code));
INSERT INTO expression_uk_replace.primary_rows VALUES (1, 'A', 'before_update');

ALTER TABLE expression_uk_replace.mixed_rows REPLICA IDENTITY FULL;
ALTER TABLE expression_uk_replace.ordinary_rows REPLICA IDENTITY FULL;
INSERT INTO expression_uk_replace.ordinary_rows VALUES (2, 'drift', 'before_update');
